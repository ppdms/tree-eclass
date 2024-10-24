package gr.ppdms;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class GoogleDriveDownloader {

    public static String downloadFile(String fileURL, String destinationPath) throws IOException {
        // Clean up the file ID from the URL if necessary
        String fileId = extractFileId(fileURL);
        
        // Extract the resourcekey if available
        String resourceKey = extractResourceKey(fileURL);
        
        String downloadUrl = String.format("https://drive.usercontent.google.com/download?id=%s&export=download&authuser=0", fileId);
        if (resourceKey != null) {
            downloadUrl += "&resourcekey=" + resourceKey;
        }
        System.out.println("Downloading from: " + downloadUrl);

        URL url = new URL(downloadUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setUseCaches(false);
        conn.setRequestProperty("Accept-Encoding", "identity");
        conn.setRequestProperty("User-Agent", "Mozilla/5.0");
        
        destinationPath = destinationPath + "/" + getFileName(conn, fileURL);

        // Create the destination directory if it doesn't exist
        File destFile = new File(destinationPath);
        destFile.getParentFile().mkdirs();

        // Download with progress tracking
        try (InputStream in = conn.getInputStream();
             FileOutputStream out = new FileOutputStream(destinationPath)) {
            
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        return destinationPath;
    }

    private static String extractFileId(String url) {
        String pattern = "https://drive.google.com/file/d/([a-zA-Z0-9_-]+)";
        Pattern compiledPattern = Pattern.compile(pattern);
        Matcher matcher = compiledPattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return url;
    }

    private static String extractResourceKey(String url) {
        String pattern = "resourcekey=([^&]+)";
        Pattern compiledPattern = Pattern.compile(pattern);
        Matcher matcher = compiledPattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    public static String getFileName(HttpURLConnection connection, String fileUrl) {
        String raw = connection.getHeaderField("Content-Disposition");
        
        // Check if the Content-Disposition header contains the filename
        if (raw != null && raw.contains("filename=")) {
            String fileName = raw.split("filename=")[1].trim();
            return fileName.replaceAll("\"", "");
        } else {
            // Fallback: Parse the HTML page to extract the title
            try {
                Document doc = Jsoup.connect(fileUrl).get();
                String title = doc.title();
                
                // Clean up the title if necessary (Google Drive adds "- Google Drive" to the title)
                if (title.contains("- Google Drive")) {
                    title = title.replace("- Google Drive", "").trim();
                }
                return title;
            } catch (IOException e) {
                e.printStackTrace();
                // Return a default name if an error occurs while fetching the page
                return "downloaded_file";
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java GoogleDriveDownloader <fileId> <destinationPath>");
            return;
        }

        String fileId = args[0];
        String destinationPath = args[1];

        try {
            downloadFile(fileId, destinationPath);
            System.out.println("File downloaded successfully to " + destinationPath);
        } catch (IOException e) {
            System.err.println("Failed to download file: " + e.getMessage());
        }
    }
}