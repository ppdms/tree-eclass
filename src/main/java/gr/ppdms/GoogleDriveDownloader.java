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
        
        // Extract authuser if available
        String authUser = extractAuthUser(fileURL);
        
        String downloadUrl = String.format("https://drive.usercontent.google.com/download?id=%s&export=download", fileId);
        if (resourceKey != null) {
            downloadUrl += "&resourcekey=" + resourceKey;
        }
        if (authUser != null) {
            downloadUrl += "&authuser=" + authUser;
        }

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
        // First try to match the file/d/ pattern
        String filePattern = "https://drive.google.com/file/d/([a-zA-Z0-9_-]+)";
        Pattern fileCompiled = Pattern.compile(filePattern);
        Matcher fileMatcher = fileCompiled.matcher(url);
        if (fileMatcher.find()) {
            return fileMatcher.group(1);
        }
        
        // Then try to match the open?id= pattern
        String openPattern = "id=([a-zA-Z0-9_-]+)";
        Pattern openCompiled = Pattern.compile(openPattern);
        Matcher openMatcher = openCompiled.matcher(url);
        if (openMatcher.find()) {
            return openMatcher.group(1);
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

    private static String extractAuthUser(String url) {
        String pattern = "authuser=([^&]+)";
        Pattern compiledPattern = Pattern.compile(pattern);
        Matcher matcher = compiledPattern.matcher(url);
        if (matcher.find()) {
            try {
                return java.net.URLDecoder.decode(matcher.group(1), "UTF-8");
            } catch (IOException e) {
                return matcher.group(1);
            }
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
            System.out.println("Usage: java GoogleDriveDownloader <fileUrl> <destinationPath>");
            return;
        }

        String fileUrl = args[0];
        String destinationPath = args[1];

        try {
            String savedPath = downloadFile(fileUrl, destinationPath);
            System.out.println("File downloaded successfully to: " + savedPath);
        } catch (IOException e) {
            System.err.println("Error downloading file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}