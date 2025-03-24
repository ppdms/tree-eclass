package gr.ppdms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Tree {

    private static final List<String> changesBuffer = new CopyOnWriteArrayList<>();

    private static void scheduleChangeChecker() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                synchronized (changesBuffer) {
                    if (!changesBuffer.isEmpty()) {
                        sendEmail(changesBuffer);
                        changesBuffer.clear();
                    }
                }
            }
        }, 0, 60*60*1000); // Every hour
    }

    private static void sendEmail(List<String> changes) {
        StringBuilder emailContent = new StringBuilder("Changes detected:\n");
        for (String change : changes) {
            emailContent.append(change).append("\n");
        }
        try {
            Process sendmail = Runtime.getRuntime().exec("sendmail -f \"basilpapadimas@gmail.com\" -t");
            try (OutputStream os = sendmail.getOutputStream()) {
                os.write(("To: p3220150@aueb.gr\r\n").getBytes());
                os.write(("From: tree-eclass <tree-eclass@ppdms.gr>\r\n").getBytes());
                os.write(("Subject: File Changes\r\n\r\n").getBytes());
                os.write((emailContent.toString()).getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to send email", e);
        }
    }

    public static class Node implements Serializable {
        public String parent = "";
        public String name = "";
        public List<Node> directoryChildren = new ArrayList<>();
        public List<String> fileChildren = new ArrayList<>();
        public List<String> fileNames = new ArrayList<>();
        public Map<String, String> fileHashes = new HashMap<>();
        public Map<String, String> fileEtags = new HashMap<>();

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();

            out.writeInt(directoryChildren.size());
            for (Node child : directoryChildren) {
                out.writeObject(child);
            }

            out.writeInt(fileChildren.size());
            for (String file : fileChildren) {
                out.writeUTF(file);
            }

            out.writeInt(fileNames.size());
            for (String fileName : fileNames) {
                out.writeUTF(fileName);
            }

            out.writeInt(fileHashes.size());
            for (Map.Entry<String, String> entry : fileHashes.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }

            out.writeInt(fileEtags.size());
            for (Map.Entry<String, String> entry : fileEtags.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();

            int numDirectoryChildren = in.readInt();
            for (int i = 0; i < numDirectoryChildren; i++) {
                Node child = (Node) in.readObject();
                directoryChildren.add(child);
            }

            int numFileChildren = in.readInt();
            for (int i = 0; i < numFileChildren; i++) {
                String file = in.readUTF();
                fileChildren.add(file);
            }

            int numFileNames = in.readInt();
            for (int i = 0; i < numFileNames; i++) {
                String fileName = in.readUTF();
                fileNames.add(fileName);
            }

            int numFileHashes = in.readInt();
            for (int i = 0; i < numFileHashes; i++) {
                String key = in.readUTF();
                String value = in.readUTF();
                fileHashes.put(key, value);
            }

            int numFileEtags = in.readInt();
            for (int i = 0; i < numFileEtags; i++) {
                String key = in.readUTF();
                String value = in.readUTF();
                fileEtags.put(key, value);
            }
        }
    }

    public static List<String>[] links(String url) {
        List<String> files = new ArrayList<>();
        List<String> directories = new ArrayList<>();
        List<String> fileNames = new ArrayList<>();
        List<String> directoryNames = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<String>[] array = new ArrayList[4];
        Elements links;
        try {
            Document doc = Jsoup.connect(url).get();

            if (doc.html().contains("Σύνδεση")) {
                doc = Jsoup.connect(url).cookies(Collections.singletonMap("PHPSESSID", getCookie())).get();
            }
            if (doc.html().contains("Σύνδεση")) {
                updateCookie();
                doc = Jsoup.connect(url).cookies(Collections.singletonMap("PHPSESSID", getCookie())).get();
            }
            links = doc.select("a[href]");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < links.size(); i++) {
            Element link = links.get(i);
            String href = link.attr("href");
            String linkText = link.html();
            if (("https://eclass.aueb.gr" + href).equals(url) || linkText.contains("Αποθήκευση") || linkText.contains("Λήψη") || href.contains("&sort") || href.contains("modules/document/?course=") || !href.contains("google") && !href.contains("modules/document/") || href.length() > 9 && href.substring(href.length() - 9).equals("openDir=/") || (href.contains("modules/document/index.php?") && (!href.contains("&openDir=/") || href.contains("&openDir=%2F")))) {
                continue;
            }
            if (href.contains("google")) {
                files.add(href);
                fileNames.add(linkText);
            } else if (href.substring(href.length() - 6).contains(".")) {
                files.add(href);
                fileNames.add(linkText);
            } else if (!href.contains("&download=/")) {
                directories.add("https://eclass.aueb.gr" + href);
                directoryNames.add(linkText);
            }
        }

        array[0] = files;
        array[1] = directories;
        array[2] = fileNames;
        array[3] = directoryNames;

        return array;
    }

    public static Node gen(String url, String parent) {
        List<String>[] array = links(url);
        List<String> files = array[0];
        List<String> directories = array[1];
        List<String> fileNames = array[2];
        List<String> directoryNames = array[3];

        Node root = new Node();
        root.parent = parent;
        root.name = url;
        root.fileChildren = files;
        root.fileNames = fileNames;

        for (int i = 0; i < directories.size(); i++) {
            String directory = directories.get(i);
            String directoryPath = parent + directoryNames.get(i) + "/";

            try {
                Files.createDirectories(Paths.get(directoryPath));
            } catch (IOException e) {
                throw new RuntimeException("Failed to create directory: " + directoryPath, e);
            }

            Node child = gen(directory, directoryPath);
            child.name = directoryNames.get(i);
            root.directoryChildren.add(child);
        }

        for (int i = 0; i < files.size(); i++) {
            String fileUrl = files.get(i);
            // For Google downloads, always re-download & hash
            if (!fileUrl.contains("google")) {
                String etag = fetchEtag(fileUrl);
                String oldEtag = root.fileEtags.get(fileUrl);

                // If ETag is missing or changed, re-download
                if (etag == null || oldEtag == null || !oldEtag.equals(etag)) {
                    String fileName = extractFileName(fileUrl);
                    String filePath = parent + fileName;
                    filePath = downloadFile(fileUrl, filePath);
                    String fileHash = computeMD5(filePath);
                    root.fileHashes.put(fileUrl, fileHash);
                    root.fileEtags.put(fileUrl, etag);  // might be null, but tracked anyway
                } else {
                    root.fileEtags.put(fileUrl, oldEtag);
                    root.fileHashes.put(fileUrl, previousHashIfAvailable(root, fileUrl));
                }
            } else {
                // Google file: always download & compute new hash
                String fileName = extractFileName(fileUrl);
                String filePath = parent + fileName;
                filePath = downloadFile(fileUrl, filePath);
                String fileHash = computeMD5(filePath);
                root.fileHashes.put(fileUrl, fileHash);
            }
        }

        return root;
    }

    public static void print(Node root, String prefix) {
        System.out.println(prefix + "\u001B]8;;" + root.parent + "\u0007" + root.name + "\u001B]8;;\u0007");
        String branch_prefix = prefix + "\t";

        for (int i = 0; i < root.directoryChildren.size(); i++) {
            Node child = root.directoryChildren.get(i);
            print(child, branch_prefix);
        }

        for (int i = 0; i < root.fileChildren.size(); i++) {
            String filename = root.fileNames.get(i);
            String fileUrl = root.fileChildren.get(i);
            System.out.println(branch_prefix + "\u001B]8;;" + fileUrl + "\u0007" + filename + "\u001B]8;;\u0007");
        }
    }

    public static void save(Node root, int CourseNum) {
        try (FileOutputStream fileOut = new FileOutputStream(CourseNum + ".ser"); ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(root);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Node load(String filename) {
        Node root = null;
        try (FileInputStream fileInputStream = new FileInputStream(filename); ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            root = (Node) objectInputStream.readObject();
        } catch (ClassNotFoundException | IOException e) {
            root = new Node();
        }
        return root;
    }

    public static void diff(Node previous, Node latest) {
        HashMap<String, Node> oldDirectoryChildren = new HashMap<>();
        HashMap<String, Node> newDirectoryChildren = new HashMap<>();

        for (Node directory : previous.directoryChildren) {
            oldDirectoryChildren.put(directory.parent, directory);
        }

        for (Node directory : latest.directoryChildren) {
            newDirectoryChildren.put(directory.parent, directory);
        }

        Set<String> allDirectories = new LinkedHashSet<>(oldDirectoryChildren.keySet());
        allDirectories.addAll(newDirectoryChildren.keySet());

        for (String directory : oldDirectoryChildren.keySet()) {
            if (!newDirectoryChildren.keySet().contains(directory)) {
                System.out.println(directory + " deleted!");
                synchronized (changesBuffer) {
                    changesBuffer.add("Deleted directory: " + directory);
                }
                allDirectories.remove(directory);
            }
        }
        for (String directory : newDirectoryChildren.keySet()) {
            if (!oldDirectoryChildren.keySet().contains(directory)) {
                System.out.println(directory + " added!");
                synchronized (changesBuffer) {
                    changesBuffer.add("Added directory: " + directory);
                }
                allDirectories.remove(directory);
            }
        }
        for (String directory : allDirectories) {
            diff(oldDirectoryChildren.get(directory), newDirectoryChildren.get(directory));
        }

        for (String file : previous.fileChildren) {
            if (!latest.fileChildren.contains(file)) {
                System.out.println(file + " deleted!");
                synchronized (changesBuffer) {
                    changesBuffer.add("Deleted: " + file);
                }
            } else if (!previous.fileHashes.get(file).equals(latest.fileHashes.get(file))) {
                System.out.println(file + " updated!");
                synchronized (changesBuffer) {
                    changesBuffer.add("Updated: " + file);
                }
            }
        }
        for (String file : latest.fileChildren) {
            if (!previous.fileChildren.contains(file)) {
                System.out.println(file + " added!");
                synchronized (changesBuffer) {
                    changesBuffer.add("Added: " + file);
                }
            }
        }
    }

    public static String getCookie() {
        String cookie = null;
        try (FileInputStream fileInputStream = new FileInputStream("cookie.ser"); ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            cookie = (String) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            updateCookie();
            cookie = getCookie();
        }
        return cookie;
    }

    public static void updateCookie() {
        String username, password, cookie;
        File file = new File("credentials.txt");

        try (Scanner scanner = new Scanner(file)) {
            username = scanner.nextLine().trim();
            password = scanner.nextLine().trim();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Credentials file not found", e);
        }

        try {
            Connection.Response initialResponse = Jsoup.connect("https://eclass.aueb.gr/main/login_form.php")
                    .method(Connection.Method.GET)
                    .execute();

            Connection.Response loginResponse = Jsoup.connect("https://eclass.aueb.gr/?login_page=1")
                    .method(Connection.Method.POST)
                    .cookies(initialResponse.cookies())
                    .data("uname", username)
                    .data("pass", password)
                    .data("submit", "Είσοδος")
                    .execute();

            cookie = loginResponse.cookie("PHPSESSID");

        } catch (IOException e) {
            throw new RuntimeException("Error during login process", e);
        }

        try (FileOutputStream fileOut = new FileOutputStream("cookie.ser"); ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(cookie);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String downloadFile(String fileUrl, String destination) {
        if (fileUrl.contains("google")) {
            try {
                return GoogleDriveDownloader.downloadFile(fileUrl, destination);
            } catch (IOException e) {
                throw new RuntimeException("Failed to download file: " + fileUrl, e);
            }
        }
        try {
            Connection.Response response = Jsoup.connect(fileUrl)
                    .cookie("PHPSESSID", getCookie())
                    .ignoreContentType(true)
                    .execute();

            if (response.statusCode() == 200) {
                // Create parent directories if they do not exist
                File destinationFile = new File(destination);
                destinationFile.getParentFile().mkdirs();

                try (InputStream in = response.bodyStream()) {
                    Files.copy(in, destinationFile.toPath());
                }
            } else if (response.statusCode() == 403) {
                updateCookie();
                response = Jsoup.connect(fileUrl)
                        .cookie("PHPSESSID", getCookie())
                        .ignoreContentType(true)
                        .execute();

                try (InputStream in = response.bodyStream()) {
                    Files.copy(in, Paths.get(destination));
                }
            } else {
                throw new RuntimeException("Failed to download file: " + fileUrl + " with status code: " + response.statusCode());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to download file: " + fileUrl, e);
        }
        return destination;
    }

    private static String computeMD5(String filePath) {
        try (InputStream is = Files.newInputStream(Paths.get(filePath))) {
            return DigestUtils.md5Hex(IOUtils.toByteArray(is));
        } catch (IOException e) {
            throw new RuntimeException("Failed to compute MD5 hash for file: " + filePath, e);
        }
    }

    private static String fetchEtag(String fileUrl) {
        try {
            Connection.Response response = Jsoup
                    .connect(fileUrl)
                    .cookie("PHPSESSID", getCookie())
                    .ignoreContentType(true)
                    .method(Connection.Method.HEAD)
                    .execute();
            return response.header("ETag"); // Might be null if server doesn't provide it
        } catch (IOException e) {
            throw new RuntimeException("Failed to fetch ETag for: " + fileUrl, e);
        }
    }

    private static String extractFileName(String fileUrl) {
        String fileName = fileUrl.substring(fileUrl.lastIndexOf('/') + 1);
        try {
            fileName = URLDecoder.decode(fileName, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to decode filename: " + fileName, e);
        }
        return fileName;
    }

    private static String previousHashIfAvailable(Node root, String fileUrl) {
        return root.fileHashes.getOrDefault(fileUrl, "");
    }

    public static void main(String[] args) {
        scheduleChangeChecker();

        Timer eclassTimer = new Timer();
        eclassTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {

                Map<Integer, String> courses = new HashMap<>();
                Map<Integer, String> downloadFolders = new HashMap<>();

                try (BufferedReader br = new BufferedReader(new FileReader("courses.csv"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] values = line.split(",");
                        int courseNum = Integer.parseInt(values[0]);
                        String courseName = values[1];
                        String downloadFolder = values[2];
                        courses.put(courseNum, courseName);
                        downloadFolders.put(courseNum, downloadFolder);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                for (int CourseNum : courses.keySet()) {
                    String url = "https://eclass.aueb.gr/modules/document/index.php?course=INF" + CourseNum;
                    String downloadFolder = downloadFolders.get(CourseNum);
                    try {
                        FileUtils.deleteDirectory(new File(downloadFolder));
                    } catch (IOException ex) {
                        System.out.println("Failed to delete directory: " + downloadFolder);
                    }
                    Node oldRoot = load(CourseNum + ".ser");
                    Node newRoot = gen(url, downloadFolder);
                    diff(oldRoot, newRoot);
                    print(newRoot, "");
                    save(newRoot, CourseNum);
                }

            }
        }, 0, 60*60*1000);

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Interrupted");
            }
        }
    }
}
