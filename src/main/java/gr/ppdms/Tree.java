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
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
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

    private static final Map<String, CopyOnWriteArrayList<String>> changesByCourseBuffer = new ConcurrentHashMap<>();

    private static void sendEmail(Map<String, List<String>> changesByCourse) {
        StringBuilder emailContent = new StringBuilder("File system changes detected:\n\n");

        for (Map.Entry<String, List<String>> entry : changesByCourse.entrySet()) {
            String courseName = entry.getKey();
            List<String> changes = entry.getValue();

            if (changes.isEmpty()) continue;

            emailContent.append("=== Course: ").append(courseName).append(" ===\n");
            for (String change : changes) {
                emailContent.append("- ").append(change).append("\n");
            }
            emailContent.append("\n");
        }

        System.out.println("\n=== Email Content ===");
        System.out.println("To: p3220150@aueb.gr");
        System.out.println("From: tree-eclass <tree-eclass@ppdms.gr>");
        System.out.println("Subject: File Changes");
        System.out.println(emailContent.toString());
        System.out.println("===================\n");

        try {
            ProcessBuilder processBuilder = new ProcessBuilder("sendmail", "-f", "basilpapadimas@gmail.com", "-t");
            processBuilder.redirectErrorStream(true);
            Process sendmail = processBuilder.start();

            try (OutputStream os = sendmail.getOutputStream()) {
                os.write(("To: p3220150@aueb.gr\r\n").getBytes());
                os.write(("From: tree-eclass <tree-eclass@ppdms.gr>\r\n").getBytes());
                os.write(("Subject: File Changes\r\n\r\n").getBytes());
                os.write((emailContent.toString()).getBytes());
            }
            int exitCode = sendmail.waitFor();
            if (exitCode != 0) {
                System.err.println("sendmail process exited with code: " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Failed to send email: " + e.getMessage());
            e.printStackTrace();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static class Node implements Serializable {
        private static final long serialVersionUID = 1L;

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
            directoryChildren = new ArrayList<>(numDirectoryChildren);
            for (int i = 0; i < numDirectoryChildren; i++) {
                Node child = (Node) in.readObject();
                directoryChildren.add(child);
            }

            int numFileChildren = in.readInt();
            fileChildren = new ArrayList<>(numFileChildren);
            for (int i = 0; i < numFileChildren; i++) {
                String file = in.readUTF();
                fileChildren.add(file);
            }

            int numFileNames = in.readInt();
            fileNames = new ArrayList<>(numFileNames);
            for (int i = 0; i < numFileNames; i++) {
                String fileName = in.readUTF();
                fileNames.add(fileName);
            }

            int numFileHashes = in.readInt();
            fileHashes = new HashMap<>(numFileHashes);
            for (int i = 0; i < numFileHashes; i++) {
                String key = in.readUTF();
                String value = in.readUTF();
                fileHashes.put(key, value);
            }

            int numFileEtags = in.readInt();
            fileEtags = new HashMap<>(numFileEtags);
            for (int i = 0; i < numFileEtags; i++) {
                String key = in.readUTF();
                String value = in.readUTF();
                fileEtags.put(key, value);
            }
        }

        private Object readResolve() throws ObjectStreamException {
            return this;
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

    public static void print(Node rootNode, String courseName) {
        // Root node: Hyperlink to course URL (rootNode.name), Text is courseName
        System.out.println("\u001B]8;;" + rootNode.name + "\u0007" + courseName + "\u001B]8;;\u0007");
        printChildrenRecursive(rootNode, "");
    }

    private static void printChildrenRecursive(Node parentNode, String indent) {
        List<Node> directories = parentNode.directoryChildren;
        List<String> fileNames = parentNode.fileNames;
        List<String> fileUrls = parentNode.fileChildren;

        int dirCount = directories.size();
        int fileCount = fileNames.size(); // Assuming fileNames and fileUrls are parallel and same size
        int totalChildren = dirCount + fileCount;
        int childrenProcessed = 0;

        // Print directory children
        for (int i = 0; i < dirCount; i++) {
            Node dir = directories.get(i);
            childrenProcessed++;
            boolean isLast = (childrenProcessed == totalChildren);
            String connector = isLast ? "└── " : "├── ";
            // Directory entry: Hyperlink to disk path (dir.parent), Text is directory name (dir.name)
            System.out.println(indent + connector + "\u001B]8;;" + dir.parent + "\u0007" + dir.name + "\u001B]8;;\u0007");

            String nextIndent = indent + (isLast ? "    " : "│   ");
            printChildrenRecursive(dir, nextIndent);
        }

        // Print file children
        for (int i = 0; i < fileCount; i++) {
            String fileName = fileNames.get(i);
            String fileUrl = fileUrls.get(i);
            childrenProcessed++;
            boolean isLast = (childrenProcessed == totalChildren);
            String connector = isLast ? "└── " : "├── ";
            // File entry: Hyperlink to file URL (fileUrl), Text is file name (fileName)
            System.out.println(indent + connector + "\u001B]8;;" + fileUrl + "\u0007" + fileName + "\u001B]8;;\u0007");
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

    private static void addChange(String courseName, String message) {

        System.out.println(message + " (Course: " + courseName + ")");
        changesByCourseBuffer.computeIfAbsent(courseName, k -> new CopyOnWriteArrayList<>()).add(message);
    }

    private static void reportAllAddedRecursive(Node dirNode, String dirEclassPath, String courseName) {
        // dirNode is the newly added directory. dirEclassPath is its full eClass path.

        // Report all files in this newly added directory
        for (int i = 0; i < dirNode.fileNames.size(); i++) {
            String fileName = dirNode.fileNames.get(i); // Assuming fileNames and fileChildren are parallel
            String fileEclassPath = dirEclassPath + "/" + fileName;
            addChange(courseName, "Added file: " + fileEclassPath);
        }

        // Report all subdirectories and recurse
        for (Node subDir : dirNode.directoryChildren) {
            String subDirName = subDir.name;
            String subDirEclassPath = dirEclassPath + "/" + subDirName;
            addChange(courseName, "Added directory: " + subDirEclassPath);
            reportAllAddedRecursive(subDir, subDirEclassPath, courseName); // Recurse
        }
    }

    public static void diff(Node previous, Node latest, String courseName, String currentDirectoryEclassPath) {
        // currentDirectoryEclassPath is the eClass path of the directory node whose contents are being compared.
        // e.g., "CourseName", "CourseName/FolderA", "CourseName/FolderA/Labs"

        // Populate directory maps using Node.name as key
        Map<String, Node> oldDirs = new HashMap<>();
        if (previous != null) {
            for (Node dir : previous.directoryChildren) {
                oldDirs.put(dir.name, dir);
            }
        }
        Map<String, Node> newDirs = new HashMap<>();
        for (Node dir : latest.directoryChildren) {
            newDirs.put(dir.name, dir);
        }

        // 1. Identify deleted directories
        if (previous != null) {
            for (String dirName : oldDirs.keySet()) {
                if (!newDirs.containsKey(dirName)) {
                    String deletedDirPath = currentDirectoryEclassPath + "/" + dirName;
                    addChange(courseName, "Deleted directory: " + deletedDirPath);
                }
            }
        }

        // 2. Identify added or common directories
        for (String dirName : newDirs.keySet()) {
            Node newDirNode = newDirs.get(dirName);
            String currentSubDirPath = currentDirectoryEclassPath + "/" + dirName;
            if (previous == null || !oldDirs.containsKey(dirName)) {
                addChange(courseName, "Added directory: " + currentSubDirPath);
                reportAllAddedRecursive(newDirNode, currentSubDirPath, courseName);
            } else {
                // Directory exists in both old and new state: recurse
                Node oldDirNode = oldDirs.get(dirName);
                diff(oldDirNode, newDirNode, courseName, currentSubDirPath);
            }
        }

        // 3. File comparison (for files directly within currentDirectoryEclassPath)
        // This section only runs if 'currentDirectoryEclassPath' itself persisted (i.e., 'previous' is not null).
        // If 'previous' is null, it implies 'currentDirectoryEclassPath' (represented by 'latest') is entirely new,
        // and its files were already handled by reportAllAddedRecursive.
        if (previous != null) {
            Map<String, String> oldFileUrlToName = new HashMap<>();
            Map<String, String> oldFileHashes = previous.fileHashes; // Map<URL, Hash>
            for (int i = 0; i < previous.fileChildren.size(); i++) { // fileChildren are URLs
                oldFileUrlToName.put(previous.fileChildren.get(i), previous.fileNames.get(i));
            }

            Map<String, String> newFileUrlToName = new HashMap<>();
            Map<String, String> newFileHashes = latest.fileHashes; // Map<URL, Hash>
            for (int i = 0; i < latest.fileChildren.size(); i++) { // fileChildren are URLs
                newFileUrlToName.put(latest.fileChildren.get(i), latest.fileNames.get(i));
            }

            // Check for deleted or updated files
            for (String fileUrl : oldFileUrlToName.keySet()) {
                String fileName = oldFileUrlToName.get(fileUrl);
                String fullFilePath = currentDirectoryEclassPath + "/" + fileName;

                if (!newFileUrlToName.containsKey(fileUrl)) {
                    addChange(courseName, "Deleted file: " + fullFilePath);
                } else {
                    // File exists in both, check for update based on hash
                    String oldHash = oldFileHashes.get(fileUrl); // Can be null if URL not in map, or value was null
                    String newHash = newFileHashes.get(fileUrl); // Can be null

                    boolean updated = false;
                    String updateReason = "";

                    if (oldHash != null && newHash != null) {
                        if (!oldHash.equals(newHash)) {
                            updated = true;
                        }
                    } else if (oldHash == null && newHash != null) {
                        updated = true;
                        updateReason = " (hash appeared)";
                    } else if (oldHash != null && newHash == null) {
                        updated = true;
                        updateReason = " (hash removed)";
                    }
                    // If both oldHash and newHash are null, it's not considered an update.

                    if (updated) {
                        addChange(courseName, "Updated file: " + fullFilePath + updateReason);
                    }
                }
            }

            // Check for added files (to this existing directory)
            for (String fileUrl : newFileUrlToName.keySet()) {
                if (!oldFileUrlToName.containsKey(fileUrl)) {
                    String fileName = newFileUrlToName.get(fileUrl);
                    String fullFilePath = currentDirectoryEclassPath + "/" + fileName;
                    addChange(courseName, "Added file: " + fullFilePath);
                }
            }
        }
    }

    public static String getCookie() {
        String cookie = null;
        try (FileInputStream fileInputStream = new FileInputStream("cookie.ser"); ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            cookie = (String) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to load cookie, attempting update: " + e.getMessage());
            try {
                updateCookie();
                cookie = getCookie();
            } catch (RuntimeException updateEx) {
                System.err.println("Failed to update cookie: " + updateEx.getMessage());
                throw updateEx;
            }
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
                System.err.println("Failed to download Google Drive file: " + fileUrl + " - " + e.getMessage());
                throw new RuntimeException("Failed to download file: " + fileUrl, e);
            }
        }
        try {
            Connection connection = Jsoup.connect(fileUrl)
                    .cookie("PHPSESSID", getCookie())
                    .ignoreContentType(true)
                    .maxBodySize(0);

            Connection.Response response = connection.execute();

            if (response.statusCode() == 403) {
                System.err.println("Access denied (403) for " + fileUrl + ", attempting cookie update and retry.");
                updateCookie();
                response = Jsoup.connect(fileUrl)
                        .cookie("PHPSESSID", getCookie())
                        .ignoreContentType(true)
                        .maxBodySize(0)
                        .execute();
            }

            if (response.statusCode() == 200) {
                File destinationFile = new File(destination);
                File parentDir = destinationFile.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    if (!parentDir.mkdirs()) {
                        throw new IOException("Failed to create parent directories for: " + destination);
                    }
                }

                try (InputStream in = response.bodyStream();
                     FileOutputStream out = new FileOutputStream(destinationFile)) {
                    IOUtils.copy(in, out);
                }
            } else {
                throw new IOException("Failed to download file: " + fileUrl + " - Status code: " + response.statusCode());
            }

        } catch (IOException e) {
            System.err.println("Error downloading file " + fileUrl + " to " + destination + ": " + e.getMessage());
            e.printStackTrace();
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
                    System.err.println("FATAL: Could not read courses.csv. Exiting TimerTask run. " + e.getMessage());
                    e.printStackTrace();
                    return;
                }

                for (int courseNumKey : courses.keySet()) {
                    String courseName = courses.get(courseNumKey);
                    String url = "https://eclass.aueb.gr/modules/document/index.php?course=INF" + courseNumKey;
                    String downloadFolder = downloadFolders.get(courseNumKey);

                    try {
                        System.out.println("");
                        
                        File dirToDelete = new File(downloadFolder);
                        if (dirToDelete.exists()) {
                            try {
                                FileUtils.deleteDirectory(dirToDelete);
                            } catch (IOException ex) {
                                System.err.println("Warning: Failed to delete old directory before processing: " + downloadFolder + " - " + ex.getMessage());
                            }
                        }

                        Node oldRoot = load(courseNumKey + ".ser");
                        Node newRoot = gen(url, downloadFolder + "/");
                        diff(oldRoot, newRoot, courseName, courseName);
                        print(newRoot, courseName);
                        save(newRoot, courseNumKey);

                    } catch (Exception e) {
                        System.err.println("ERROR processing course " + courseName + " (ID: " + courseNumKey + "): " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                Map<String, List<String>> changesToSend = new HashMap<>();
                if (!changesByCourseBuffer.isEmpty()) {
                    synchronized (changesByCourseBuffer) {
                        for (Map.Entry<String, CopyOnWriteArrayList<String>> entry : changesByCourseBuffer.entrySet()) {
                            if (!entry.getValue().isEmpty()) {
                                changesToSend.put(entry.getKey(), new ArrayList<>(entry.getValue()));
                            }
                        }
                        changesByCourseBuffer.clear();
                    }
                }

                if (!changesToSend.isEmpty()) {
                    try {
                        System.out.println("Attempting to send email with changes from " + changesToSend.size() + " course(s).");
                        sendEmail(changesToSend);
                    } catch (Exception e) {
                        System.err.println("Error sending email notification after download cycle: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
                System.out.println("");
            }
        }, 0, 60*60*1000);

        System.out.println("tree-eclass checker started. Scheduled tasks running in background.");
    }
}