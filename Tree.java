import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.io.*;
import java.util.*;

public class Tree {
	public static List<String>[] links(String url) {
		List<String> filter_words = Arrays.asList("&sort", "help.php?language=el&topic=documents", "#collapse0",
				"info/terms.php", "info/privacy_policy.php", "announcements/?course=",
				"/courses", "/?course=", "https://", "&openDir=%",
				"help.php?language=en&", "topic=documents&subtopic", "creativecommons.org/licenses", "main/",
				"#collapse1", "#", "modules/auth/lostpass.php", "modules/course_metadata/openfaculties.php",
				"modules/usage/", "modules/message", "modules/announcements", "modules/help/", "index.php?logout=yes");

		List<String> files = new ArrayList<>();
		List<String> directories = new ArrayList<>();
		@SuppressWarnings("unchecked")
		List<String>[] array = new ArrayList[2];

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

		linktest: for (int i = 0; i < links.size(); i++) {
			Element link = links.get(i);
			String href = link.attr("href");
			for (int j = 0; j < filter_words.size(); j++) {
				String filter_word = filter_words.get(j);
				if (href.contains(filter_word)) {
					continue linktest;
				}
			}
			if (!href.contains("http") & !href.equals("/")) {
				href = "https://eclass.aueb.gr" + href;
				if (!(href + " ").contains("&openDir=/ ") & !(href + " ").contains("&openDir= ") & !href.equals(url)) {

					if (href.substring(href.length() - 6).contains(".")) {
						files.add(href);
					} else if (!href.contains("&download=/")) {
						directories.add(href);
					}
				}
			}
		}

		array[0] = files;
		array[1] = directories;

		return array;
	}

	public static class Node implements Serializable {
		public String parent;
		public List<Node> directoryChildren = new ArrayList<>();
		public List<String> fileChildren = new ArrayList<>();

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
		}
	}

	public static Node gen(String url) {
		List<String>[] array = links(url);
		List<String> files = array[0];
		List<String> directories = array[1];

		Node root = new Node();
		root.parent = url;
		root.fileChildren = files;

		for (int i = 0; i < directories.size(); i++) {
			String directory = directories.get(i);
			root.directoryChildren.add(gen(directory));
		}

		return root;
	}

	public static void print(Node root, String prefix) {
		System.out.println(prefix + "\t" + root.parent);
		String branch_prefix = prefix + "\t";

		for (int i = 0; i < root.directoryChildren.size(); i++) {
			Node child = root.directoryChildren.get(i);
			print(child, branch_prefix);
		}

		for (int i = 0; i < root.fileChildren.size(); i++) {
			String file = root.fileChildren.get(i);
			System.out.println(branch_prefix + '\t' + file);
		}
	}

	public static void save(Node root, int CourseNum) {
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(CourseNum+".ser");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		try {
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(root);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Node load(String filename) {
		Node root = null;
		try {
			FileInputStream fileInputStream = new FileInputStream(filename);
			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
			root = (Node) objectInputStream.readObject();
			objectInputStream.close();
			fileInputStream.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			root = new Node();
		}
		return root;
	}

	public static void diffChildren(Node previous, Node latest) {
		for (String file : previous.fileChildren) {
			if (!latest.fileChildren.contains(file)) {
				System.out.println(file + " deleted!");
			}
		}
		for (String file : latest.fileChildren) {
			if (!previous.fileChildren.contains(file)) {
				System.out.println(file + " added!");
			}
		}
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
				allDirectories.remove(directory);
			}
		}
		for (String directory : newDirectoryChildren.keySet()) {
			if (!oldDirectoryChildren.keySet().contains(directory)) {
				System.out.println(directory + " added!");
				allDirectories.remove(directory);
			}
		}
		for (String directory : allDirectories) {
			diff(oldDirectoryChildren.get(directory), newDirectoryChildren.get(directory));
		}
		diffChildren(previous, latest);
	}

	public static String getCookie() {
		String cookie = null;
		try {
			FileInputStream fileInputStream = new FileInputStream("cookie.ser");
			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
			cookie = (String) objectInputStream.readObject();
			objectInputStream.close();
			fileInputStream.close();
		} catch (IOException e) {
			updateCookie();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return cookie;
	}

	public static void updateCookie() {
		String username, password, cookie = "";

		File file = new File("credentials.txt");
		Scanner scanner = null;
		try {
			scanner = new Scanner(file);
			username = scanner.nextLine().trim();
			password = scanner.nextLine().trim();
			scanner.close();
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}

		try {
			Connection.Response response = Jsoup.connect("https://eclass.aueb.gr/main/login_form.php")
					.method(Connection.Method.GET).execute();
			Document doc = response.parse();

			cookie = response.cookie("PHPSESSID");

			Connection connection = Jsoup.connect("https://eclass.aueb.gr/?login_page=1")
					.method(Connection.Method.POST)
					.cookie("PHPSESSID", cookie)
					.data("uname", username)
					.data("pass", password)
					.data("submit", "Είσοδος");

			Connection.Response loginResponse = connection.execute();

		} catch (IOException e) {
			e.printStackTrace();
		}

		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream("cookie.ser");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		try {
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(cookie);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) {
		List<Integer> CourseNumbers = Arrays.asList(453, 169, 482, 176, 358, 157);
		for (int CourseNum : CourseNumbers) {
			String url =  "https://eclass.aueb.gr/modules/document/index.php?course=INF" + CourseNum;
			Node oldRoot = load(CourseNum+".ser");
			Node newRoot = gen(url);
			diff(oldRoot, newRoot);
			save(newRoot, CourseNum);
		}
	}
}