import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Tree {
	public static List<String>[] links(String url) {
		List<String> filter_words = Arrays.asList("&sort", "help.php?language=el&topic=documents", "#collapse0",
				"info/terms.php", "info/privacy_policy.php", "announcements/?course=",
				"/courses", "modules/document/?course=", "&openDir=%", "/?course=", "https://",
				"help.php?language=en&", "topic=documents&subtopic", "creativecommons.org/licenses");

		List<String> files = new ArrayList<>();
		List<String> directories = new ArrayList<>();
		List<String>[] array = new ArrayList[2];

		Elements links;
		try {
			Document doc = Jsoup.connect(url).get();
			links = doc.select("a[href]");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		linktest:
		for (int i = 0; i < links.size(); i++) {
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
				if (!(href+" ").contains("&openDir=/ ") & !(href+" ").contains("&openDir= ") & !href.equals(url)) {
					if (href.substring(href.length()-6).contains(".")) {
						files.add(href);
					} else {
						directories.add(href);
					}
				}
			}
		}

		array[0] = files;
		array[1] = directories;

		return array;
	}

	public static class Node {
		public String parent;
		public List<Node> directoryChildren = new ArrayList<Node>();;
		public List<String> fileChildren = new ArrayList<String>();
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

	public static void print(Node root) {
		System.out.println("\t" + root.parent);
		String branch_prefix = "\t";
		for (int i = 0; i < root.directoryChildren.size(); i++) {
			Node child = root.directoryChildren.get(i);
			Boolean is_last_child = i == root.directoryChildren.size() - 1;
			print(child, branch_prefix, is_last_child);
		}
		for (int i = 0; i < root.fileChildren.size(); i++) {
			String file = root.fileChildren.get(i);
			System.out.println(branch_prefix + '\t' + file);
		}
	}

	public static void print(Node root, String prefix, Boolean is_last) {
		System.out.println(prefix + "\t" + root.parent);
		String branch_prefix = prefix + "\t";

		for (int i = 0; i < root.directoryChildren.size(); i++) {
			Node child = root.directoryChildren.get(i);
			Boolean is_last_child = i == root.directoryChildren.size() - 1;
			print(child, branch_prefix, is_last_child);
		}

		for (int i = 0; i < root.fileChildren.size(); i++) {
			String file = root.fileChildren.get(i);
			System.out.println(branch_prefix + '\t' + file);
		}
	}

	public static void main(String[] args) {
		String url = "https://eclass.aueb.gr/modules/document/?course=INF111";
		//System.out.println(links(url));
		//System.out.println(gen(url));
		print(gen(url));
	}
}