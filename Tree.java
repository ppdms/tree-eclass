import java.util.ArrayList;
import java.util.List;

public class Tree {
	public static List<String>[] links(String url) {
		List<String> files = new ArrayList<>();
		List<String> directories = new ArrayList<>();
		List<String>[] array = new ArrayList[2];
		array[0] = files;
		array[1] = directories;
		return array;
	}
	public static List<Object> gen(String url) {
		List<Object> tree = new ArrayList<>();
        tree.add(url);
        return tree;
	}
	public static void print(List<Object> tree) {

	}
	public static void main(String[] args) {
		String url = "https://eclass.aueb.gr/modules/document/?course=INF111";
		System.out.println(gen(url));	
	}
}