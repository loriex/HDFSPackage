import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.lang.Runtime;

public class User {
	public static void main(String[] args) throws IOException {
		Scanner scan = new Scanner(System.in);
		HDFSPackage pack = null;
		while (scan.hasNextLine()) {
			String str = scan.nextLine();
			String[] strs = str.split(" ");
			
			strs[0] = strs[0].toUpperCase();
			
			if (strs[0].equals("ENTER")) {
				if (pack != null)
					pack.close();
				pack = new HDFSPackage(strs[1]);
			}
			else if (strs[0].equals("LIST")) {
				if (strs.length > 1 && strs[1].equals("-l")) {
					Process process = Runtime.getRuntime().exec("ls -a -l");
		            InputStream in = process.getInputStream();
		            BufferedReader bs = new BufferedReader(new InputStreamReader(in));
		            String result = null;
		            while ((result = bs.readLine()) != null) {
		                System.out.println(result);
		            }
		            in.close();
		            process.destroy();
				} else {
					pack.list();
				}
			}
			else if (strs[0].equals("PUT")) {
				pack.putFilesIntoPackage(strs[1]);
			}
			else if (strs[0].equals("GET")) {
				pack.getFileFromPackage(strs[1], strs[2]);
			}
			else if (strs[0].equals("QUIT")) {
				if (pack != null)
					pack.close();
				break;
			}
		}
	}
}
