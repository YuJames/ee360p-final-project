package finalProject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class GenerateServerInfo {
	private static PrintWriter writer;
	
	public static void go(int numServers, String ip, int basePort) {
		writer = null;
		File file = new File("input/maekawa_servers.txt");
		try {
//			FileWriter fw = new FileWriter(file, true);
//			writer = new PrintWriter(fw);
			writer = new PrintWriter("input/maekawa_servers.txt");
			
			// printIntro();
//			ArrayList<String> ips = new ArrayList<String>();
//			for(int i = 0; i < numServers; i++) {
//				int port = basePort + 5*i;
//				ips.add(ip + ":" + port);
//			}
			
			for(int i = 0; i < numServers; i++) {
				writer.println((i + 1) + " " + numServers);
				for(int j = 0; j < numServers; j++) {
					int port = basePort + j*5;
					writer.println(ip + ":" + port);
				}
				writer.println();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
				writer.close();
			}
		}
	}
	
	private static void printIntro() {
		DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
		Date dateobj = new Date();
		writer.println("\n\n********************************************");
		writer.println("started on " + df.format(dateobj));
		writer.println("********************************************\n");
	}
	
	public static void main(String[] args) {
		go(9, "localhost", 8025);
	}
	
}
