import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server {
	// what exactly is syncing?
	private static AtomicBoolean syncing;
	
	// static classes
	public static class ServerThread extends Thread {
		private ArrayList<String[]> invent;
	  	private ArrayList<String[]> orders;
	  	private ArrayList<String[]> orders2;
	  	private InetSocketAddress[] servers;
		private Socket s = null;
		private int myID;
		private Lamport lamp;
		private Integer orderid;
		
	  	public ServerThread(ArrayList<String[]> invent, 
	  						ArrayList<String[]> orders,
	  						ArrayList<String[]> orders2,
	  						int orderid, 
	  						Socket s, 
	  						InetSocketAddress[] servers, 
	  						int myID, 
	  						Lamport lamp) {
	  		this.invent = invent;
	  		this.s = s;
	  		this.orders = orders;
	  		this.orderid = orderid;
	  		this.orders2 = orders2;
	  		this.servers = servers;
	  		this.myID = myID;
	  		this.lamp = lamp;
		}
	  	
	  	private void syncInvent() {
			for (SocketAddress done : servers) {
				if(done.equals(servers[myID]))
					continue;
				Socket s = new Socket();
				try {
					s.setSoTimeout(500);
					s.connect(done);

					Scanner sc = new Scanner(s.getInputStream());
					OutputStream os = s.getOutputStream();
					PrintWriter out = new PrintWriter(os);

					out.println("syncinvent");
					String outs = "";
					for (int i = 0; i < invent.size(); i++) {
						for(int j = 0; j < invent.get(i).length; j++){
							if(j != 0)
								outs = outs + " ";
							outs = outs + invent.get(i)[j];
						}
						out.println(outs);
						outs = "";
					}
					out.flush();

					sc.close();

					return;
				} catch (IOException e) {} finally {
					try {
						s.close();
					} catch (IOException e) {}
				}
			}
		}
	  	private void syncOrders1() {
			for (SocketAddress done : servers) {
				if(done.equals(servers[myID]))
					continue;
				Socket s = new Socket();
				try {
					s.setSoTimeout(500);
					s.connect(done);

					Scanner sc = new Scanner(s.getInputStream());
					OutputStream os = s.getOutputStream();
					PrintWriter out = new PrintWriter(os);

					out.println("syncorders");
					String outs = "";
					for (int i = 0; i < orders.size(); i++) {
						for(int j = 0; j < orders.get(i).length; j++){
							if(j != 0)
								outs = outs + " ";
							outs = outs + orders.get(i)[j];
						}
						out.println(outs);
						outs = "";
					}
					out.flush();

					sc.close();

					return;
				} catch (IOException e) {} finally {
					try {
						s.close();
					} catch (IOException e) {}
				}
			}
		}
	  	private void syncOrders2() {
			for (SocketAddress done : servers) {
				if(done.equals(servers[myID]))
					continue;
				Socket s = new Socket();
				try {
					s.setSoTimeout(500);
					s.connect(done);

					Scanner sc = new Scanner(s.getInputStream());
					OutputStream os = s.getOutputStream();
					PrintWriter out = new PrintWriter(os);

					out.println("syncorders2");
					String outs = "";
					for (int i = 0; i < orders2.size(); i++) {
						for(int j = 0; j < orders2.get(i).length; j++){
							if(j != 0)
								outs = outs + " ";
							outs = outs + orders2.get(i)[j];
						}
						out.println(outs);
						outs = "";
					}
					out.flush();

					
					sc.close();

					return;
				} catch (IOException e) {} finally {
					try {
						s.close();
					} catch (IOException e) {}
				}
			}
		}
		public synchronized int purchase(String username, String product, int quantity, PrintWriter pout){
			int id = orders2.size() + 1;
			for(int i = 0; i < invent.size(); i++){
				if(invent.get(i)[0].equals(product)){
					int inventquantity = Integer.parseInt(invent.get(i)[1]);
					if(inventquantity >= quantity){
						System.out.println("You order has been placed, " 
								+ id + " " + username + " " + product + " " + quantity);
						pout.println("You order has been placed, " 
								+ id + " " + username + " " + product + " " + quantity);
						String ordernum = String.valueOf(id);
						String orderquant = String.valueOf(quantity);
						String[] order = new String[]{ordernum,username,product,orderquant};
						invent.get(i)[1] = String.valueOf(inventquantity-quantity);
						orders.add(order);
						orders2.add(order);
						syncInvent();
						syncOrders1();
						syncOrders2();
						return 0;
					}
					else{
						System.out.println("Not Available - Not enough items");
						pout.println("Not Available - Not enough items");
						return 1;
					}
				}
			}
			System.out.println("Not Available - We do not sell this product");
			pout.println("Not Available - We do not sell this product");
			return 2;
		}
		public synchronized int cancel(String id, PrintWriter pout){
			for(int i = 0; i < orders.size(); i++){
				if(orders.get(i)[0].equals(id)){
					String product = orders.get(i)[2];
					int orderquant = Integer.parseInt(orders.get(i)[3]);
					for(int j = 0; j < invent.size(); j++){
						if(invent.get(j)[0].equals(product)){
							int inventquant = Integer.parseInt(invent.get(j)[1]);
							invent.get(j)[1] = String.valueOf(inventquant+orderquant);
						}
					}
					orders.remove(i);
					System.out.println("Order " + id + " is canceled");
					pout.println("Order " + id + " is canceled");
					syncInvent();
					syncOrders1();
					syncOrders2();
					return 0;
				}
			}
			System.out.println(id +" not found, no such order");
			pout.println(id +" not found, no such order");
			return 1;
		}
		public synchronized int search(String username, PrintWriter pout){
			int count = 0;
			for(int i = 0; i < orders.size(); i++){
				if(orders.get(i)[1].equals(username)){
					System.out.println(orders.get(i)[0] + " " + orders.get(i)[2] + " "
							+ orders.get(i)[3]);
					pout.println(orders.get(i)[0] + " " + orders.get(i)[2] + " "
							+ orders.get(i)[3]);
					count ++;
				}
			}
			if(count == 0){
				System.out.println("No order found for " + username);
				pout.println("No order found for " + username);
				return 1;
			}
			else{
				return 0;
			}
		}
		public synchronized void list(PrintWriter pout){
				for(int i = 0; i < invent.size(); i++){
					System.out.println(invent.get(i)[0] + " " + invent.get(i)[1]);
					pout.println(invent.get(i)[0] + " " + invent.get(i)[1]);
				}
			}
			
		public void run() {
			while (syncing.get()) {
				//wait
				try { Thread.sleep(10);
				} catch (InterruptedException e) {}
			}
			Scanner sc = null;
			try {
				OutputStream os = null;
				PrintWriter pout;
				if (s != null) {
					sc = new Scanner(s.getInputStream());
					os = s.getOutputStream();
				} 
				pout = new PrintWriter(os);

				try {
					
					String command = sc.next();
					if (command.equals("purchase")) {
						lamp.request();
						String username = sc.next();
						String product = sc.next();
						String quant = sc.next();
						int quantity = Integer.parseInt(quant);
						purchase(username,product,quantity, pout);
						lamp.release(username + " " + product + " " + quant);
					} else if (command.equals("cancel")) {
						lamp.request();
						String id = sc.next();
						cancel(id,pout);
						lamp.release(id);
					} else if(command.equals("search")){
						String username = sc.next();
						search(username, pout);
					}else if(command.equals("list")){
						//syncing.set(true);
						list(pout);
						//syncing.set(false);
					}
					else if(command.equals("lamport")){
						String id = sc.next();
						lamp.receive(Integer.parseInt(id),sc);
					}else if (command.equals("ack")) {
						String id = sc.next();
						lamp.recvAck(Integer.parseInt(id));
					}else if (command.equals("release")) {
						String id = sc.next();
						lamp.clean(Integer.parseInt(id));
					}else if (command.equals("syncinvent")) {
						syncing.set(true);
						invent.clear();
						sc.nextLine();
						while (sc.hasNext()) {
							
							String[] tokens = sc.nextLine().split(" ");
							invent.add(tokens);
						}
						syncing.set(false);
					}
					else if (command.equals("syncorders")) {
						syncing.set(true);
						orders.clear();
						sc.nextLine();
						while (sc.hasNext()) {
							String[] tokens = sc.nextLine().split(" ");
							orders.add(tokens);
						}
						syncing.set(false);
					}
					else if (command.equals("syncorders2")) {
						syncing.set(true);
						orders2.clear();
						sc.nextLine();
						while (sc.hasNext()) {
							String[] tokens = sc.nextLine().split(" ");
							orders2.add(tokens);
						}
						syncing.set(false);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				pout.flush();
				if (s != null) {
					s.close();
				} 
				pout.close();
				sc.close();
			} catch (Exception e) {
				// do nothing
				e.printStackTrace();
			}
		}
  	}
	public static class Lamport {
		// members
		private SocketAddress[] servers;
		private PriorityQueue<Request> requests;
		private boolean[] ack;
		private Clock cl;
		private int id;

		// constructors
		public Lamport(SocketAddress[] servers, int self) {
			this.servers = servers;
			this.requests = new PriorityQueue<Request>();
			this.ack = new boolean[servers.length];
			this.cl = new Clock(servers.length, id);
			this.id = self;
		}

		// methods
		public synchronized void request() {
			// init to have no acknowledgements yet
			Arrays.fill(this.ack, false);
			
			// add own request to queue
			requests.add(new Request(this.id, this.cl.c[id]));
			// own acknowledgement 
			this.ack[id] = true;
			// why this?
			notifyAll();
			// go through all other servers (except this) and send them my vector clock
			for (int i = 0; i < this.servers.length; i++) {
				if (i == this.id) continue;
				SocketAddress addr = this.servers[i];
				Socket s = new Socket();
				try {
					// connect to other servers and send them this vector clock
					s.connect(addr);
					OutputStream os = s.getOutputStream();
					PrintWriter out = new PrintWriter(os);
					out.println("lamport " + this.id);
					for (int ts : this.cl.send()) {
						out.println(ts);
					}
					out.flush();
				} catch (IOException e) {
					e.printStackTrace();
					// why this?
					ack[i] = true;
				} finally {
					try {
						s.close();
					} catch (IOException e) { e.printStackTrace(); }
				}
			}
			try {
				while (checkWait()) {
					this.wait();
				}
			} catch (InterruptedException e) { e.printStackTrace(); }
		}
		private boolean checkWait() {
			if (requests.peek() != null && requests.peek().id != id) {
				return true;
			}
			for (int i = 0; i < ack.length; i++) {
				if (!(ack[i])) return true;
			}
			return false;
		}
		public synchronized void recvAck(int sid) {
			ack[sid] = true;
			this.notifyAll();
		}
		public synchronized void sendAck(int sid) {
			SocketAddress addr = servers[sid];
			Socket c = new Socket();
			try {
				c.connect(addr);
				OutputStream os = c.getOutputStream();
				PrintWriter out = new PrintWriter(os);
				out.println("ack " + id);
				for (int ts : cl.send()) {
					out.println(ts);
				}
				out.flush();
			} catch (IOException e) { 
				e.printStackTrace(); 
			} 
			finally {
				try {
					c.close();
				} catch (IOException e) { 
					e.printStackTrace(); 
				}
			}
		}
		public synchronized void receive(int sid, Scanner in) {
			int[] rec = new int[servers.length];
			for (int i = 0; i < rec.length; i++) {
				rec[i] = in.nextInt();
			}
			cl.recieve(rec);
			requests.add(new Request(sid, cl.c[sid]));

			sendAck(sid);
			this.notifyAll();
		}
		public synchronized void release(String msg) {
			for (int i = 0; i < servers.length; i++) {
				SocketAddress addr = servers[i];
				Socket c = new Socket();
				try {
					c.connect(addr);
					OutputStream os = c.getOutputStream();
					PrintWriter out = new PrintWriter(os);
					out.println("release " + id + " " + msg);
					out.flush();
				} catch (IOException e) { e.printStackTrace(); } finally {
					try {
						c.close();
					} catch (IOException e) { e.printStackTrace(); }
				}
			}
		}
		public synchronized void clean(int sid) {
			Iterator<Request> it = requests.iterator();
			Request req = null;
			while (it.hasNext()) {
				Request e = it.next();
				if (e.id == sid) {
					req = e;
					break;
				}
			}
			if (req != null) {
				requests.remove(req);
			} else {
				//nothing to clean
			}
		}

		// nested classes
		public static class Clock {
			private int size, id;
			private int[] c;
			
			public Clock(int instances, int self) {
				this.size = instances;
				this.id = self;
				this.c = new int[size];
				this.c[id] = 1;
			}
			void internal() {
				this.c[id]++;
			}
			void recieve(int[] r) {
				for (int i = 0; i < size; i++) {
					if (r[i] > this.c[i]) {
						this.c[i] = r[i];
					}
				}
				this.c[id]++;
			}
			int[] send() {
				this.c[id]++;
				return this.c;
			}
		}
		public static class Request {
			private int id, ts;
			public Request(int id, int ts) {
				this.id = id;
				this.ts = ts;
			}
			public String toString() {
				return id + " " + ts;
			}
		}
	}
	public static class Runner implements Runnable {
		private ArrayList<String[]> invent;
		private ArrayList<String[]> orders;
		private ArrayList<String[]> orders2;
		private InetSocketAddress[] servers;
		private Socket s = null;
		private int myID;
		private Lamport lamp;
		private Integer orderid;
		public Runner(ArrayList<String[]> invent,
					  ArrayList<String[]> orders,
					  ArrayList<String[]> orders2,
					  int orderid, 
					  InetSocketAddress[] servers, 
					  int myID) {
	  		this.invent = invent;
	  		this.s = s;
	  		this.orders = orders;
	  		this.orderid = orderid;
	  		this.orders2 = orders2;
	  		this.servers = servers;
	  		this.myID = myID;
	  		this.lamp = new Lamport(servers,myID);
		}
			
		public void run() {
			ServerSocket server = null;
			try {
				server = new ServerSocket(servers[myID].getPort());
				Socket s;
				// listens for a connection, and makes a thread to handle it
				while ((s = server.accept()) != null) {
					/* TCP requires a socket (which gives two IO streams) */
					Thread t = new ServerThread(invent,orders,orders2,orderid, s, servers,myID,lamp);
					t.start();
				}
			} catch (Exception e) { e.printStackTrace(); } finally {
				if (server != null) {
					try {
						server.close();
					} catch (IOException e) { e.printStackTrace(); }
				}
			}
		}
	}

	public static void main (String[] args) {
	    Scanner sc = new Scanner(System.in);
	    final int myID = sc.nextInt();
	    int numServer = sc.nextInt();
	    final InetSocketAddress[] servers = new InetSocketAddress[numServer];
	    String inventoryPath = sc.next();
	    final ArrayList<String[]> inventory = new ArrayList<String[]>();
	    final ArrayList<String[]> orders = new ArrayList<String[]>();
	    final ArrayList<String[]> orders2 = new ArrayList<String[]>();
	    final int orderid = 0;
	    syncing = new AtomicBoolean(false);
	    System.out.println("[DEBUG] my id: " + myID);
	    System.out.println("[DEBUG] numServer: " + numServer);
	    System.out.println("[DEBUG] inventory path: " + inventoryPath);
	    Scanner scanner;
	    File file = new File(inventoryPath);
		try {
			scanner = new Scanner(file);
			 while(scanner.hasNext()){
				 String[] tokens = scanner.nextLine().split(" ");
				 inventory.add(tokens);
			 }
			 scanner.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    for (int i = 0; i < numServer; i++) {
	    	// TODO: parse inputs to get the ips and ports of servers
	    	String str = sc.next();
	    	String serverAddr[] = str.trim().split(":");
	    	servers[i] = new InetSocketAddress(serverAddr[0], Integer.parseInt(serverAddr[1]));
	    	System.out.println("address for server " + i + ": " + str);
	    }
	    sc.close();
	    Thread t = new Thread(new Runner(inventory, orders,orders2,orderid, servers,myID-1));
		/*Thread tcp = new Thread(new Runnable() {
			public void run() {
				try {
					ServerSocket listener = new ServerSocket(servers[myID-1].getPort());
					Socket s;
					while ((s = listener.accept()) != null) {
						System.out.println("Server " + myID);
						System.out.println("New TCP connection from " + s.getInetAddress());
						Thread t = new Thread(new Runner(inventory, orders,orders2,orderid, s,servers,myID-1));
						t.start();
					}
					listener.close();
				} catch (Exception e) { }
			}
		});*/
		t.start();
	    // TODO: start server socket to communicate with clients and other servers
	    
	    // TODO: parse the inventory file
	
	    // TODO: handle request from client
	}
}