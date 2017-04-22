package finalProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Scanner;

public class Maekawa implements Lock, Messenger {
	public static boolean debug = true;
	
	DirectClock v;
	
	private boolean voted = false;
	PriorityQueue<Request> requests;
	Request current = null; // may not need
	
	ArrayList<ArrayList<ServerComm>> grid = new ArrayList<ArrayList<ServerComm>>();
	int myRow, myCol, myID;
	ServerComm me;
	
	List<ServerComm> myQuorum = new LinkedList<ServerComm>();
	List<ServerComm> everyone;
	
	public Maekawa() {
		// establish connection with all other servers
		
	   Scanner sc = new Scanner(System.in);
		//Scanner sc = null;
//		try {
//			sc = new Scanner(new File("input/maekawa_servers.txt"));
//		} catch (FileNotFoundException e2) {
//			// TODO Auto-generated catch block
//			e2.printStackTrace();
//		}
	    myID = sc.nextInt() - 1;
	    int numServer = sc.nextInt();
	    requests = new PriorityQueue<Request>(numServer);
	    
	    v = new DirectClock(numServer, myID);

	    System.out.println("[DEBUG] my id: " + myID);
	    System.out.println("[DEBUG] numServer: " + numServer);
	    
	    ArrayList<String> ips = new ArrayList<String>(numServer);
	    ArrayList<Integer> ports = new ArrayList<Integer>(numServer);
	    for (int i = 0; i < numServer; i++) {
	      // TODO: parse inputs to get the ips and ports of servers
	       String str = sc.next();
	       System.out.println("address for server " + i + ": " + str);
	    	if(Maekawa.debug)
	    		System.out.println("got: " + str);
	    	String[] tokens = str.split(":");
	    	ips.add(tokens[0]);
	    	ports.add(Integer.parseInt(tokens[1]));
	    }
	  
	    // start server socket to communicate with clients and other servers
	    List<ServerComm> serverComms = new ArrayList<ServerComm>(numServer);
	    for(int i = 0; i < numServer; i++)
	    	serverComms.add(new ServerComm());
	    me = serverComms.get(myID);
	    // set up this server
	    // serverComms.set(myID, new ServerComm(ips.get(myID), ports.get(myID)));
	    ServerSocket listener = null;
	    try {
			listener = new ServerSocket(ports.get(myID));
			// accept connections from servers with lower ids
			for(int i = 0; i < myID; i++) {
				if(Maekawa.debug)
					System.out.println("waiting for server with lower id");
		    	Socket otherServer = listener.accept();
		    	ServerComm comm = new ServerComm(otherServer);
		    	// find out id of this server
		    	int n = Integer.parseInt(comm.dataIn.readLine());
		    	serverComms.set(n, comm);
		    	if(Maekawa.debug)
		    		System.out.println("server " + myID + " accepted server " + n);
		    }
		    // contact servers with larger ids
		    for(int i = myID + 1; i < numServer; i++) {
		    	if(Maekawa.debug)
		    		System.out.println("contacting servers with larger ids");
		    	while(true) {
			    	try {
						Socket otherServer = new Socket(ips.get(i), ports.get(i));
						ServerComm comm = new ServerComm(otherServer);
						serverComms.set(i, comm);
						comm.dataOut.println(myID);
						if(Maekawa.debug)
				    		System.out.println("server " + myID + " contacted server " + i);
						break;
			    	} catch(IOException e) {
			    		// keep trying until contact is successful
//			    		if(Maekawa.debug)
//			    			System.out.println("reattempt to connect to server with higher id");
			    		continue;
			    	}
		    	}
		    }
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	    everyone = serverComms;
	    
	    // create the grid
	    int numPerRow = (int) Math.sqrt(numServer);
	    int numRows = (int) Math.ceil((double) numServer / numPerRow);
	    int serverCommIndex = 0;
	    for(int row = 0; row < numRows; row++) {
	    	ArrayList<ServerComm> rowList = new ArrayList<ServerComm>();
	    	for(int col = 0; col < numPerRow; col++) {
	    		ServerComm s = serverComms.get(serverCommIndex++);
	    		if(me.equals(s)) {
	    			myRow = row;
	    			myCol = col;
	    		}
	    		rowList.add(s);
	    	}
	    	grid.add(rowList);
	    }
	    if(Maekawa.debug) {
	    	for(int i = 0; i < grid.size(); i++) {
	    		System.out.println("row " + i + " has " + grid.get(i).size() + " cols");
	    	}
	    }
	    
	    for(int i = 0; i < serverComms.size(); i++) {
	    	if(i != myID) {
	    		serverComms.get(i).set(this);
	    		new Thread(serverComms.get(i)).start();
	    	}
	    }
	    
	}

	@Override
	public synchronized void requestCS() {
		Message m = new Message(myID, "request", v.getValue(myID), "");
		// request CS from our row
		for(int col = 0; col < grid.get(myRow).size(); col++) {
			v.sendAction();
			if(myCol != col) {
				m.timeStamp = v.getValue(myID);
				grid.get(myRow).get(col).sendMsg(m);
			}
			else {
				Request r = new Request(myID, v.getValue(myID), me, "request");
				v.sendAction();
				m.timeStamp = v.getValue(myID);
				handleMessage(m);
			}
		}
		// request CS from everyone in column under us
		for(int row = myRow - 1; row >= 0; row--) {
			if(Maekawa.debug) {
				System.out.println("my row " + myRow);
				System.out.println("contacting row, col " + row + " " + myCol);
			}
			v.sendAction();
			m.timeStamp = v.getValue(myID);
			grid.get(row).get(myCol).sendMsg(m);
		}
		while(!okayCS()) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void requestCS(String s) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void releaseCS() {
		// TODO Auto-generated method stub
		Message m = new Message(myID, "release", -1, "");
		for(ServerComm s: myQuorum) {
			v.sendAction();
			if(!s.equals(me)) {
				m.timeStamp = v.getValue(myID);
				s.sendMsg(m);
			}
			else {
				// my own request
				current = requests.poll();
				if(current != null) {
					// let them know they have our vote now
					v.sendAction();
					Message msg = new Message(myID, "success", v.getValue(myID), "");
					current.getServer().sendMsg(msg);
				}
				else {
					voted = false;
				}
			}
		}
		myQuorum.clear();
	}

	@Override
	public void releaseCS(String s) {
		// TODO Auto-generated method stub
		
	}
	
	private boolean okayCS() {
		int currentVotes = myQuorum.size();
		int requiredVotes = grid.get(myRow).size() + myRow;
		boolean enough = currentVotes == requiredVotes;
		if(Maekawa.debug && !enough) {
			System.out.println("not enough votes");
		}
		return enough;
	}
	


	@Override
	public synchronized void handleMessage(Message m) {
		
		if(m.tag.equals("request")) {
			
			
			Request r = new Request(m.id, m.timeStamp, everyone.get(m.id), m.message);
			requests.add(r);
			
			if(current == null) {
				current = requests.poll();
				
				if(current.getID() != myID) {
					if(Maekawa.debug) {
						System.out.println("notifying new request");
					}
					v.sendAction();
					Message reply = new Message(myID, "success", v.getValue(myID), "");
					everyone.get(m.id).sendMsg(reply);
				}
				else {
					if(Maekawa.debug) {
						System.out.println("added ourself to quorum");
					}
					myQuorum.add(current.getServer());
				}
			}
		}
		else if(m.tag.equals("release")) {
			current = requests.poll();
			if(current != null) {
				v.sendAction();
				if(current.getID() != myID) {
					Message reply = new Message(myID, "success", v.getValue(myID), "");
					everyone.get(current.getID()).sendMsg(reply);
				}
				else {
					myQuorum.add(me);
					notify();
				}
			}
			else {
				voted = false;
			}
		}
		else if(m.tag.equals("success")) {
			myQuorum.add(everyone.get(m.id));
			notify();
		}
		else if(m.tag.equals("fail")) {
			
		}
		else if(m.tag.equals("inquire")) {
			
		}
		
	}

	
	
	
	
	
	
	
	
	
	public static void main(String[] args) {
		//GenerateServerInfo.go(4, "localhost", 8025);
		Maekawa mutex = new Maekawa();
		
		for(int i = 0; i < 10; i++) {
			mutex.requestCS();
			
			System.out.println("iteration: " + i);
			logTime(mutex.myID, System.nanoTime());
			
			mutex.releaseCS();
		}
		//while(true) {}
	}
	private static void logTime(int id, long time) {
		System.out.println("server " + id + ": " + time + " ns");
	}
	
}
