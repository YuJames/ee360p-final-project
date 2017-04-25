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
	
	private boolean hasGranted = false;
	PriorityQueue<Request> requests;
	Request current = null; // may not need
	
	ArrayList<ArrayList<ServerComm>> grid = new ArrayList<ArrayList<ServerComm>>();
	int myRow, myCol, myID;
	ServerComm me;
	
	List<ServerComm> myQuorum = new LinkedList<ServerComm>();
	List<ServerComm> everyone;
	
	List<Site> requestSet;
	
	Message requestMessage;
	Message grantMessage;
	Message failMessage;
	Message inquireMessage;
	Message yieldMessage;
	Message releaseMessage;
	
	boolean wantCS = false;
	
	public Maekawa() {
		// establish connection with all other servers
		
	   Scanner sc = new Scanner(System.in);
	   
	    myID = sc.nextInt() - 1;
	    int numServer = sc.nextInt();
	    requests = new PriorityQueue<Request>(numServer);
	    
	    requestMessage = new Message(myID, "request", Integer.MAX_VALUE, "");
		grantMessage = new Message(myID, "grant", Integer.MAX_VALUE, "");
		failMessage = new Message(myID, "fail", Integer.MAX_VALUE, "");
		inquireMessage = new Message(myID, "inquire", Integer.MAX_VALUE, "");
		yieldMessage = new Message(myID, "yield", Integer.MAX_VALUE, "");
		releaseMessage = new Message(myID, "release", Integer.MAX_VALUE, "");
	    
	    v = new DirectClock(numServer, myID);

	    System.out.println("[DEBUG] my id: " + myID);
	    System.out.println("[DEBUG] numServer: " + numServer);
	    
	    ArrayList<String> ips = new ArrayList<String>(numServer);
	    ArrayList<Integer> ports = new ArrayList<Integer>(numServer);
	    for (int i = 0; i < numServer; i++) {
	      // parse inputs to get the ips and ports of servers
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
	    me.setID(myID);
	    ServerSocket listener = null;
	    try {
			listener = new ServerSocket(ports.get(myID));
			// accept connections from servers with lower ids
			for(int i = 0; i < myID; i++) {
				if(Maekawa.debug)
					System.out.println("waiting for server with lower id");
		    	Socket otherServer = listener.accept();
		    	ServerComm comm = new ServerComm(otherServer, i);
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
						ServerComm comm = new ServerComm(otherServer, i);
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
	    		if(serverCommIndex >= serverComms.size()) continue;
	    		ServerComm s = serverComms.get(serverCommIndex++);
	    		if(me.equals(s)) {
	    			myRow = row;
	    			myCol = col;
	    		}
	    		rowList.add(s);
	    	}
	    	grid.add(rowList);
	    }
	    if(Maekawa.debug && myID == 0) {
	    	System.out.println("grid setting");
	    	for(int i = grid.size()-1; i <= 0; i--) {
	    		for(int j = 0; j < grid.get(i).size(); j++) {
	    			System.out.print("" + grid.get(i).get(j).toString() + " ");
	    		}
	    		System.out.println();
	    	}
	    }
	    
	    // create our quorum
	    requestSet = new ArrayList<Site>();
	    for(int col = 0; col < grid.get(myRow).size(); col++) {
	    	// add everyone in our row
			Site s = new Site(grid.get(myRow).get(col));
			requestSet.add(s);
		}
		for(int row = 0; row < grid.size(); row++) {
			// add everyone in our mod column (except ourself, since added our entire row first)
			ArrayList<ServerComm> theRow = grid.get(row);
			int rowSize = theRow.size();
			Site s;
			if(row != myRow) {
				s = new Site(theRow.get(myCol % rowSize));
				requestSet.add(s);
			}
		}
		
		if(Maekawa.debug) {
			System.out.println("*****************************");
			System.out.println("myQuorum: ");
			for(Site s: requestSet) {
				System.out.println(s);
			}
			System.out.println("*****************************");
		}
	    
		// activate channels to everyone else
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
		wantCS = true;
		for(Site s: requestSet) {
			// send request to everyone in our quorum
			s.isActive = true;
			v.sendAction();
			m.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + m + "to: " + s);
			}
			if(s.server.id != myID) {
				s.server.sendMsg(m);
			}
			else {
				handleMessage(m);
			}
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
		// not used
	}

	@Override
	public synchronized void releaseCS() {
		wantCS = false;
		for(Site s: requestSet) {
			v.sendAction();
			releaseMessage.timeStamp = v.getValue(myID);
			s.resetFlags();
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + releaseMessage + " to:" + s);
			}
			if(s.server.id != myID) {
				s.server.sendMsg(releaseMessage);
			}
			else {
				handleMessage(releaseMessage);
			}
		}
	}

	@Override
	public void releaseCS(String s) {
		// TODO Auto-generated method stub
		// not used
	}
	
	private boolean okayCS() {
		for(Site s: requestSet) {
			if(!s.grantReceived) {
				return false;
			}
		}
		return true;
	}
	


	@Override
	public synchronized void handleMessage(Message m) {
		if(Maekawa.debug) {
			System.out.println("before handling message:");
			Site.printCurrent(requestSet);
		}

		if(Maekawa.debug) {
			System.out.println(me + " received from " + everyone.get(m.id) + " : " + m);
		}
		
		v.receiveAction(m.id, m.timeStamp);
		v.sendAction();
		
		if(m.tag.equals("request")) {
			handleRequest(m);
		}
		else if(m.tag.equals("release")) {
			handleRelease(m);
		}
		else if(m.tag.equals("grant")) {
			handleGrant(m);
		}
		else if(m.tag.equals("inquire")) {
			handleInquire(m);
		}
		else if(m.tag.equals("yield")) {
			handleYield(m);
		}
		else if(m.tag.equals("fail")) {
			Site.fail(requestSet, m.id);
		}
		
		if(Maekawa.debug) {
			System.out.println("after handling message:");
			Site.printCurrent(requestSet);
		}
		
	}
	
	private synchronized void handleYield(Message m) {
		requests.add(current);
		current = requests.poll();
		grantMessage.timeStamp = v.getValue(myID);
		if(Maekawa.debug) {
			System.out.println(me + " sending:" + grantMessage + " to:" + current.getID());
		}
		if(myID != current.getServer().id) {
			current.getServer().sendMsg(grantMessage);
		}
		else {
			//handleMessage(grantMessage);
			handleGrant(grantMessage);
		}
	}
	private synchronized void handleGrant(Message m) {
		if(wantCS) {
			Site.grant(requestSet, m.id);
			notify();
		}
		else {
			releaseMessage.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending: " + releaseMessage + " to " + everyone.get(m.id));
			}
			if(m.id != myID) {
				everyone.get(m.id).sendMsg(releaseMessage);
			}
			else {
				handleRelease(m);
			}
		}
	}
	private synchronized void handleInquire(Message m) {
		if(Site.mustYield(requestSet, m.id)) {
			Site.yield(requestSet, m.id);
			yieldMessage.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + yieldMessage + " to:" + current.getID());
			}
			
			if(myID != m.id) {
				everyone.get(m.id).sendMsg(yieldMessage);
			}
			else {
				//handleMessage(yieldMessage);
				handleYield(m);
			}
		}
	}
	private synchronized void handleRelease(Message m) {
		current = requests.poll();
		if(current != null) {
			if(current.getID() != myID) {
				grantMessage.timeStamp = v.getValue(myID);
				if(Maekawa.debug) {
					System.out.println(me + " sending:" + grantMessage + " to:" + current.getID());
				}
				current.getServer().sendMsg(grantMessage);
			}
			else {
				//handleMessage(grantMessage);
				handleGrant(grantMessage);
			}
		}
		else {
			hasGranted = false;
		}
	}
	private synchronized void handleRequest(Message m) {

		Request r = new Request(m.id, m.timeStamp, everyone.get(m.id), m.message);
		requests.add(r);
		
		Request newHead = requests.peek();
		
		if(!hasGranted) {
			hasGranted = true;
			grantMessage.timeStamp = v.getValue(myID);
			current = requests.poll();
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + grantMessage + " to:" + current.getID());
			}
			if(current.getID() != myID) {
				current.getServer().sendMsg(grantMessage);
			}
			else {
				//handleMessage(grantMessage);
				handleGrant(grantMessage);
			}
		}
		else if(isGreater(current.getTS(), current.getID(), newHead.getTS(), newHead.getID())) {
			inquireMessage.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + inquireMessage + " to:" + current.getID());
			}
			if(current.getID() != myID) {
				current.getServer().sendMsg(inquireMessage);
			}
			else {
				handleInquire(inquireMessage);
			}
		}
		else if(myID < m.id || okayCS()) {
			failMessage.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + failMessage + " to:" + r.getID());
			}
			if(r.getID() != myID)
				r.getServer().sendMsg(failMessage);
			else
				handleMessage(failMessage);
		}
		else {
			// if we have our own grant,
			// and we need another grant from m.id for CS,
			// then if m.id < my.id,
			// yield our grant to m.id
			Site.yield(requestSet, myID);
			Request temp = current;
			current = requests.poll();
			requests.add(temp);
			grantMessage.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + grantMessage + " to: " + current.getServer());
			}
			if(current.getID() != myID) {
				current.getServer().sendMsg(grantMessage);
			}
			else {
				handleGrant(grantMessage);
			}
		}
	}
	
	
	private void returnVote(int id) {
		synchronized(myQuorum) {
		Iterator<ServerComm> it = myQuorum.iterator();
		while(it.hasNext()) {
			if(it.next().equals(everyone.get(id))) {
				it.remove();
				return;
			}
		}
		}
	}
	
	private void printQuorum() {
		System.out.println("current votes: ");
		for(Site s: requestSet) {
			if(s.grantReceived) {
				System.out.println(s);
			}
		}
	}
	
	private boolean isGreater(Integer entry1, int pid1, Integer entry2, int pid2) {
		if(entry2 == Integer.MAX_VALUE) return false;
		return (entry1 > entry2) ||
				((entry1 == entry2) && (pid1 > pid2));
	}
	

	
	
	
	
	
	
	
	
	
	public static void main(String[] args) {
		Maekawa mutex = new Maekawa();
		Scanner userInput = new Scanner(System.in);
		int count = 0;
		
		while(true) {
			
			if(userInput.hasNextLine()) {
				String s = userInput.nextLine();
				if(!s.equals("request")) {
					continue;
				}
			}
			
			mutex.requestCS();
			
			System.out.println("iteration: " + count++);
			logTime(mutex.myID, System.nanoTime());
			
			mutex.releaseCS();
			
			System.out.println("*****\n\n\n*****\ndone\n\n\n*****\n\n\n******" +
					"****************************************************");
			
		}

	}
	private static void logTime(int id, long time) {
		System.out.println("server " + id + ": " + time + " ns");
	}
	
}

class Site {
	ServerComm server;
	boolean failReceived = false;
	boolean grantReceived = false;
	boolean yieldGiven = false;
	
	boolean isActive = false;
	
	public Site(ServerComm s) {
		server = s;
	}
	public void resetFlags() {
		failReceived = false;
		grantReceived = false;
		yieldGiven = false;
		isActive = false;
	}
	
	@Override
	public String toString() {
		return server.toString();
	}
	
	public static boolean mustYield(List<Site> sites, int skip) {
		for(Site s: sites) {
			if(s.server.id != skip && s.isActive) {
				if(s.failReceived) {
					return true;
				}
				if(s.yieldGiven && !s.grantReceived) {
					return true;
				}
			}			
		}
		
		return false;
	}

	public static void yield(List<Site> sites, Integer id) {
		Site site = find(sites, id);
		site.isActive = true;
		site.yieldGiven = true;
		site.grantReceived = false;
	}
	public static void fail(List<Site> sites, int id) {
		Site site = find(sites, id);
		site.resetFlags();
		site.isActive = true;
		site.failReceived = true;
	}
	public static void grant(List<Site> sites, int id) {
		Site site = find(sites, id);
		site.resetFlags();
		site.isActive = true;
		site.grantReceived = true;
	}
	public static Site find(List<Site> sites, int id) {
		for(Site s: sites) {
			if(s.server.id == id) {
				return s;
			}
		}
		return null;
	}
	public static void printCurrent(List<Site> sites) {
		for(Site site: sites) {
			String s = "site " + site.toString() + ":";
			if(site.isActive) s += "active ";
			if(site.grantReceived) s += "grant ";
			if(site.failReceived) s += "fail ";
			if(site.yieldGiven) s += "yield ";
			System.out.println(s);
		}
	}
}
