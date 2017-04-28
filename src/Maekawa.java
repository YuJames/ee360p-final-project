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
	   
	    myID = sc.nextInt();
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
		v.sendAction();
		m.timeStamp = v.getValue(myID);
		for(Site s: requestSet) {
			// send request to everyone in our quorum
			s.isActive = true;
			
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + m + "to: " + s + "\n");
			}
			if(s.server.id != myID) {
				s.server.sendMsg(m);
			}
			else {
				handleRequest(m);
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
		v.sendAction();
		for(Site s: requestSet) {
			//v.sendAction();
			releaseMessage.timeStamp = v.getValue(myID);
			s.resetFlags();
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + releaseMessage + " to:" + s + "\n");
			}
			if(s.server.id != myID) {
				s.server.sendMsg(releaseMessage);
			}
			else {
				handleMessage(releaseMessage);
			}
		}
		
		if(Maekawa.debug) {
			Site.printCurrent(requestSet);
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
	


	//@Override
	private synchronized void handleMsg(Message m) {
		if(Maekawa.debug) {
			System.out.println("before handling message:");
			Site.printCurrent(requestSet);
		}

		if(Maekawa.debug) {
			System.out.println(me + " received from " + everyone.get(m.id) + " : " + m + "\n");
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
			handleFail(m);
		}
		
		if(Maekawa.debug) {
			System.out.println("after handling message:");
			Site.printCurrent(requestSet);
			System.out.println("hasGranted = " + hasGranted + "\n");
		}
		
	}
	
	@Override
	public void handleMessage(Message m) {
		handleMsg(m);
	}
	
	private synchronized void handleFail(Message m) {
		Site.fail(requestSet, m.id);
	}
	
	private synchronized void handleYield(Message m) {
		requests.add(current);
		current = requests.poll();
		grantMessage.timeStamp = v.getValue(myID);
		if(Maekawa.debug) {
			System.out.println(me + " sending:" + grantMessage + " to:" + current.getID() + "\n");
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
		else if(!requests.isEmpty()) {
			Request temp = current;
			current = requests.poll();
			grantMessage.timeStamp = v.getValue(myID);
			if(temp == null) {
				
			}
			if(current.getID() != myID) {
				current.getServer().sendMsg(grantMessage);
			}
			else {
				handleGrant(grantMessage);
			}
		}
		else {
			releaseMessage.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending: " + releaseMessage + " to " + everyone.get(m.id) + "\n");
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
//		if(okayCS()) {
//			failMessage.timeStamp = v.getValue(myID);
//			if(myID != m.id) {
//				everyone.get(m.id).sendMsg(failMessage);
//			}
//			else {
//				handleFail(failMessage);
//			}
//		}
		//else if(Site.mustYield(requestSet, m.id)) {
		if(Site.mustYield(requestSet, m.id)) {
			Site.yield(requestSet, m.id);
			yieldMessage.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + yieldMessage + " to:" + current.getID() + "\n");
			}
			
			if(myID != m.id) {
				everyone.get(m.id).sendMsg(yieldMessage);
			}
			else {
				//handleMessage(yieldMessage);
				handleYield(m);
			}
		}
//		else {
//			failMessage.timeStamp = v.getValue(myID);
//			if(myID != m.id) {
//				everyone.get(m.id).sendMsg(failMessage);
//			}
//			else {
//				handleFail(failMessage);
//			}
//		}
	}
	private synchronized void handleRelease(Message m) {
		current = requests.poll();
		if(Maekawa.debug && current == null) {
			System.out.println(me + " has no requests in queue" + "\n");
		}
		if(Maekawa.debug) {
			printRequests(requests);
		}
		if(current != null) {
			if(current.getID() != myID) {
				grantMessage.timeStamp = v.getValue(myID);
				if(Maekawa.debug) {
					System.out.println(me + " sending:" + grantMessage + " to:" + current.getID() + "\n");
				}
				current.getServer().sendMsg(grantMessage);
			}
			else {
				//handleMessage(grantMessage);
				handleGrant(grantMessage);
			}
		}
		else if(Site.find(requestSet, m.id).yieldGiven) {
			Site.grant(requestSet, m.id);
			notify();
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
				System.out.println(me + " sending:" + grantMessage + " to:" + current.getID() + "\n");
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
				System.out.println(me + " sending:" + inquireMessage + " to:" + current.getID() + "\n");
			}
			if(current.getID() != myID) {
				current.getServer().sendMsg(inquireMessage);
			}
			else {
				handleInquire(inquireMessage);
			}
		}
		//else if(myID <= m.id || okayCS()) {
		else {
			failMessage.timeStamp = v.getValue(myID);
			if(Maekawa.debug) {
				System.out.println(me + " sending:" + failMessage + " to:" + r.getID() + "\n");
			}
			if(r.getID() != myID)
				r.getServer().sendMsg(failMessage);
			else
				handleFail(failMessage);
		}
//		else {
//			// if we have our own grant,
//			// and we need another grant from m.id for CS,
//			// then if m.id < my.id,
//			// yield our grant to m.id
//			Site.yield(requestSet, myID);
//			Request temp = current;
//			current = requests.poll();
//			requests.add(temp);
//			grantMessage.timeStamp = v.getValue(myID);
//			if(Maekawa.debug) {
//				System.out.println(me + " sending:" + grantMessage + " to: " + current.getServer() + "\n");
//			}
//			if(current.getID() != myID) {
//				current.getServer().sendMsg(grantMessage);
//			}
//			else {
//				handleGrant(grantMessage);
//			}
//		}
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
		if(Maekawa.debug) {
			System.out.println("ts:" + entry1 + " id:" + pid1 + 
					" ts:" + entry2 + " id:" + pid2);
		}
		return (entry1 > entry2) ||
				((entry1 == entry2) && (pid1 > pid2));
	}
	
	private void printRequests(Iterable<Request> requests) {
		for(Request r: requests) {
			System.out.println("request: " + r);
		}
	}
	

	
	
	
	
	
	
	
	
	
	public static void main(String[] args) {
		Maekawa mutex = new Maekawa();
		Scanner userInput = new Scanner(System.in);
		
//		int count = 0;
//		
//		for(count = 0; count < 3; count++) {
//			mutex.requestCS();
//			System.out.println("\n\n\n\nin CS\n\n\n\n");
//			mutex.releaseCS();
//		}
//		System.out.println("*****\n\n\n*****\ndone\n\n\n*****\n\n\n******" +
//		"****************************************************");


		
		while(true) {
			
			while(userInput.hasNextLine()) {
				String s = userInput.nextLine();
				if(!s.equals("request")) {
					continue;
				}
				else {
					break;
				}
			}
			
			mutex.requestCS();
			
			System.out.println("********\n\nin CS\n\n********");
			
			while(userInput.hasNextLine()) {
				String s = userInput.nextLine();
				if(!s.equals("release")) {
					continue;
				}
				else {
					break;
				}
			}
			
			mutex.releaseCS();
			
			System.out.println("*******\n\nexiting CS\n\n*******\n");
			
		}

	}
	private static void logTime(int id, long time) {
		System.out.println("server " + id + ": " + time + " ns");
	}
	
}

class Site {
	ServerComm server;
	boolean failReceived;
	boolean grantReceived;
	boolean yieldGiven;
	
	boolean isActive = false;
	
	public Site(ServerComm s) {
		server = s;
		failReceived = true;
		grantReceived = false;
		yieldGiven = false;
	}
	public void resetFlags() {
		failReceived = true;
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
			//if(s.server.id != skip && s.isActive) {
			if(s.isActive) {
				if(s.failReceived && !s.grantReceived) {
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
		if(site.isActive) {
			site.isActive = true;
			site.yieldGiven = true;
			site.grantReceived = false;
		}
	}
	public static void fail(List<Site> sites, int id) {
		Site site = find(sites, id);
		if(site.isActive) {
			site.resetFlags();
			site.isActive = true;
			site.failReceived = true;
			site.grantReceived = false;
		}
	}
	public static void grant(List<Site> sites, int id) {
		Site site = find(sites, id);
		if(site.isActive) {
			site.resetFlags();
			site.isActive = true;
			site.grantReceived = true;
			site.failReceived = false;
		}
	}
	public static Site find(List<Site> sites, int id) {
		if(Maekawa.debug) {
			System.out.println("findingn site: " + id);
		}
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
