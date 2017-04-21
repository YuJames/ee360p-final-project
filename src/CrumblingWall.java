import java.io.PrintStream;
import java.util.*;

import hw4.multiServerShop.Server;

public class CrumblingWall {
	public static boolean debug = true;
	
	DirectClock v;
	
	List<Row> rows;
	private int myRow;
	private int myID;
	
	private boolean voted = false;
	List<ServerComm> myQuorum = new ArrayList<ServerComm>();
	ServerComm me;
	
	Queue<Request> requests;
	int requiredVotes;
	
	public CrumblingWall
	(List<Integer> numPerRow, List<ServerComm> servers, ServerComm them, int id)
		throws IllegalArgumentException 
	{
		me = them;
		myID = id;
		rows = new ArrayList<Row>();
		for(int i = 0; i < numPerRow.size(); i++) {
			rows.add(new Row());
		}
		Iterator<ServerComm> itr = servers.iterator();
		int count = 0;
		for(Integer row: numPerRow) {
			for(int i = 0; i < row; i++) {
				if(!itr.hasNext()) { // too few servers
					throw new IllegalArgumentException();
				}
				ServerComm s = itr.next();
				if(s.equals(them)) {
					myRow = count;
					requiredVotes = row + numPerRow.size() - count - 1;
				}
				rows.get(count).add(s);
			}
			count++;
		}
		if(itr.hasNext()) // too many servers
			throw new IllegalArgumentException();
		requests = new PriorityQueue<Request>(servers.size());
		
	}
	
	public synchronized void requestCS() {
		// TODO: send request to everyone else
		
		while(!okayCS()) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void releaseCS() {
		for(ServerComm s: myQuorum) {
			if(!s.equals(me)) {
				v.sendAction();
				s.sendMsg(new Message(myID, "release", v.getValue(myID), ""));
			}
			else {
				v.tick();
				voted = false;
			}
		}
		myQuorum.clear();
		requests.poll();
	}
	
	public synchronized void handleMessage(Message m) {
		if(m.tag.equals("request")) {
			
		}
		else if(m.tag.equals("release")) {
			voted = false;
			requests.poll();
			v.sendAction();
			if(!requests.isEmpty()) {
				ServerComm s = requests.peek().getServer();
				if(!s.equals(me))
					s.dataOut.println(new Message(myRow, "ack", v.getValue(myID), "success"));
				else {
					myQuorum.add(me);
					notify();
				}
			}
		}
		else if(m.tag.equals("ack")) {
			if(m.message.equals("success")) {
				// TODO
			}
		}
	}
	
	private boolean okayCS() {
		return myQuorum.size() == requiredVotes;
	}
}

class Request implements Comparable {
	private ServerComm server;
	private int id, ts;
	public Request(int id, int ts, ServerComm s) {
		this(id, ts);
		server = s;
	}
	public Request(int id, int ts) {
		this.id = id;
		this.ts = ts;
	}
	public String toString() {
		return id + " " + ts;
	}
	public ServerComm getServer() {
		return server;
	}
	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return ts - ((Request)arg0).ts;
	}
}

class Row {
	List<ServerComm> serverComms;
	public Row() {
		serverComms = new ArrayList<ServerComm>();
	}
	public void add(ServerComm s) {
		serverComms.add(s);
	}
	public ServerComm get(int i) {
		return serverComms.get(i);
	}
	public ServerComm set(int i, ServerComm s) {
		return serverComms.set(i, s);
	}
}



class Message {
	Integer timeStamp;
	Integer id;
	String tag;
	String message;
	public Message(int _id, String _tag, int _timeStamp, String _message) {
		timeStamp = _timeStamp;
		id = _id;
		tag = _tag;
		message = _message;
	}
	public Message(String string) {
		Scanner sc = new Scanner(string);
		try {
			id = sc.nextInt();
			tag = sc.next();
			timeStamp = sc.nextInt();
			message = sc.nextLine();
		} catch(Exception e) {
			message = "XXXXXXXX  error  XXXXXXX";
		} finally {
			sc.close();
		}
	}
	@Override
	public String toString() {
		return id + " " + tag + " " +
				timeStamp + " " + message;
	}
	public boolean isActive() {
		return timeStamp < Integer.MAX_VALUE;
	}
	public void setInactive() {
		timeStamp = Integer.MAX_VALUE;
	}
}

/**************************************************************************************
 * ************************************************************************************
 * ********************  timeout class ************************************************
 * ************************************************************************************/
class Timeout implements Runnable {
	int sleepTime;
	Thread toInterrupt;
	
	public Timeout(int maxTimeMillis, Thread _toInterrupt) {
		sleepTime = maxTimeMillis;
		toInterrupt = _toInterrupt;
	}
	
	@Override
	public void run() {
		try {
			Thread.sleep(sleepTime);
			// indicates a timeout to the spawner thread
			toInterrupt.interrupt();
			if(CrumblingWall.debug)
				System.out.println("timeout duration exceeded");
		} catch (InterruptedException e) {
			// timeout duration not reached
			if(CrumblingWall.debug)
				System.out.println("timeout duration not reached");
			return;
		} catch (SecurityException e) {
			// timeout duration reached
			return;
		}
	}
	
}


/**************************************************************************************
 * ************************************************************************************
 * ********************  distributed algos ********************************************
 * ************************************************************************************/
class DirectClock {
    public int[] clock;
    int myId;
    public DirectClock(int numProc, int id) {
        myId = id;
        clock = new int[numProc];
        for (int i = 0; i < numProc; i++) clock[i] = 0;
        clock[myId] = 1;
    }
    public int getValue(int i) {
        return clock[i];
    }
    public void tick() {
        clock[myId]++;
    }
    public void sendAction() {
        // sentValue = clock[myId];
        tick();
    }
    public void receiveAction(int sender, int sentValue) {
        clock[sender] = Math.max(clock[sender], sentValue);
        clock[myId] = Math.max(clock[myId], sentValue) + 1;
        if(CrumblingWall.debug)
        	System.out.println(this);
    }
    public void clear(int id) {
    	clock[id] = Integer.MAX_VALUE;
    }
    @Override
    public String toString() {
    	String s = "";
    	for(int i = 0; i < clock.length; i++) {
    		s += "server " + i + " clock: " + clock[i] + "\n";
    	}
    	return s;
    }
}

class Heartbeat implements Runnable {
	PrintStream sendTo;
	int periodMillis;
	
	public Heartbeat(PrintStream _sendTo, int _periodMillis) {
		sendTo = _sendTo;
		periodMillis = _periodMillis;
	}
	
	@Override
	public void run() {
		if(CrumblingWall.debug)
			System.out.println("heartbeat starting");
		try {
			while(true) {
				Thread.sleep(periodMillis);
				synchronized(sendTo) {
					if(CrumblingWall.debug)
						System.out.println("heartbeat sending");
					sendTo.println("ack");
				}
			}
		} catch(InterruptedException e) {
			// heartbeat interrupted; end heartbeat
			if(CrumblingWall.debug)
				System.out.println("heartbeat ended");
		}
	}
}