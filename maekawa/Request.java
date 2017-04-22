package finalProject;


public class Request implements Comparable<Request> {
	private ServerComm server;
	private int id, ts;
	private String request;
	
	private boolean isVoid = false;
	
	public Request(int id, int ts, ServerComm s, String _request) {
		this(id, ts);
		server = s;
		request = _request;
	}
	public Request(int id, int ts) {
		this.id = id;
		this.ts = ts;
	}
	public Request() {
		isVoid = true;
	}
	public String toString() {
		return id + " " + ts;
	}
	public ServerComm getServer() {
		return server;
	}
	public String getRequestString() {
		return request;
	}
	@Override
	public int compareTo(Request arg0) {
		return ts - (arg0).ts;
	}
	public int getTS() {
		return ts;
	}
	public int getID() {
		return id;
	}
	public boolean isVoid() {
		return isVoid;
	}
}
