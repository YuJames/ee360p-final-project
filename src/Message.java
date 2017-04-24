package finalProject;

import java.util.Scanner;

public class Message {
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
