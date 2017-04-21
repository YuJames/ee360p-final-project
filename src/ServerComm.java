import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import hw4.multiServerShop.Message;
import hw4.multiServerShop.Messenger;
import hw4.multiServerShop.Server;
import hw4.multiServerShop.TCPserver;
import hw4.multiServerShop.Timeout;
import hw4.multiServerShop.TCPserver.TCPclientHandler;

public class ServerComm implements Runnable {
	BufferedReader dataIn;
	PrintStream dataOut;
	
	Messenger messenger;
	Lock ackLock = new ReentrantLock();
	Condition ack = ackLock.newCondition();
	
	TCPserver server;
	TCPserver.TCPclientHandler handler;
	
	public ServerComm(String ip, int port) {
		try {
			InetAddress addr = InetAddress.getByName(ip);
			Socket socket = new Socket(addr, port);
			InputStreamReader isr = new InputStreamReader(socket.getInputStream());
			dataIn = new BufferedReader(isr);
			dataOut = new PrintStream(socket.getOutputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public ServerComm(Socket socket) {
		try {
			InputStreamReader isr = new InputStreamReader(socket.getInputStream());
			dataIn = new BufferedReader(isr);
			dataOut = new PrintStream(socket.getOutputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public ServerComm() {
		// simply a placeholder
	}
	
	// sends message to another client
	// returns true if reply was received within timeout
	/**
	 * sends message to another server
	 * returns true if reply was received within timeout
	 * @param msg
	 * @return
	 */
	public boolean sendMsg(Message msg) {
		ackLock.lock();
		dataOut.println(msg.toString());
		if(!msg.tag.equals("request")) {
			// no ack needed unless this server is requesting to other server
			ackLock.unlock();
			return true;
		}
		try {
			// using Timeout
			Thread t = new Thread(new Timeout(100, Thread.currentThread()));
			// should not readLine(), since readLine is used by run()
			// reply = dataIn.readLine();
			if(Server.debug)
				System.out.println("waiting for ack");
			t.start();
			ack.await();
			t.interrupt();
			if(Server.debug)
				System.out.println("ack received");
		} catch (SecurityException e) {
			// could be thrown by interrupting the Timeout
		} catch (InterruptedException e) {
			// timeout exceeded
			if(Server.debug)
				System.out.println("wait for ack timed out");
			return false;
		} finally {
			ackLock.unlock();
		}
//		catch (IOException e) {
//			// read reply failed
//			return false;
//		}
		if(Thread.currentThread().isInterrupted()) {
			if(Server.debug)
				System.out.println("wait for ack timed out (if)");
			return false;
		}
		if(Server.debug)
			System.out.println("msg has been acked");
		return true;
	}
	
	public void set(Messenger _messenger) {
		messenger = _messenger;
	}
	
	public void setServer(TCPserver _server) {
		server = _server;
		handler = _server.new TCPclientHandler();
	}
	public void updateDatabase(String command) {
		handler.executeCommand(command);
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				if(Server.debug)
					System.out.println("waiting for new message");
				String message = dataIn.readLine();
				if(Server.debug) System.out.println("reading new message with: " + message);
				Message msg = new Message(message);
				if(msg.tag.equals("ack")) {
					ackLock.lock();
					try {
						ack.signalAll();
					} finally {
						ackLock.unlock();
					}
				}
				messenger.handleMessage(msg);
			} catch (IOException e) {
				// other server is down
				if(Server.debug)
					System.out.println("server comm broken");
				break;
			}
		}
	}
	
	public void waitForAck() {
		// used to check if message timed out
		ackLock.lock();
		try {
			ack.await();
		} catch (InterruptedException e) {
			// interrupt caused by external timer
			if(Server.debug) System.out.println("timeout on waiting for ack");
		} finally {
			ackLock.unlock();
		}
	}
	
}
