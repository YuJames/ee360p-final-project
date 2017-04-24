package finalProject;

public class DirectClock {
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
