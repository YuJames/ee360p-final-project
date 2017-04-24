package finalProject;

public interface Lock {
	public void requestCS();
	public void requestCS(String s);
	
	public void releaseCS();
	public void releaseCS(String s);
}
