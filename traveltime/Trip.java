import java.util.ArrayList;

public class Trip {
	
	public int index;
	public long suid;
	public int start_gid;
	public double start_pos;
	public long start_utc;
	
	public int end_gid;
	public double end_pos;
	public long end_utc;
	public ArrayList<Integer> path;
	
	public double cost;
	
	Trip(){
		this.suid=-1;
		this.start_gid=-1;
		this.start_pos=-1;
		this.start_utc=-1;
		
		this.end_gid=-1;
		this.end_pos=-1;
		this.end_utc=-1;
		
		path=new ArrayList<Integer>();
		this.cost=-1;
	}
	
	Trip(long suid, int start_gid, double start_pos, long start_utc, int end_gid, double end_pos){
		this.suid=suid;
		this.start_gid=start_gid;
		this.start_pos=start_pos;
		this.start_utc=start_utc;
		
		this.end_gid=end_gid;
		this.end_pos=end_pos;
		this.end_utc=-1;
		
		this.path=new ArrayList<Integer>();
		this.cost=-1;
	}
	
	Trip(long suid, long start_utc, double cost){
		this.suid=suid;
		this.start_utc=start_utc;
		this.cost=cost;
		this.path=new ArrayList<Integer>();
	}
	
}