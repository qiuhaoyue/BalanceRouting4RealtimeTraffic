import java.util.ArrayList;


public class Event {
	public double utc;
	public int type; 
	public int trip_seq;
	public int gid;
	
	long suid;
	long trip_utc;
	
	//public ArrayList<Integer> gid_list;
	//double end_pos;
	
	public static final int REMOVE_LINK=0;
	public static final int ROUTING_REQUEST=1;
	public static final int ADD_LINK=2;
	

	//for ROUTING_REQUEST
	Event(long suid, long trip_utc, double utc, int type, int pointer){
		this.suid=suid;
		this.trip_utc=trip_utc;
		
		this.utc=utc;
		this.type=type;
		
		if(type==REMOVE_LINK || type==ADD_LINK){
			this.gid=pointer;
		}
		else{
			this.trip_seq=pointer;
		}
		//gid_list=new ArrayList<Integer>();
	}
	
	//for REMOVE_LINK
	/*
	Event(long suid, long trip_utc, double utc, int type, int gid){
		this.suid=suid;
		this.trip_utc=trip_utc;
		
		this.utc=utc;
		this.type=type;
		this.gid=gid;
		//gid_list=new ArrayList<Integer>(roadlist);
		//end_pos=last_pos;
	}*/
	void print(){
		String output="<";
		output+=(long)(this.utc);
		if(type==REMOVE_LINK){
			output+=">	REMOVE_LINK:"+this.gid;
			
		}
		else{
			output+=">	ROUTING_REQUEST:"+this.trip_seq;
		}
		
		System.out.println(output);
	}
}


