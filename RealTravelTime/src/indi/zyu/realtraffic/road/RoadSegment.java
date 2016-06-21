/** 
 * 2016Äê3ÔÂ11ÈÕ 
 * RoadSegment.java 
 * author:ZhangYu
 */

package indi.zyu.realtraffic.road;

public class RoadSegment {
	public long base_gid;
	public long gid;
	public String name;
	public double time;
	public double x1;
	public double x2;
	public double y1;
	public double y2;
	public double to_cost;
	public double reverse_cost;
	public double length;
	public String geom;
	public long source;
	public long target;
	public int class_id;
	public double max_speed;
	public double avg_speed;
	public long osm_id;
    public double priority;
    public int reference;
    
    public double taxi_ratio;
    public double direction;
    public double lane_count;
    public double taxi_count;
    public double traffic_count;
    
    RoadSegment(){
    	avg_speed=-1.0;
    	max_speed=-1.0;
    	reference=0;
    	to_cost=-1.0;
    	reverse_cost=-1.0;
    	length=-1.0;
    	taxi_ratio=-1.0;
    	taxi_count=-1.0;
    }
	
	public RoadSegment(long gid, double x1, double y1, double x2, double y2){
		this.x1=x1;
		this.x2=x2;
		this.y1=y1;
		this.y2=y2;
		this.gid=gid;	
	}
	
	RoadSegment(long gid, double max_speed, double average_speed, int reference){
		this.gid=gid;
		this.avg_speed=average_speed;
	    this.max_speed=max_speed;
	    this.reference=reference;
	}
	
	RoadSegment(long gid, double average_speed, double taxi_count, double taxi_ratio){
		this.gid=gid;
		this.avg_speed=average_speed;
	    this.taxi_count=taxi_count;
	    this.taxi_ratio=taxi_ratio;
	}
	
	RoadSegment(long gid, long osm_id, int source, int target){
		this.gid=gid;
		this.osm_id=osm_id;
		this.source=source;
		this.target=target;
	}
	
	RoadSegment(long gid, double cost, double r_cost, double len, int class_id){
		this.to_cost=cost;
		this.reverse_cost=r_cost;
		this.length=len;
		this.class_id=class_id;
		this.gid=gid;	
	}
	
	RoadSegment(long gid, double x1, double y1, double x2, double y2, String geom, double lane_count){
		this.gid=gid;
		this.x1=x1;
		this.x2=x2;
		this.y1=y1;
		this.y2=y2;
		this.geom=geom;
		this.lane_count=lane_count;
		//this.gid=gid;	
	}
	
	public String toString(){
		String output="["+gid+":("+x1+","+y1+"),("+x2+","+y2+")]";		
		return output;
	}
}
