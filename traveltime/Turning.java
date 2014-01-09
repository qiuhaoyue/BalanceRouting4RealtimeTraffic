
public class Turning {
	public long intersection_id;
	public int from_edge;
	public int to_edge;
	public long node_id;
	public int from_way;
	public int to_way;
	public String hour_on;
	public String hour_off;
	public String examption;
    
	Turning(long id, int from_edge, int to_edge, long node_id, int from_way, int to_way, String hour_on, String hour_off, String examption){
		this.intersection_id=id;
		this.from_edge=from_edge;
		this.to_edge=to_edge;
		this.node_id=node_id;
		this.from_way=from_way;
		this.to_way=to_way;
		this.hour_on=hour_on;
		this.hour_off=hour_off;
		this.examption=examption;
	}
	
}
