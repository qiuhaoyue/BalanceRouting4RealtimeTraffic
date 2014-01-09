import java.util.HashMap;

public class AllocationRoadsegment extends RoadSegment {
	
	HashMap<Integer, Double> turning_time=null;
	AllocationRoadsegment(){
		super();
		turning_time=new HashMap<Integer, Double>();
	}
	
	AllocationRoadsegment(int gid, double max_speed, double average_speed, int reference){
		super(gid, max_speed, average_speed, reference);
		turning_time=new HashMap<Integer, Double>();
	}
	
	public boolean add_turning_speed(int next_gid, double speed){
		if(this.turning_time.containsKey(next_gid)){
			return false;
		}
		else{
			turning_time.put(next_gid, speed);
			return true;
		}
	}
	
	public double get_speed(int next_gid){
		if(next_gid>0 && turning_time.containsKey(next_gid)){
			return turning_time.get(next_gid);
		}
		else{
			return this.avg_inst_speed;
		}
	}
	
	public double get_traveltime(int next_gid){
		double speed=0.0;
		if(next_gid>0 && turning_time.containsKey(next_gid)){
			speed=turning_time.get(next_gid);
		}
		else{
			speed=avg_inst_speed;
		}
		
		if(speed>0){
			return this.length*1000/speed;
		}
		else{
			return -1;
		}
	}
	
	public double get_traveltime(double pos, boolean covered, int next_gid){
		double speed=0.0;
		if(next_gid>0 && turning_time.containsKey(next_gid)){
			speed=turning_time.get(next_gid);
		}
		else{
			speed=this.avg_inst_speed;
		}
		
		if(speed>0){
			if(this.to_cost>=0 && this.to_cost< RoadCostUpdater.inconnectivity-1){
				if(covered){
					return this.length*1000*pos/speed;
				}
				else{
					return this.length*1000*(1-pos)/speed;
				}
			}
			else{
				if(covered){
					return this.length*1000*(1-pos)/speed;
				}
				else{
					return this.length*1000*pos/speed;
				}
			}
		}
		else{
			return -1;
		}
	}
	
}
