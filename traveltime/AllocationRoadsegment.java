import java.util.HashMap;

public class AllocationRoadsegment extends RoadSegment {
	
	double model_maxspeed;
	double model_alpha;
	double model_beta;
	double model_capacity;
	double model_r2;

	HashMap<Integer, Double> turning_time=null;
	AllocationRoadsegment(){
		super();
		this.model_maxspeed=-1;
		this.model_alpha=-1;
		this.model_beta=-1;
		this.model_capacity=-1;
		turning_time=new HashMap<Integer, Double>();
	}
	
	AllocationRoadsegment(int gid, double max_speed, double average_speed, int reference){
		super(gid, max_speed, average_speed, reference);
		turning_time=new HashMap<Integer, Double>();
	}
	
	AllocationRoadsegment(int gid, double average_speed, double taxi_count, double taxi_ratio){
		super(gid, average_speed, taxi_count, taxi_ratio);
		turning_time=new HashMap<Integer, Double>();
	}
	
	AllocationRoadsegment(int cur_gid, double revised_max_speed, double alpha, double beta, double capacity){
		super();
		this.gid=cur_gid;
		this.model_maxspeed=revised_max_speed;
		this.model_alpha=alpha;
		this.model_beta=beta;
		this.model_capacity=capacity;
		
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
	
	public double get_traveltime(double pos, boolean covered, double time){
		
		if(this.to_cost>=0 && this.to_cost< RoadCostUpdater.inconnectivity-1){
			if(covered){
				return pos*time;
			}
			else{
				return (1-pos)*time;
			}
		}
		else{
			if(covered){
				return (1-pos)*time;
			}
			else{
				return pos*time;
			}
		}
	}
	
}