/** 
 * 2016Äê3ÔÂ12ÈÕ 
 * AllocationRoadsegment.java 
 * author:ZhangYu
 */

package indi.zyu.realtraffic.road;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;

import java.sql.SQLException;
import java.util.HashMap;

public class AllocationRoadsegment extends RoadSegment {
	
	public int seq;//by last updated utc,1-288
	public String date;//by last updated date

	HashMap<Integer, Double> turning_time=null;
	HashMap<Integer, Integer> turning_seq=null;
	public AllocationRoadsegment(){
		super();
		this.seq = -1;
		this.date = "unknown";
		turning_time=new HashMap<Integer, Double>();
		turning_seq = new HashMap<Integer, Integer>();
	}
	
	public AllocationRoadsegment(long gid, double max_speed, double average_speed, int reference){
		super(gid, max_speed > Common.max_speed ? Common.max_speed:max_speed, 
				average_speed < Common.min_speed ? Common.min_speed : average_speed, reference);
		turning_time=new HashMap<Integer, Double>();
		turning_seq = new HashMap<Integer, Integer>();
		this.seq = -1;
		this.date = "unknown";
	}
	
	AllocationRoadsegment(int gid, double average_speed, double taxi_count, double taxi_ratio){
		super(gid, average_speed==0 ? 20 : average_speed, taxi_count, taxi_ratio);
		turning_time=new HashMap<Integer, Double>();
		turning_seq = new HashMap<Integer, Integer>();
		this.seq = -1;
		this.date = "unknown";
	}
	
	public boolean add_turning_time(int next_gid, double time){
		if(this.turning_time.containsKey(next_gid)){
			return false;
		}
		else{
			turning_time.put(next_gid, time);
			turning_seq.put(next_gid, -1);
			return true;
		}
	}
	
	
	public double get_speed(int next_gid){
		if(next_gid>0 && turning_time.containsKey(next_gid)){
			return turning_time.get(next_gid);
		}
		else{
			return this.avg_speed;
		}
	}
	
	public double get_traveltime(int next_gid){
		double speed=0.0;
		if(next_gid>0 && turning_time.containsKey(next_gid)){
			speed=turning_time.get(next_gid);
		}
		else{
			speed=avg_speed;
		}
		
		if(speed>0){
			return this.length*1000/speed;
		}
		else{
			return -1;
		}
	}
	
	
	synchronized public void update_speed_sample(double speed, Sample sample){
		if(speed < 0){
			return;
		}
		//lock.lock();
		int cur_seq = Common.get_seq(sample);
		int pre_seq = this.seq;
		//seq_lock.lock();
		if(!check_seq(cur_seq, sample.date)){
			//Common.logger.debug("sample too old");
			//lock.unlock();
			return;
		}
		//seq_lock.unlock();
		
		
		//traffic jam, slow down
		if(speed == 0){
			//road_lock.lock();
			this.avg_speed = restrict_speed(this.avg_speed * 0.9);
			this.time = this.length / this.avg_speed;
			//road_lock.unlock();	
		}
		else if(speed > Common.max_speed){
			return;
		}
		else{
			speed = restrict_speed(speed);
			//smooth
			int diff_seq = get_diff_seq(pre_seq, cur_seq);
			double smooth_alpha = 0.9 - diff_seq * Common.smooth_delta;
			if(smooth_alpha < 0){
				smooth_alpha = 0;
			}
			//delay sample
			else if(smooth_alpha > 0.9){
				smooth_alpha = 0.9;
			}
			//road_lock.lock();
			this.avg_speed = restrict_speed(speed * (1-smooth_alpha) + 
					this.avg_speed * smooth_alpha);
			this.time = this.length / this.avg_speed;
			//road_lock.unlock();	
		}	
		//lock.unlock();
	}
	
	//update speed in current seq
	private void update_speed(double speed){
		if(speed <= 0){
			return;
		}
		
		//road_lock.lock();
		this.avg_speed = speed;
		this.time = this.length / this.avg_speed;
		//road_lock.unlock();
	}
	
	//update road time, return current seq to update turning time, -3 stands for error updating
	synchronized public int update_time(double new_time, Sample sample){
		if(new_time <= 0){
			return -3;
		}
		//lock.lock();
		int cur_seq = Common.get_seq(sample);
		int pre_seq = this.seq;
		//seq_lock.lock();
		if(!check_seq(cur_seq, sample.date)){
			//Common.logger.debug("sample too old");
			//lock.unlock();
			return -3;
		}
		
		//check sensed speed
		double sensed_speed = this.length/new_time;
		if(sensed_speed > Common.max_speed){
			return -3;
		}
		//seq_lock.unlock();
		
		//smooth
		int diff_seq = get_diff_seq(pre_seq, cur_seq);
		double smooth_alpha = 0.9 - diff_seq * Common.smooth_delta;
		if(smooth_alpha < 0){
			smooth_alpha = 0;
		}
		//delay sample
		else if(smooth_alpha > 0.9){
			smooth_alpha = 0.9;
		}
		
		//road_lock.lock();
		//check whether time increase too fast, to avoid wrong traffic
		/*int max_times = (int) (3 + (0.9 - smooth_alpha) * 10);
		if(new_time / this.time > max_times){
			return -3;
		}*/
		
		this.avg_speed = restrict_speed(sensed_speed * (1-smooth_alpha) + 
				this.avg_speed * smooth_alpha);
		this.time = this.length / this.avg_speed;
		
		//road_lock.unlock();	
		//lock.unlock();
		return cur_seq;
	}
	
	public double get_turning_time(int gid){
		if(turning_time.containsKey(gid)){
			return turning_time.get(gid);
		}
		else{
			return Common.init_turning_time;
		}
	}
	
	public int get_turning_seq(int gid){
		if(turning_seq.containsKey(gid)){
			return turning_seq.get(gid);
		}
		else{
			return 0;
		}
	}
	
	public HashMap<Integer, Double> get_all_turning_time(){
		return turning_time;
	}
	
	public HashMap<Integer, Integer> get_all_turning_seq(){
		return turning_seq;
	}
	
	//road time has been updated before, so no need to check seq
	synchronized public void update_turning_time(int gid, double new_turning_time, int cur_seq){
		if(new_turning_time <= 0){
			return;
		}
		
		//lock.lock();
		//smooth
		int diff_seq = get_diff_seq(this.get_turning_seq(gid),cur_seq);
		double smooth_alpha = 0.9 - diff_seq * Common.smooth_delta;
		if(smooth_alpha < 0){
			smooth_alpha = 0;
		}
		//delay sample
		else if(smooth_alpha > 0.9){
			smooth_alpha = 0.9;
		}
		
		new_turning_time = this.get_turning_time(gid) * smooth_alpha + 
				new_turning_time * (1 - smooth_alpha);
		
		//turning_lock.lock();
		turning_time.put(gid, restrict_turning_time(new_turning_time));
		turning_seq.put(gid, cur_seq);//maybe do not need to consider the problem of asynchronization
		//turning_lock.unlock();
		//lock.unlock();
		
	}
	
	//check whether to update traffic and insert old traffic
	private boolean check_seq(int cur_seq, String cur_date){
		//Common.logger.debug("cur: " + cur_seq + "old " + this.seq);
		//update traffic in last seq
		if(this.seq == -1){
			this.seq = cur_seq;
			this.date = cur_date;
			return true;
		}
		
		//last day
		if(cur_date.compareTo(this.date) < 0){
			return false;
		}
		
		//current sample is later than last sample
		if(cur_seq > this.seq || cur_date.compareTo(this.date) > 0){
			
			//update current seq and date
			//seq_lock.lock();
			int old_seq = this.seq;	
			this.seq = cur_seq;
			String old_date = this.date;
			if(cur_date != this.date){
				this.date = cur_date;
			}
			//seq_lock.unlock();
			
			//insert old traffic to database
			try{
				Common.real_traffic_updater.update(old_date, (int)gid, old_seq);
				Common.logger.debug("update real traffc success! seq: " + old_seq + " new seq: " + 
				this.seq + " gid: " + this.gid + " date: " + old_date + " cur date: " + this.date);
				//no traffic sensed in the interval
				if(get_diff_seq(cur_seq, old_seq) > 1){
					//use history traffic to infer speed
					infer_speed(old_date, old_seq, cur_seq);
				}
			}
			catch (SQLException e) {
				Common.logger.error("update real traffc failed!");
				e.printStackTrace();
				return false;
			}
			return true;		
		}
		else if(get_diff_seq(cur_seq, this.seq) <= Common.delay_update_thresold){			
			return true;
		}
		//sample is too old
		return false;
	}
	
	public int get_seq(){
		return this.seq;
	}
	
	public String get_date(){
		return this.date;
	}
	
	public double get_road_time(){
		return this.time;
	}
	
	public double get_road_speed(){
		return this.avg_speed;
	}
	//insert current traffic to database to simulate a new day
	/*public void flush(){
		if(this.seq != -1){
			try {
				Common.real_traffic_updater.update((int)gid, this.seq);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				Common.logger.debug("flush traffc failed!");
				e.printStackTrace();
			}
		}
	}*/
	
	//infer traffic if no traffic sensed during intervals
	private void infer_speed(String pre_date, int pre_seq, int cur_seq) throws SQLException{
		double pre_speed = Common.default_traffic[(int)gid][pre_seq];
		if(pre_speed <= 0){
			return;
		}
		double correction_factor = this.avg_speed / pre_speed;
		double default_speed = 0.0;
		double infer_speed = 0.0;
		double class_speed_transverse = Common.default_class_traffic[this.class_id][pre_seq];
		double class_speed_vertical = 0.0;
		double max_speed_limit = 0.0;
		double min_speed_limit = 0.0;
		
		int max_infer_seq = pre_seq + Common.max_infer_traffic_interval;
		if(max_infer_seq > cur_seq - 1){
			max_infer_seq = cur_seq - 1;
		}
		for(int i=pre_seq+1; i<= max_infer_seq; i++){
			int tmp_seq = i % Common.max_seg;
			
			default_speed = Common.default_traffic[(int)gid][tmp_seq];
			
			if(default_speed <= 0){
				continue;
			}
			//calculate inferred speed
			infer_speed = default_speed * correction_factor;
			class_speed_vertical = Common.default_class_traffic[this.class_id][tmp_seq];
			//avoid saltation
			//transverse comparison
			max_speed_limit = this.avg_speed + Common.infer_alpha * (i- pre_seq) * class_speed_transverse;
			min_speed_limit = this.avg_speed - Common.infer_alpha * (i- pre_seq) * class_speed_transverse;
			//vertical comparison
			if(max_speed_limit > default_speed + Common.infer_alpha * class_speed_vertical){
				max_speed_limit = default_speed + Common.infer_alpha * class_speed_vertical;
			}
			if(min_speed_limit < default_speed - Common.infer_alpha * class_speed_vertical){
				min_speed_limit = default_speed - Common.infer_alpha * class_speed_vertical;
			}			
			if(max_speed_limit <= min_speed_limit){
				continue;
			}
			
			if(infer_speed > max_speed_limit){
				infer_speed = max_speed_limit;
			}
			else if(infer_speed < min_speed_limit){
				infer_speed = min_speed_limit;
			}
			infer_speed = restrict_speed(infer_speed);
			//if(tmp_seq != cur_seq){
				//insert it
				String tmp_date;
				//another day
				if(tmp_seq != i ){
					tmp_date = this.date;
				}
				else{
					tmp_date = pre_date;
				}
				/*Common.logger.debug("infer traffc seq: " + tmp_seq + " cur seq: " + 
						this.seq + " gid: " + this.gid  + "cur date: " + this.date);*/
				Common.real_traffic_updater.update_road(tmp_date, (int)gid, tmp_seq, infer_speed);	
			//}
			
			//correct current speed
			/*else{
				//treat infer speed as sensed speed to smooth current speed
				double smooth_alpha = 0.8;		
				double new_speed = this.avg_speed * smooth_alpha 
					+ infer_speed * (1 - smooth_alpha);				
				new_speed = restrict_speed(new_speed);
				
				update_speed(new_speed);
			}*/
						
		}
	}
	
	//restrict speed between max and min speed
	private double restrict_speed(double speed){
		if(speed > this.max_speed){
			speed = this.max_speed;
		}
		if(speed < Common.min_speed){
			speed = Common.min_speed;
		}
		return speed;
	}
	
	//restrict turning time
	private double restrict_turning_time(double time){
		if(time < Common.MIN_TURNING_TIME){
			time = Common.MIN_TURNING_TIME;
		}
		//there is not suitable way to get max turning time
		return time;
	}
	
	private int get_diff_seq(int old_seq, int cur_seq){
		int diff_seq = cur_seq - old_seq;
		while(diff_seq < 0){
			diff_seq += Common.max_seg;
		}
		return diff_seq;
	}
	
}
