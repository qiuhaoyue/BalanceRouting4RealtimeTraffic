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
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AllocationRoadsegment extends RoadSegment {
	
	double model_maxspeed;
	double model_alpha;
	double model_beta;
	double model_capacity;
	double model_r2;
	private Lock road_lock = null;
	private Lock turning_lock = null;
	private Lock seq_lock = null;
	int seq;//by last updated utc,1-96

	HashMap<Integer, Double> turning_time=null;
	public AllocationRoadsegment(){
		super();
		this.model_maxspeed=-1;
		this.model_alpha=-1;
		this.model_beta=-1;
		this.model_capacity=-1;
		this.seq = -1;
		turning_time=new HashMap<Integer, Double>();
		road_lock = new ReentrantLock();
		turning_lock = new ReentrantLock();
		seq_lock = new ReentrantLock();
	}
	
	public AllocationRoadsegment(long gid, double max_speed, double average_speed, int reference){
		super(gid, max_speed > Common.max_speed ? Common.max_speed:max_speed, 
				average_speed < Common.min_speed ? Common.min_speed : average_speed, reference);
		turning_time=new HashMap<Integer, Double>();
		this.seq = -1;
		road_lock = new ReentrantLock();
		turning_lock = new ReentrantLock();
		seq_lock = new ReentrantLock();
	}
	
	AllocationRoadsegment(int gid, double average_speed, double taxi_count, double taxi_ratio){
		super(gid, average_speed==0 ? 20 : average_speed, taxi_count, taxi_ratio);
		turning_time=new HashMap<Integer, Double>();
		this.seq = -1;
		road_lock = new ReentrantLock();
		turning_lock = new ReentrantLock();
		seq_lock = new ReentrantLock();
	}
	
	AllocationRoadsegment(int cur_gid, double revised_max_speed, double alpha, double beta, double capacity){
		super();
		this.gid=cur_gid;
		this.model_maxspeed=revised_max_speed;
		this.model_alpha=alpha;
		this.model_beta=beta;
		this.model_capacity=capacity;
		this.seq = -1;
		road_lock = new ReentrantLock();
		turning_lock = new ReentrantLock();
		seq_lock = new ReentrantLock();
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
	
	
	public void update_avg_speed(double speed, Sample sample){
		if(speed <= 0){
			return;
		}
		road_lock.lock();
		this.avg_speed = speed;
		this.time = length / avg_speed;
		road_lock.unlock();
		check_seq(sample);
	}
	
	public void update_time(double time, Sample sample){
		if(time <= 0){
			return;
		}
		road_lock.lock();
		this.time = time;
		this.avg_speed = length / time;
		if(this.avg_speed > Common.max_speed){
			this.avg_speed = Common.max_speed;
			this.time = length / avg_speed;
		}
		road_lock.unlock();
		
		check_seq(sample);
	}
	
	public double get_turning_time(int gid){
		if(turning_time.containsKey(gid)){
			return turning_time.get(gid);
		}
		else{
			return Common.init_turning_time;
		}
	}
	
	public HashMap<Integer, Double> get_all_turning_time(){
		return turning_time;
	}
	
	public void update_turning_time(int gid, double time, Sample sample){
		if(time <= 0){
			return;
		}
		turning_lock.lock();
		turning_time.put(gid, time);
		turning_lock.unlock();
		check_seq(sample);
	}
	
	private void check_seq(Sample sample){
		int cur_seq = Common.get_seq(sample);
		//Common.logger.debug("cur: " + cur_seq + "old " + this.seq);
		//update traffic in last seq
		if(this.seq == -1){
			this.seq = cur_seq;
			return;
		}
		if(cur_seq > this.seq){
			seq_lock.lock();
			int old_seq = this.seq;	
			this.seq = cur_seq;
			seq_lock.unlock();
			try{
				Common.real_traffic_updater.update((int)gid, old_seq);
			}
			catch (SQLException e) {
				Common.logger.debug("update real traffc failed!");
			    e.printStackTrace();
			}
			Common.logger.debug("update real traffc success! seq: " + old_seq + " len: " + this.length);
			
		}
	}
}
