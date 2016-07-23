package indi.zyu.realtraffic.process;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.road.AllocationRoadsegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bmwcarit.barefoot.markov.KState;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.matcher.MatcherTransition;
import com.bmwcarit.barefoot.roadmap.Route;
import com.esri.core.geometry.Point;

/** 
 * 2016Äê5ÔÂ25ÈÕ 
 * TaxiInfo.java 
 * author:ZhangYu
 */
//store some info about one taxi by suid
public class TaxiInfo {
	
	ArrayList<Sample> taxi_queue = null;//gps point haven't been spilt to trajectory	
	//int pre_gid;//id of roads of previous gps point
	Sample pre_sample = null;//last gps point
	//status of real-time matching
	KState<MatcherCandidate, MatcherTransition, MatcherSample> state;
	int status;//0-movement, 1-stop
	//if size of vector in state exceed thresold, clear some points and remain some points
	int max_vector_size = 50;
	int remain_vector_size = 10;
	//params to judge long stop
	int interval_thresold = 5;
	double distance_thresold = 50.0;
	double speed_thresold;
	//if current time of received gps point exceed pre_utc by this thresold, queue will be emptyed
	int timeout_thresold = 10;
	double time_increment;
	double speed_increment;
	double jam_speed = 2.0;
	
	
	public TaxiInfo(){
		taxi_queue = new ArrayList();
		state = new KState<MatcherCandidate, MatcherTransition, 
				MatcherSample>(Common.match_windows_size, -1);
		//pre_gid = -1;
		speed_thresold = 0.1;
		//pre_utc = new Date(0);
		status = 0;
		//gid_list = new ArrayList<Integer>();
		
	}
	//process gps point in queue
	public void process(){
		if(taxi_queue.isEmpty()){
			return;
		}
		Object[] sample_list;
		//Sample sample;
		synchronized(taxi_queue){
			//sample = taxi_queue.get(0);
			//taxi_queue.remove(0);
			sample_list = taxi_queue.toArray();
			taxi_queue.clear();
		}
		
		for(int i=0; i< sample_list.length; i++){
			Sample sample = (Sample) sample_list[i];
			//preprocess
			if(!preprocess(sample)){
				continue;
			}
					
			//misconvergency or match failed
			if(!realtime_match(sample)){
				continue;
			}
					
			//Common.logger.debug("convergency point: " + sample.gid);
			if(pre_sample == null){
				//Common.logger.debug(sample.suid + " first point; gid: " + sample.gid);
				//taxi_queue.add(sample);
				pre_sample = sample;
				continue;
			}
					
			//check whether to split seq
			//1.change road
			if(pre_sample.gid != sample.gid){
				//calculate turning time
				estimite_turning(sample);
				//split();
			}
			//2. long stop
			//else if(status == 1 && !taxi_queue.isEmpty()){
				//split();
			//}
			//do not split, just estimite traffic by single point	
			else{
				estimite_road(sample);
			}
			//taxi_queue.add(sample);
			pre_sample = sample;
		}
		
	}
	//add gps to queue
	public void add_gps(Sample sample){
		synchronized(taxi_queue){
			taxi_queue.add(sample);
		}	
	}
	//preprocess of one gps point, return true if gps point is valid, otherwise return false 
	public boolean preprocess(Sample sample){
		if(pre_sample == null){
			return true;
		}
		//Date of point is previous to last point 
		if (!sample.utc.after(pre_sample.utc)){
			Common.logger.debug("time order error");
			return false;
		}
		//point in queue will be discarded and state will be initialized
		long interval = sample.utc.getTime()/1000 - pre_sample.utc.getTime()/1000;
		if(interval < 20){
			return false;
		}
		
		if( interval > timeout_thresold * 60){
			Common.logger.debug("time out!");
			//taxi_queue.clear();
			//state = new KState<MatcherCandidate, MatcherTransition, MatcherSample>(6, -1);
			status = 0;
			return true;
		}
		//check if there exists long stop
		float distance = Common.calculate_dist(sample.lat,sample.lon,
				pre_sample.lat, pre_sample.lon);
		
		if(distance < distance_thresold && interval > interval_thresold * 60){
			status = 1;
		}
		else{
			status = 0;
		}
		//other preprocess, wait to add...
		return true;
	}
	
	private boolean realtime_match(Sample sample){
		try{
			MatcherSample matcher_sample = new MatcherSample(String.valueOf(sample.suid), 
					sample.utc.getTime()/1000, new Point(sample.lon, sample.lat));

			//this function cost most of time
			Set<MatcherCandidate> vector = Common.matcher.execute(this.state.vector(), this.state.sample(),
		    		matcher_sample);
			
			//convergency point or top point if windows size exceed thresold or null
			MatcherCandidate converge = this.state.update_converge(vector, matcher_sample);
			
		    // test whether the point is unable to match
		    MatcherCandidate estimate = this.state.estimate(); // most likely position estimate
		    if(estimate == null || estimate.point() == null){
		    	//Common.logger.debug("match fail!: " + sample.toString());
		    	//store unmatched gps
				ArrayList list = new ArrayList();
				list.add(sample);
				if(Common.unkown_gps_updater.addGPS(new ArrayList(list))){
					//Common.logger.debug("insert unknown gps successfully");
				}
				else{
					//Common.logger.debug("insert unknown gps failed");
				}
		    	return false;
		    }
		    //unconvergency
			if(converge == null){
				return false;
			}
		    sample.gid = (int)converge.point().edge().id(); // road id
		    Point position = converge.point().geometry(); // position

		    sample.lat = position.getY();
		    sample.lon = position.getX();
		    sample.offset = converge.point().fraction();
		    if(converge.transition() != null ){
		    	sample.route = converge.transition().route().toString(); // route to position
		    }	

		}
		catch(Exception e){
		    e.printStackTrace();			
			return false;
		}
		return true;
	}
	
	private void split(){
		if(taxi_queue.size() >= 3){
			//Common.fixedThreadPool.execute(new ProcessThread(taxi_queue.get(0).suid, taxi_queue, state));
		}
		
		taxi_queue.clear();
		status = 0;
		/*int remain_num = this.remain_vector_size;
		if(state.vector().size() > this.max_vector_size){
			Set<MatcherCandidate> c = new Set<MatcherCandidate>();
			Object[] array = state.vector().toArray();
			for(int i = array.length -1 ; remain_num>=0 ;i--,remain_num--){
				c.add((MatcherCandidate)array[i]);
			}
			state.vector().retainAll(c);
		}*/
		//state.vector().clear();
		Common.logger.debug("start thread!");
	}
	//sample and pre_sample on same road, estimite traffic
	private void estimite_road(Sample sample){
		double offset = Math.abs(sample.offset - pre_sample.offset);
		long interval = sample.utc.getTime()/1000 - pre_sample.utc.getTime()/1000;
		int gid = sample.gid;
		AllocationRoadsegment road = Common.roadlist[gid];
		
		if(interval == 0){
			Common.logger.debug("interval 0!");
			return;
		}
		
		//slow down	
		if(offset == 0){
			Common.logger.debug("offset 0!");
			//consider it traffic jam
			if(road.avg_speed < jam_speed){
				double slowdown_speed = Common.roadlist[gid].avg_speed * 0.9;
				road.update_avg_speed(restrict_speed(slowdown_speed, road.max_speed), sample);
			}
			//consider it error
			return;
		}
		double speed = restrict_speed(offset * road.length / interval, road.max_speed);
		double smooth_speed = road.avg_speed * Common.smooth_alpha + speed * (1- Common.smooth_alpha);
		road.update_avg_speed(restrict_speed(smooth_speed, road.max_speed), sample);
		
		//Common.logger.debug("estimate real traffic: " +  gid + " " + smooth_speed);
	}
	
	private void estimite_turning(Sample sample){
		if(sample.route == null){
			Common.logger.debug("route null");
			return;
		}
		long interval = sample.utc.getTime()/1000 - pre_sample.utc.getTime()/1000;
		int  total_length = 0;
		
		ArrayList<Double> coverage_list = new ArrayList<Double>();
		//construct route gid list and coverage list
		String[] str_gids=sample.route.split(",");
		
		ArrayList<Integer> route_gid = new ArrayList<Integer>();
		//first road
		route_gid.add(Integer.parseInt(str_gids[0]));
		//previous match is right
		if(pre_sample.gid == Integer.parseInt(str_gids[0])){
			coverage_list.add(1 - pre_sample.offset);
			total_length += Common.roadlist[pre_sample.gid].length * (1 - pre_sample.offset);
		}
		//match wrong
		else{
			//just a estimated value
			coverage_list.add(0.5);
			total_length += Common.roadlist[Integer.parseInt(str_gids[0])].length * (1 - pre_sample.offset);
		}
		//fully covered road
		for(int i=1; i<str_gids.length-1; i++){
			route_gid.add(Integer.parseInt(str_gids[i]));
			coverage_list.add(1.0);
			total_length += Common.roadlist[Integer.parseInt(str_gids[i])].length;
		}
		//last road
		route_gid.add(Integer.parseInt(str_gids[str_gids.length-1]));
		coverage_list.add(sample.offset);
		total_length += Common.roadlist[Integer.parseInt(str_gids[str_gids.length-1])].length * sample.offset;
		if(total_length / interval > Common.max_speed){
			Common.logger.debug("route wrong, too fast");
			return;
		}
		//start calulate route time
		//calculate total time
		double total_time = 0;
		for(int i=0; i<route_gid.size(); i++){
			int gid = route_gid.get(i);
			double coverage = coverage_list.get(i);
			total_time += coverage * Common.roadlist[gid].time;
			//add turning time
			if(i != route_gid.size()-1){
				total_time += Common.roadlist[gid].get_turning_time(route_gid.get(i+1));
			}
		}
		if(total_time == 0){
			Common.logger.debug("total time zero error!");
			return;
		}
		//calculate time in each road
		
		for(int i=0; i<route_gid.size(); i++){
			int gid = route_gid.get(i);
			double coverage = coverage_list.get(i);
			double new_road_time;
			double new_turning_time = -1;
			double road_time = Common.roadlist[gid].time;
			double turning_time = Common.init_turning_time;
			
			double percentage;//percentage of travel time in total time, not real
			double travel_time;//real travel time
			if(i != route_gid.size()-1){
				turning_time = Common.roadlist[gid].get_turning_time(route_gid.get(i+1));
				road_time = Common.roadlist[gid].time;
				percentage = (coverage * Common.roadlist[gid].time + turning_time)/total_time;
				travel_time = interval * percentage;
				new_road_time = travel_time * road_time /(road_time + turning_time);
				new_turning_time = travel_time * turning_time /(road_time + turning_time);
			}
			else{
				percentage = (coverage * Common.roadlist[gid].time)/total_time;
				travel_time = interval * percentage;
				new_road_time = travel_time; //no turning time
			}
			
			//update time
			double update_road_time = road_time * Common.smooth_alpha + new_road_time * (1-Common.smooth_alpha);
			Common.roadlist[gid].update_time(update_road_time, sample);
			//update turning time
			if(new_turning_time > 0){
				double update_turning_time = turning_time * Common.smooth_alpha + 
						new_turning_time * (1-Common.smooth_alpha);
				Common.roadlist[gid].update_turning_time(route_gid.get(i+1), 
						update_turning_time, sample);
			}
		}
		
		//Common.logger.debug(sample.suid + ": update speed by route: " + route_gid.toString());
	}
	
	//restrict speed between max and min speed
	private double restrict_speed(double speed, double max_speed){
		if(speed > max_speed){
			speed = max_speed;
		}
		if(speed < Common.min_speed){
			speed = Common.min_speed;
		}
		return speed;
	}
}
