package indi.zyu.realtraffic.process;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.road.AllocationRoadsegment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import com.bmwcarit.barefoot.markov.KState;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.matcher.MatcherTransition;
import com.esri.core.geometry.Point;

/** 
 * 2016Äê5ÔÂ25ÈÕ 
 * TaxiInfo.java 
 * author:ZhangYu
 */
//store some info about one taxi by suid
public class TaxiInfo {
	
	private ArrayList<Sample> taxi_queue = null;//gps point haven't been spilt to trajectory	
	private HashMap<Integer,Long> time_map = null;//record map from sample_id to utc
	private HashMap<Integer,String> date_map = null;//record map from sample_id to date
	private HashMap<Integer,Boolean> passenager_map = null;//record map from sample_id to passenager
	private final long suid;
	
	int sample_id = 0;
	
	//int pre_gid;//id of roads of previous gps point
	Sample pre_sample = null;//last gps point,for preprocessing
	Sample pre_converge = null;//last convergency point
	//status of real-time matching
	KState<MatcherCandidate, MatcherTransition, MatcherSample> state;

	//if current time of received gps point exceed pre_utc by this thresold, queue will be emptyed
	static int timeout_thresold = 15;
	static double jam_speed = 2.0;
	
	
	public TaxiInfo(long suid){
		this.suid = suid;
		taxi_queue = new ArrayList<Sample>();
		state = new KState<MatcherCandidate, MatcherTransition, 
				MatcherSample>(Common.match_windows_size, -1);
		time_map = new HashMap<Integer,Long>();
		date_map = new HashMap<Integer,String>();
		passenager_map = new HashMap<Integer,Boolean>();
	}
	
	//process gps point in queue
	public void process(){
		if(taxi_queue.isEmpty()){
			return;
		}
		Object[] sample_list;
		//Sample sample;
		synchronized(taxi_queue){
			sample_list = taxi_queue.toArray();
			taxi_queue.clear();
		}
		
		for(int i=0; i< sample_list.length; i++){
			Sample sample = (Sample) sample_list[i];
			//preprocess
			if(!preprocess(sample)){
				continue;
			}
			pre_sample = sample;
			
			//get convergency point
			Sample converge_sample = realtime_match(sample);
			
			//misconvergency or match failed
			if(converge_sample == null){
				continue;
			}
			//Common.logger.debug("match success!");		
			
			if(pre_converge == null){
				pre_converge = converge_sample;
				continue;
			}
			long interval = converge_sample.utc.getTime()/1000 - pre_converge.utc.getTime()/1000;
			//interval too long, do not process
			if(interval > Common.MAX_GPS_INTERVAL){
				pre_converge = converge_sample;
				continue;
			}
			//do not estimate traffic when status of taxi changes
			if(pre_converge.passenager != converge_sample.passenager){
				Common.logger.debug("taxi status change, ignore it!");
				pre_converge = converge_sample;
				continue;
			}
			//Common.logger.debug("start to estimate traffic!");
			//Common.logger.debug(pre_converge.toString() + ";" + converge_sample.toString());
			//change road
			if(pre_converge.gid != converge_sample.gid){
				//calculate turning time
				estimite_turning(converge_sample);
			}
			//just estimite traffic by single point	
			else{
				estimite_road(converge_sample);
			}
			pre_converge = converge_sample;
		}
		
	}
	
	//add gps to queue
	public void add_gps(Sample sample){
		synchronized(taxi_queue){
			taxi_queue.add(sample);
		}	
	}
	
	//preprocess of one gps point, 
	public boolean preprocess(Sample sample){
		if(pre_sample == null){
			return true;
		}
		//Date of point is previous to last point 
		if (!sample.utc.after(pre_sample.utc)){
			//Common.logger.debug("time order error");
			return false;
		}
		//point in queue will be discarded and state will be initialized
		long interval = sample.utc.getTime()/1000 - pre_sample.utc.getTime()/1000;
		if(interval < Common.MIN_GPS_INTERVAL){
			return false;
		}
		
		//check if there exists long stop
		/*float distance = Common.calculate_dist(sample.lat,sample.lon,
				pre_sample.lat, pre_sample.lon);*/
		
		//other preprocess, wait to add...
		return true;
	}
	
	private Sample realtime_match(Sample sample){
		try{
			MatcherSample matcher_sample = new MatcherSample(String.valueOf(sample_id), 
					sample.utc.getTime(), new Point(sample.lon, sample.lat));
			time_map.put(sample_id, sample.utc.getTime()/1000);
			date_map.put(sample_id, sample.date);
			passenager_map.put(sample_id, sample.passenager);
			sample_id++;

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
				ArrayList<Sample> list = new ArrayList<Sample>();
				list.add(sample);
				if(Common.unkown_gps_updater.addGPS(new ArrayList<Sample>(list))){
					//Common.logger.debug("insert unknown gps successfully");
				}
				else{
					//Common.logger.debug("insert unknown gps failed");
				}
		    	return null;
		    }
		    //unconvergency
			if(converge == null){
				return null;
			}
			int id = Integer.parseInt(converge.matching_id());
			long utc = time_map.remove(id);
			boolean passenager = passenager_map.remove(id);
			String date = date_map.remove(id);
			Point position = converge.point().geometry(); // position
			
			Sample converge_sample = new Sample(date, this.suid, utc, position.getY(), 
					position.getX(), 0, passenager);
			converge_sample.gid = (int)converge.point().edge().id(); // road id
		    converge_sample.offset = converge.point().fraction();
		    
		    if(converge.transition() != null ){
		    	converge_sample.route = converge.transition().route().toString(); // route to position
		    }	
		    return converge_sample;

		}
		catch(Exception e){
		    e.printStackTrace();			
			return null;
		}
	}
	
	//sample and pre_sample on same road, estimite traffic
	private void estimite_road(Sample converge_sample){
		double offset = Math.abs(converge_sample.offset - pre_converge.offset);
		long interval = converge_sample.utc.getTime()/1000 - pre_converge.utc.getTime()/1000;
		int gid = converge_sample.gid;
		AllocationRoadsegment road = Common.roadlist[gid];
		
		if(interval == 0){
			Common.logger.debug("interval 0!");
			return;
		}
		
		//slow down	
		if(offset == 0){
			//consider it traffic jam
			if(road.avg_speed < jam_speed){
				road.update_speed_sample(0, converge_sample);
			}
			//consider it error
			return;
		}
		
		//update speed
		double speed = offset * road.length / interval;
		road.update_speed_sample(speed, converge_sample);
	}
	
	private void estimite_turning(Sample converge_sample){
		if(converge_sample.route == null){
			return;
		}
		long interval = converge_sample.utc.getTime()/1000 - pre_converge.utc.getTime()/1000;
		double  total_length = 0;
		
		ArrayList<Double> coverage_list = new ArrayList<Double>();
		//construct route gid list and coverage list
		String[] str_gids=converge_sample.route.split(",");
		
		ArrayList<Integer> route_gid = new ArrayList<Integer>();
		//first road
		route_gid.add(Integer.parseInt(str_gids[0]));
		
		//previous match is right
		if(pre_converge.gid == Integer.parseInt(str_gids[0])){
			coverage_list.add(1 - pre_converge.offset);
			total_length += Common.roadlist[pre_converge.gid].length * (1 - pre_converge.offset);
		}
		//match wrong
		else{
			//just a estimated value
			coverage_list.add(0.5);
			total_length += Common.roadlist[Integer.parseInt(str_gids[0])].length * (1 - pre_converge.offset);
		}
		//fully covered road
		for(int i=1; i<str_gids.length-1; i++){
			route_gid.add(Integer.parseInt(str_gids[i]));
			coverage_list.add(1.0);
			total_length += Common.roadlist[Integer.parseInt(str_gids[i])].length;
		}
		//last road
		route_gid.add(Integer.parseInt(str_gids[str_gids.length-1]));
		coverage_list.add(converge_sample.offset);
		total_length += Common.roadlist[Integer.parseInt(str_gids[str_gids.length-1])].length * converge_sample.offset;
		if(total_length / interval > Common.max_speed){
			//Common.logger.debug("route wrong, too fast");
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
		
		//calculate change rate
		/*double change_rate = Math.abs(total_time - interval) / total_time;
		Common.add_change_rate(Common.get_seq(converge_sample), change_rate);*/
		
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
				//avoid some bug of map matching
				if(gid == route_gid.get(i+1)){
					continue;
				}
				turning_time = Common.roadlist[gid].get_turning_time(route_gid.get(i+1));
				road_time = Common.roadlist[gid].time * coverage;
				percentage = (road_time + turning_time)/total_time;
				travel_time = interval * percentage;
				new_road_time = travel_time * road_time /(road_time + turning_time);
				if(coverage != 0){
					new_road_time /= coverage;
				}
				new_turning_time = travel_time * turning_time /(road_time + turning_time);
			}
			else{
				percentage = (coverage * Common.roadlist[gid].time)/total_time;
				travel_time = interval * percentage;
				new_road_time = travel_time; //no turning time
				if(coverage != 0){
					new_road_time /= coverage;
				}
			}
			
			//update road time
			int cur_seq = Common.roadlist[gid].update_time(new_road_time, converge_sample);
			
			if(cur_seq == -3){
				continue;
			}
			
			//update turning time
			if(new_turning_time > 0){
				Common.roadlist[gid].update_turning_time(route_gid.get(i+1), 
						new_turning_time, cur_seq);
			}
		}
		
		//Common.logger.debug(sample.suid + ": update speed by route: " + route_gid.toString());
	}
	
	//get a list of samples in taxi queue and matching windows
	public ArrayList<Sample> get_unprocessed_samples(){
		ArrayList<Sample> list = new ArrayList<Sample>();
		
		int queue_counter = 0;
		int window_counter = 0;
		//add samples in queue 
		synchronized(taxi_queue){
			list.addAll(taxi_queue);
			queue_counter += taxi_queue.size();
		}
		//add samples in matching windows
		for(MatcherSample i : this.state.samples()){
			long lat = (long) (i.point().getY() * 100000.0);
			long lon = (long) (i.point().getX() * 100000.0);
			int id = Integer.parseInt(i.id());
			boolean passenager = passenager_map.get(id);
			list.add(new Sample(Common.Date_Suffix, this.suid, 
					i.time()/1000, lat, lon, 0, passenager));
		}
		window_counter += this.state.samples().size();
		Common.logger.debug("queue size:" + queue_counter + " window size: " + window_counter);
		return list;
	}
	
	//get pre_sample of current taxi
	public Sample get_pre_sample(){
		return pre_sample;
	}
	
	//get pre_sample of current taxi
	public void set_pre_sample(Sample pre_sample){
		this.pre_sample = pre_sample;
	}
}
