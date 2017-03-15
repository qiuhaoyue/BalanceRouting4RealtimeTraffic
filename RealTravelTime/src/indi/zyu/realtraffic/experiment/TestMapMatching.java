package indi.zyu.realtraffic.experiment;

import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.common.Common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.bmwcarit.barefoot.markov.KState;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.matcher.MatcherTransition;
import com.bmwcarit.barefoot.roadmap.Route;
import com.esri.core.geometry.Point;

// to test accuracy of OHMMM with different window size
public class TestMapMatching {
	static ArrayList taxi[] = null;//all taxi data
	static ArrayList offline_gid[]  = null;//store matched gid
	static ArrayList online_gid[]  = null;//store matched gid
	static ArrayList offline_route[] = null;
	static ArrayList online_route[] = null;
	static int max_suid;
	//static int window_size = 3;
	static int min_window_size = 2;
	static int max_window_size = 20;
	//static int time_range = 60;
	static double rest_counter[] = new double[max_window_size+1];
	
	
	public static void main(String[] args){  
		max_suid = get_taxi_data();
		Common.init(max_suid);
		int total_counter[] = new int[max_window_size+1];
		int error_counter[] = new int[max_window_size+1];
		int route_total_counter[] = new int[max_window_size+1];
		int route_error_counter[] = new int[max_window_size+1];
		double ave_delay[] = new double[max_window_size+1];
		
		offline_route = new ArrayList[10000];
		online_route = new ArrayList[10000];
		for(int i=0; i<10000; i++){
			offline_route[i] = new ArrayList(30);
			online_route[i] = new ArrayList(30);
		}
		
		int trajectory_counter = 0;
		
		for(int i=0; i <= max_suid; i++){
			if(taxi[i] == null){
				continue;
			}
			
			Common.logger.debug("suid " + i +": ");
			if(taxi[i].size() < 300){
				Common.logger.debug("too short, skip");
			}
			
			trajectory_counter++;
			
			//offline
			offline_match(i, taxi[i]);
			//caculate online map-matching with different window size and compare with offline matching
			for(int window_size=min_window_size; window_size<=max_window_size; window_size++){
				ave_delay[window_size] += online_match(i, taxi[i], window_size);
				//normal condition, if some points match failed, matched number of offline will decrease
				if(offline_gid[i].size() > 0 && offline_gid[i].size() >= online_gid[i].size()
						&& offline_gid[i].size() - online_gid[i].size() <= window_size + 1){
					total_counter[window_size] += online_gid[i].size();

					for(int k=0; k< online_gid[i].size(); k++){
						//compare matched gid
						// do not use !=
						if(!online_gid[i].get(k).equals(offline_gid[i].get(k)) ){
							error_counter[window_size] += 1;
						}
						//compare route
						if(online_route[k].size() == 0 || offline_route[k].size() == 0){
							continue;
						}
						route_total_counter[window_size] += online_route[k].size();
						offline_route[k].retainAll(online_route[k]);
						route_error_counter[window_size] += online_route[k].size() - offline_route[k].size();
					}
				}
			}	
		}
		
		double error_rate;
		double[][] data = new double[2][max_window_size - min_window_size + 1];
		for(int window_size=min_window_size; window_size<=max_window_size; window_size++){
			//gid comparison
			Common.logger.debug("windows size: " + window_size);
			Common.logger.debug("total: " + total_counter[window_size] + ", error: " + error_counter[window_size]);
			error_rate = (double)error_counter[window_size]/(double)total_counter[window_size];
			Common.logger.debug("error rate: " + error_rate);
			data[0][window_size - min_window_size] = error_rate;
			
			//route comparison
			Common.logger.debug("route total: " + route_total_counter[window_size] + ", error: " + route_error_counter[window_size]);
			error_rate = (double)route_error_counter[window_size]/(double)route_total_counter[window_size];
			Common.logger.debug("route error rate: " + error_rate);
			data[1][window_size - min_window_size] = error_rate;
			
			//average delay
			//Common.logger.debug(ave_delay[window_size] + ";" + trajectory_counter);
			ave_delay[window_size] /= trajectory_counter;
			Common.logger.debug("average delay: " + ave_delay[window_size]);
			
			//rest point number
			rest_counter[window_size] /= trajectory_counter;
			Common.logger.debug("rest point number: " + rest_counter[window_size]);
		}
		Chart.output(data, "/home/zyu/data_map_matching");
		
	}
	
	public static void offline_match(int suid, ArrayList<Sample> trajectory){
		//Common.logger.debug(trajectory.toString());
		KState<MatcherCandidate, MatcherTransition, MatcherSample> state = new KState<MatcherCandidate, MatcherTransition, 
				MatcherSample>();
		Sample pre_sample = null;//to avoid stop point
		for(Sample sample : trajectory){
			//stop point, ignore it
			if(pre_sample != null && sample.lat == pre_sample.lat && sample.lon == pre_sample.lon){
				continue;
			}
			MatcherSample matcher_sample = new MatcherSample(String.valueOf(sample.suid), 
					sample.utc.getTime(), new Point(sample.lon, sample.lat));
			Set<MatcherCandidate> vector = Common.matcher.execute(state.vector(), state.sample(),
		    		matcher_sample);
			state.update2(vector, matcher_sample);
			pre_sample = sample;
		}
		List<MatcherCandidate> sequence = state.sequence();
		if(sequence == null){
			return;
		}
		for(int i=0; i< sequence.size(); i++){
			MatcherCandidate estimate = sequence.get(i);
			int gid = (int)estimate.point().edge().id();
			offline_gid[suid].add(gid);
			
			//get route
			if(estimate.transition() != null){
				Route route = estimate.transition().route();
				for(int k=0; k<route.size(); k++){
					offline_route[i].add(route.get(k));
				}
			}
			
		}
	}
	
	public static double online_match(int suid, ArrayList<Sample> trajectory, int window_size){
		KState<MatcherCandidate, MatcherTransition, MatcherSample> state = new KState<MatcherCandidate, MatcherTransition, 
				MatcherSample>(window_size, -1);
		online_gid[suid].clear();
		Sample pre_sample = null;//to avoid stop point
		int match_counter = 0;
		int start_pos = 0;
		int end_pos = 0;
		int id=0;
		double total_time = 0;
		HashMap<Integer,Long> id_time_map= new HashMap<Integer,Long>();
		
		for(Sample sample : trajectory){
			//stop point, ignore it
			if(pre_sample != null && sample.lat == pre_sample.lat && sample.lon == pre_sample.lon){
				continue;
			}
			//start_time[start_pos++] = System.currentTimeMillis();
			
			MatcherSample matcher_sample = new MatcherSample(String.valueOf(id), 
					sample.utc.getTime(), new Point(sample.lon, sample.lat));
			
			id_time_map.put(id, System.currentTimeMillis());
			id++;
			
			Set<MatcherCandidate> vector = Common.matcher.execute(state.vector(), state.sample(),
		    		matcher_sample);
			MatcherCandidate converge = state.update_converge(vector, matcher_sample);
			pre_sample = sample;
			if(converge == null){
				continue;
			}
			else{
				long start_time = id_time_map.get(Integer.parseInt(converge.matching_id()));
				total_time += System.currentTimeMillis() - start_time;
				int gid = (int)converge.point().edge().id();
				online_gid[suid].add(gid);
				online_route[match_counter].clear();
				//get route
				if(converge.transition() != null){
					Route route = converge.transition().route();
					for(int k=0; k<route.size(); k++){
						online_route[match_counter].add(route.get(k));
					}
				}
				match_counter++;
			}
			
		}
		//calculate average delay
		
		//end time of unconvergency point
		double last_time = System.currentTimeMillis();
		/*if(end_pos == 0){
			
		}*/
		/*for(int i=0; i< end_pos; i++){
			//Common.logger.debug(start_time[i] + ";" + end_time[i]);
			total_time += (end_time[i] - start_time[i]);
		}*/
		if(match_counter > 0){
			total_time /= match_counter;
		}
		
		Common.logger.debug("window size: " + window_size + "; average delay: " + total_time);
		//rest point number in window
		Common.logger.debug("rest point number: " + state.samples().size());
		rest_counter[window_size] += state.samples().size();
		
		return total_time;
		
	}
	
	//store taxi data into memory,return max_suid
	public static int get_taxi_data(){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList<Integer> taxi_list=new ArrayList<Integer>();
		con = Common.getConnection();
		if (con == null) {
			System.out.println("Failed to make connection!");
			return -1;
		}
			
		try {
			//import the data from database;
			stmt = con.createStatement();
			//get max suid
			String sql="select suid from " + Common.ValidSampleTable +" where suid < 2000 group by suid";
			//String sql="select suid from " + sample_table +" group by suid order by suid";
			System.out.println(sql);
			rs = stmt.executeQuery(sql);
			    
			if(taxi_list==null){
				return -1;
			}
			taxi_list.clear();
			while(rs.next()){
			    taxi_list.add(rs.getInt("suid"));
			    //System.out.println(rs.getInt("suid"));
			}
			Common.logger.debug("get_suids finished!\n");
			Common.logger.debug("size: " + taxi_list.size() + "\n");
			int max_suid = taxi_list.get(0);
			for(int i:taxi_list){
				if(i > max_suid){
					max_suid = i;
				}
			}
			//int max_suid = Integer.getInteger(Collections.max(taxi_list));
			Common.logger.debug("max suid: " + max_suid + "\n");
			taxi = new ArrayList[max_suid + 1];
			offline_gid  = new ArrayList[max_suid + 1];
			online_gid  = new ArrayList[max_suid + 1];
			//ArrayList<Integer> taxi;
			//提取同一辆车的有效轨迹点,按时间排序
			//import the data from database;
			stmt = con.createStatement();
			sql="select * from " + Common.ValidSampleTable + " where suid < 2000 order by utc;";// + " and ostdesc not like '%定位无效%' order by utc";
			Common.logger.debug(sql);
			rs = stmt.executeQuery(sql);
			//int count = 0;
			int temp_id;
			while(rs.next()){
				temp_id = (int)rs.getLong("suid");
				if(taxi[temp_id] == null){
					taxi[temp_id] = new ArrayList<Sample>();
					offline_gid[temp_id] = new ArrayList<Integer>();
					online_gid[temp_id] = new ArrayList<Integer>();
				}
				taxi[temp_id].add(new Sample("_2010_03_01", rs.getLong("suid"), rs.getLong("utc"), rs.getLong("lat"), 
	    		rs.getLong("lon"), (int)rs.getLong("head"), true));
				//count++;
				//System.out.println(count + "\n");
			}
			return max_suid;
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			Common.dropConnection(con);
		}
		return -1;
	}
}
