/** 
* 2016年11月8日 
* TestTravelTime.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.experiment;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

import com.bmwcarit.barefoot.markov.KState;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.matcher.MatcherTransition;
import com.bmwcarit.barefoot.roadmap.Route;
import com.esri.core.geometry.Point;

public class TestTravelTime {
	private static Connection con = null;
	private static Statement stmt = null;
	private static HashMap<Integer, Integer> id_map = null;
	private static ArrayList<Sample>[] trajectory = null;
	//20100414: 1271174400 ~ 1271260800
	//8:00~22:00
	private static long start_time_bound = 1271174400 + 6*60*60;
	private static long end_time_bound = 1271260800 - 1*60*60;
	private static long period_thresold = 10 * 60;//min travel time for test
	private static int  min_sample_number = 7;
	private static int  max_route_number = 15;
	private static String date="_2010_04_14";
	
	private static ArrayList<Double> real_time_list = null;
	private static ArrayList<Double> exp_time_list = null;
	
	private static RoadTrafficAnalysis road_analyzer;
	private static TurningTrafficAnalysis turning_analyzer;
	
	//to record error info
	private static ArrayList<TravelTimeInfo> error_travel_info = null;
	
	private static int total_time = 0;
	private static int error_time = 0;
	
	private static int total_route_number = 0;
	private static int total_route_counter = 0;
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, SQLException {
		// TODO Auto-generated method stub
		Common.start_utc = 1271174400;
		Common.end_utc   = 1271260800;
		con = Common.getConnection();
		//70000~130000 split trajectory
		trajectory = new ArrayList[130000];
		id_map = new HashMap<Integer, Integer>();
		
		real_time_list = new ArrayList<Double>();
		exp_time_list  = new ArrayList<Double>();
		error_travel_info = new ArrayList<TravelTimeInfo>();
		
		Common.init(40000);
		Common.init_roadlist();
		Common.logger.debug("start bound: " + start_time_bound);
		Common.logger.debug("end bound: " + end_time_bound);
		try{
			stmt = con.createStatement();
			//load data
			read_id_map();
			read_trajectory();
			//start to test
			ArrayList<Sample> sample_list = null;
			ArrayList<Double> error_rate_list = new ArrayList<Double>();
			//read traffic
			road_analyzer = new RoadTrafficAnalysis(date, -1);
			turning_analyzer = new TurningTrafficAnalysis(date);
			turning_analyzer.read_all_turning();
			
			for(int i=0; i<trajectory.length; i++){			
				if(trajectory[i] != null){
					Common.logger.debug("trajectory " + i);
					sample_list = trajectory[i];
					//select samples in region
					sample_list = cut_out(sample_list);
					if(sample_list.size() < min_sample_number){
						//Common.logger.debug("samples not enough, ignored: " + sample_list.size());
						continue;
					}
					double error_rate =  compare_travel_time(i, sample_list);
					if(error_rate == -1){
						continue;
					}
					else{
						error_rate_list.add(error_rate);
					}				
				}
			}
			
			double[][] data = new double[3][error_rate_list.size()];
			int pos = 0;
			double sum_total = 0;
			double sum_valid = 0;
			int  count_valid = 0;
			double error_rate = 0;
			
			for(int i=0; i<error_rate_list.size(); i++){
				error_rate = error_rate_list.get(i);
				//Common.logger.debug("error rate: " + error_rate);
				data[0][pos] = error_rate;
				data[1][pos] = real_time_list.get(i);
				data[2][pos++] = exp_time_list.get(i);
				sum_total += Math.abs(error_rate);
				if(error_rate < 2){
					sum_valid += Math.abs(error_rate);
					count_valid++;
				}
			}
			Chart.output(data, "/home/zyu/error_rate");
			sum_total /= error_rate_list.size();
			sum_valid /= count_valid;
			Common.logger.debug("total average error rate: " + sum_total);
			Common.logger.debug("valid average error rate: " + sum_valid);
			
			Common.logger.debug("total time: " + total_time);
			Common.logger.debug("error time: " + error_time);
			
			double ave_route_number = total_route_number / total_route_counter;
			Common.logger.debug("average route size: " + ave_route_number);
			
			int counter = 1;
			for(TravelTimeInfo travel_info:error_travel_info){
				Common.logger.debug(counter + ":");
				Common.logger.debug(travel_info.toString());
				counter++;
			}
			
			analyse_result("/home/zyu/error_rate");
			//analyse_result("C:/Users/rainymoon911/Downloads/error_rate_7");
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
	}
	
	public static double bytes2Double(byte[] arr, int start) {  
        long value = 0;  
        for (int i = start; i < start + 8; i++) {  
            value |= ((long) (arr[i] & 0xff)) << (8 * (i - start));  
        }  
        return Double.longBitsToDouble(value);  
    } 
	
	public static int bytes2Int(byte[] bytes, int start) {  
	    int number = bytes[start] & 0xFF;  
	    // "|="按位或赋值。  
	    number |= ((bytes[start + 1] << 8) & 0xFF00);  
	    number |= ((bytes[start + 2] << 16) & 0xFF0000);  
	    number |= ((bytes[start + 3] << 24) & 0xFF000000);  
	    return number;  
	}  
	
	//map of id->suid
	public static void read_id_map() throws SQLException{
		id_map = new HashMap<Integer, Integer>();
		String sql = "select * from hisinfo;";
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()){
			//ignored short trajectory
			long start_time = rs.getLong("start_time");
			long end_time = rs.getLong("end_time");
			long diff_time = end_time - start_time;
			if(diff_time < period_thresold){
				//Common.logger.debug("travel time too short, ignored: " + diff_time);
				continue;
			}
			
			int gen_id = rs.getInt("id");
			int suid   = rs.getInt("origin_code");
			
			id_map.put(gen_id, suid);
		}
	}
	
	public static void read_trajectory() throws SQLException, IOException{
		String sql = "select * from hisbin;";
		ResultSet rs = stmt.executeQuery(sql);
		int split_id = 70000;
		while(rs.next()){
			//byte[] blob = rs.getBytes("bin");
			//System.out.println(blob);
			int gen_id = rs.getInt("id_his");
			//Common.logger.debug("gen id: " + gen_id);
			Integer suid = id_map.get(gen_id);
			//trajectory time is too short
			if(suid == null){
				continue;
			}
			//Common.logger.debug("suid: " + suid);
			int id = gen_id - 1126180;
			if(trajectory[id] == null){
				trajectory[id] = new ArrayList<Sample>();
			}
			//read sample
			InputStream in = rs.getBinaryStream("bin");
			byte[] buffer = new byte[in.available()];
			in.read(buffer);
			int counter = buffer.length / 44;//44 bytes a record
			//ignore trajectory whose samples is not enough
			if(counter < min_sample_number){
				//Common.logger.debug("samples not enough, ignored");
				continue;
			}
			//System.out.println(buffer.length + ":" + buffer.toString());
			int pre_utc = -1;
			for(int i=0; i<counter; i++){
				int start = i * 44;
				double lat = bytes2Double(buffer, start);
				double lon = bytes2Double(buffer, start + 8);
				//double high = bytes2Double(buffer, start + 16);
				double head = bytes2Double(buffer, start + 24);
				//double speed = bytes2Double(buffer, start + 32);
				int utc = bytes2Int(buffer,start + 40);
				//interval > 10min
				if(pre_utc != -1 && utc - pre_utc > 600){
					id = split_id++;
					trajectory[id] = new ArrayList<Sample>();
				}
				pre_utc = utc;
				/*System.out.println(lat + "; " + lon + "; " + high + "; " 
						+ direct + "; " + speed + "; " + time);*/
				trajectory[id].add(new Sample((long)suid, (long)utc, lat, 
			    		lon, (int)head, true));
			}
		}
	}
	
	//select samples in region
	public static ArrayList<Sample> cut_out(ArrayList<Sample> origin_list){
		ArrayList<Sample> reduced_list = new ArrayList<Sample>();
		for(Sample sample:origin_list){
			long utc = sample.utc.getTime()/1000;
			if(utc < start_time_bound){
				continue;
			}
			if(utc > end_time_bound){
				break;
			}
			reduced_list.add(sample);
		}
		
		return reduced_list;
	}
	
	public static double compare_travel_time(int id, ArrayList<Sample> sample_list) throws SQLException{
		long start_time = sample_list.get(0).utc.getTime()/1000;
		long end_time   = sample_list.get(sample_list.size()-1).utc.getTime()/1000;
		long stop_time = 0;
		int  diff_time = (int) (end_time - start_time);
		if(diff_time < period_thresold){
			Common.logger.debug("travel time too short, ignored");
			return -1;
		}
		double error_rate = 0.0;
		KState<MatcherCandidate, MatcherTransition, MatcherSample> state = 
				new KState<MatcherCandidate, MatcherTransition, 
				MatcherSample>(6, -1);
		Sample pre_sample = null;//to avoid stop point
		Sample cur_sample = null;
		Sample pre_converge = null;
		double start_offset = -1;
		double end_offset = -1;
		//start_time = 0;
		end_time = 0;
		ArrayList<Integer> gid_list = new ArrayList<Integer>();
		ArrayList<Integer> seq_list = new ArrayList<Integer>();
		ArrayList<Sample>  old_sample_list = new ArrayList<Sample>();
		ArrayList<Sample>  new_sample_list = new ArrayList<Sample>();
		int counter = 0;
		
		Common.logger.debug("start to map matching");
		
		for(int i=0; i<sample_list.size(); i++){
			cur_sample = sample_list.get(i);
			//stop point, ignore it
			if(pre_sample != null && cur_sample.lat == pre_sample.lat && cur_sample.lon == pre_sample.lon){
				continue;
			}
			MatcherSample matcher_sample = new MatcherSample(String.valueOf(i), 
					cur_sample.utc.getTime(), new Point(cur_sample.lon, cur_sample.lat));
			Set<MatcherCandidate> vector = Common.matcher.execute(state.vector(), state.sample(),
		    		matcher_sample);
			MatcherCandidate converge = state.update_converge(vector, matcher_sample);
			
			pre_sample = cur_sample;
			if(converge == null){
				continue;
			}
			else{
				int converge_id = Integer.parseInt(converge.matching_id());
				Sample tmp_sample = sample_list.get(converge_id);
				
				//correct gps location
				Point position = converge.point().geometry(); // position
				Sample new_sample = new Sample(tmp_sample.suid, tmp_sample.utc.getTime()/1000, position.getY(), 
						position.getX(), tmp_sample.head, true);
				
				//stop point
				if(pre_converge != null && new_sample.lat == pre_converge.lat && new_sample.lon == pre_converge.lon){
					stop_time += new_sample.utc.getTime()/1000 - pre_converge.utc.getTime()/1000;
					counter--;
				}
				
				old_sample_list.add(tmp_sample);
				new_sample_list.add(new_sample);
				
				int seq = Common.get_seq(tmp_sample);
				int gid = (int)converge.point().edge().id();
				//Common.logger.debug("gid: " + gid);
				double offset = converge.point().fraction();
				tmp_sample.offset = offset;	
				
				//first point
				if(start_offset == -1){
					start_offset = offset;
					start_time = tmp_sample.utc.getTime()/1000;
				}
				
				//get route
				if(converge.transition() != null){
					Route route = converge.transition().route();
					if(route.size() > 0 && route.get(route.size()-1).id() != gid){
						Common.logger.debug("route error");
						return -1;
					}
					total_route_number += route.size();
					total_route_counter ++;
					if(route.size() > max_route_number){
						Common.logger.debug("route too long");
						return -1;
					}
					
					
					//judge whether average speed of route exceed max speed
					if(pre_converge != null && check_route_speed(pre_converge, tmp_sample, route) == false){
						Common.logger.debug("route too fast");
						return -1;
					}
								
					//Common.logger.debug("route: " + route.toString());
					for(int k=0; k<route.size()-1; k++){
						gid_list.add((int)route.get(k).id());
						seq_list.add(seq);
					}
				}
				gid_list.add(gid);
				seq_list.add(seq);
				counter++;
				
				//record last point
				end_offset = offset;
				end_time = tmp_sample.utc.getTime()/1000;
				
				pre_converge = new_sample;
				
			}
		}
		Common.logger.debug("map matching finished");
		
		if(counter < min_sample_number){
			Common.logger.debug("matched samples not enough, ignored");
			return -1;
		}
		double real_time = (int) (end_time - start_time - stop_time);
		if(real_time < period_thresold){
			Common.logger.debug("matched travel time too short, ignored");
			return -1;
		}
		
		remove_dup(gid_list, seq_list);
		if(gid_list.size() < min_sample_number){
			Common.logger.debug("matched samples not enough, ignored");
			return -1;
		}
		Common.logger.debug("gid list: " + gid_list.toString());
		Common.logger.debug("seq list: " + seq_list.toString());
		
		//check size of gid_list and seq_list
		if(gid_list.size() != seq_list.size()){
			Common.logger.debug("list of gid and seq not equal");
			return -1;
		}
		//return 0;
		//calculate total time
		ArrayList<Double> time_list = new ArrayList<Double>();
		double exp_time = 0.0;
		double tmp_time = 0.0;
		
		//start point
		int gid = gid_list.get(0);
		int next_gid = -1;
		int seq = seq_list.get(0);
		double road_speed = get_road_speed(gid, seq);
		double turning_time = 0.0;
		if(road_speed <= 0){
			Common.logger.debug("no road speed.");
			return -1;
		}
		tmp_time = (1 - start_offset) * Common.roadlist[gid].length / road_speed;
		exp_time += tmp_time;
		time_list.add(tmp_time);
		
		next_gid = gid_list.get(1);
		turning_time = get_turning_time(gid, next_gid, seq);
		
		//no turning time
		if(turning_time  <= 0){
			Common.logger.debug("no turning traffic. " + gid + "," + next_gid + " seq: " + seq);
			return -1;
		}
		exp_time += turning_time;
		time_list.add(turning_time);
		
		for(int i=1; i<gid_list.size()-1;i++){
			gid = gid_list.get(i);
			seq = seq_list.get(i);
			//road time
			road_speed = get_road_speed(gid, seq);
			if(road_speed <= 0){
				Common.logger.debug("no road speed.");
				return -1;
			}
			tmp_time = Common.roadlist[gid].length / road_speed;//travel through entire road
			exp_time +=  tmp_time;
			time_list.add(tmp_time);
			//turning time
			next_gid = gid_list.get(i+1);
			turning_time = get_turning_time(gid, next_gid, seq);
			
			//no turning time
			if(turning_time  <= 0){
				Common.logger.debug("no turning traffic. " + gid + "," + next_gid + " seq: " + seq);
				return -1;
			}
			exp_time += turning_time;
			time_list.add(turning_time);
			
		}
		//last point
		gid = gid_list.get(gid_list.size() - 1);
		seq = seq_list.get(gid_list.size() - 1);
		road_speed = road_analyzer.road_traffic[gid][seq];
		if(road_speed <= 0){
			Common.logger.debug("no road speed.");
			return -1;
		}
		tmp_time = end_offset * Common.roadlist[gid].length / road_speed;
		exp_time += tmp_time;
		time_list.add(tmp_time);
		
		if(exp_time <=0){
			Common.logger.debug("error: exp_time<0");
			return -1;
		}
		//error_rate = Math.abs(real_time - exp_time)/ real_time;
		error_rate = (exp_time - real_time)/ real_time;
		Common.logger.debug("error rate: " + error_rate);
		real_time_list.add(real_time);
		exp_time_list.add(exp_time);
		//record error info
		if(Math.abs(error_rate) > 0.7){
			int gen_id = id + 1126180;
			TravelTimeInfo travel_info = new TravelTimeInfo(gen_id, gid_list, seq_list, time_list, 
					old_sample_list, new_sample_list, counter, error_rate,real_time, exp_time);
			error_travel_info.add(travel_info);
		}
		total_time += real_time;
		error_time += Math.abs(real_time-exp_time);
		
		return error_rate;
	}
	
	public static void remove_dup(ArrayList<Integer> origin_list, ArrayList<Integer> seq_list){
		ArrayList<Integer> new_gid_list = new ArrayList<Integer>();
		ArrayList<Integer> new_seq_list = new ArrayList<Integer>();
		
		int pre_gid = -1;
		int cur_gid;
		int cur_seq;
		for(int i=0; i<origin_list.size(); i++){
			cur_gid = origin_list.get(i);
			cur_seq = seq_list.get(i);
			
			if(pre_gid == -1){
				new_gid_list.add(cur_gid);
				new_seq_list.add(cur_seq);
				pre_gid = origin_list.get(i);
				continue;
			}
			
			if(cur_gid == pre_gid){
				continue;
			}
			pre_gid = cur_gid;
			new_gid_list.add(cur_gid);
			new_seq_list.add(cur_seq);
		}
		origin_list.clear();
		seq_list.clear();
		origin_list.addAll(new_gid_list);
		seq_list.addAll(new_seq_list);
	}
	
	public static double get_road_speed(int gid, int seq){
		double road_speed = road_analyzer.road_traffic[gid][seq];
		if(road_speed <= 0){
			road_speed = road_analyzer.road_traffic[gid][seq-1];
		}
		if(road_speed <= 0){
			road_speed = road_analyzer.road_traffic[gid][seq-2];
		}
		if(road_speed <= 0){
			road_speed = road_analyzer.road_traffic[gid][seq+1];
		}
		if(road_speed <= 0){
			road_speed = road_analyzer.road_traffic[gid][seq+2];
		}
		return road_speed;
		
	}
	
	public static double get_turning_time(int gid, int next_gid, int seq){
		double turning_time = turning_analyzer.turning_traffic[seq].get_turning_time(gid, next_gid);
		if(turning_time < 0){
			turning_time = turning_analyzer.turning_traffic[seq-1].get_turning_time(gid, next_gid);
		}
		if(turning_time < 0){
			turning_time = turning_analyzer.turning_traffic[seq-2].get_turning_time(gid, next_gid);
		}
		if(turning_time < 0){
			turning_time = turning_analyzer.turning_traffic[seq+1].get_turning_time(gid, next_gid);
		}
		if(turning_time < 0){
			turning_time = turning_analyzer.turning_traffic[seq+2].get_turning_time(gid, next_gid);
		}
		
		return turning_time;
	}
	
	//check whether average speed of route exceed max speed
	public static boolean check_route_speed(Sample pre_sample, Sample cur_sample, Route route){
		double interval = cur_sample.utc.getTime()/1000 - pre_sample.utc.getTime()/1000;
		double  total_length = 0;
		total_length += Common.roadlist[pre_sample.gid].length * (1 - pre_sample.offset);
		if(route.size() > 1){
			for(int k=0; k<route.size()-1; k++){
				int tmp_gid = (int)route.get(k).id();
				total_length += Common.roadlist[tmp_gid].length;
			}
			total_length += cur_sample.offset * Common.roadlist[cur_sample.gid].length;
			if(total_length / interval > Common.max_speed){
				//Common.logger.debug("route wrong, too fast");
				return false;
			}
		}
		return true;
	}
	
	public static void analyse_result(String path){
		double[][] data = read_data(path);
		
		Common.logger.debug(data.length);
		
		//analyse error rate
		int[] pos_count_rate = new int[30];
		int[] neg_count_rate = new int[30];
		double error_rate;
		int idx;
		for(int i=0; i<data[0].length; i++){
			error_rate = data[0][i];
			idx = (int) (Math.abs(error_rate)/0.1);
			
			if(error_rate > 0){
				if(idx > 29){
					pos_count_rate[29]++;
				}
				else{
					pos_count_rate[(int) (error_rate/0.1)]++;
				}
			}
			else{
				if(idx > 29){
					neg_count_rate[29]++;
				}
				else{
					neg_count_rate[(int) (Math.abs(error_rate)/0.1)]++;
				}
			}
			if(idx > 10){
				real_time_list.add(data[1][i]);
				exp_time_list.add(data[2][i]);
			}
			
		}
		
		
		double real_time, exp_time;
		int low_counter = 0;
		int high_counter =0;
		for(int i=0;i <data[1].length; i++){
			real_time = data[1][i];
			exp_time  = data[2][i];  
			if(exp_time < real_time){
				low_counter++;
			}
			else{
				high_counter++;
			}
		}
		
		Common.logger.debug("exp>real: " + high_counter);
		
		for(int i=0; i<pos_count_rate.length; i++){
			if(pos_count_rate[i] == 0){
				continue;
			}
			Common.logger.debug(i*10 + "%" + "-" + (i*10+10) + "%: " + pos_count_rate[i]);
		}
		
		Common.logger.debug("exp<real: " + low_counter);
		
		for(int i=0; i<neg_count_rate.length; i++){
			if(neg_count_rate[i] == 0){
				continue;
			}
			Common.logger.debug(i*10 + "%" + "-" + (i*10+10) + "%: " + neg_count_rate[i]);
		}
		
		Common.logger.debug("error rate > 1: ");
		for(int i=0; i< real_time_list.size(); i++){
			Common.logger.debug("real: " + real_time_list.get(i) + ", exp: " + exp_time_list.get(i));
		}
	}
	
	public static double[][] read_data(String path){
		double[][] les = null;
        try
        {
            Scanner scanner = new Scanner(new File(path));
            int i = 0;
            while(scanner.hasNextLine())
            {
                String[] line = scanner.nextLine().split(" ");
                if(i==0){
                	les = new double[3][line.length];
                }
                for(int j = 0; j < line.length; j++)
                {
                    les[i][j] = Double.parseDouble(line[j]);
                }
                
                i++;
            }
            scanner.close();
            return les;
        }
        catch(FileNotFoundException e)
        {
            e.printStackTrace();
        }
        return null;
	}

}
