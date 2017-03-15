/** 
* 2016Äê7ÔÂ2ÈÕ 
* TrafficAnalysis.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.experiment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;



import java.util.HashMap;

import indi.zyu.realtraffic.common.Common;

//to analyse quality of traffic
public class RoadTrafficAnalysis {
	//store speed of roads for a whole day
	public  double[][] road_traffic = null;
	public  int[]      traffic_counter = null;//traffic number for each period
	public  double[]   average_speed = null;//average speed of all roads
	private int[]      counter = null;
	//average speed of roads in same class
	public  double[][] default_class_traffic = null;
	
	private Connection con = null;
	private Statement stmt = null;
	
	//for real-time traffic data
	public RoadTrafficAnalysis(String Date_Suffix, int class_id) throws SQLException{
		int length = Common.roadlist.length;
		road_traffic = new double[length][(int)Common.max_seg + 1];
		traffic_counter = new int[(int)Common.max_seg + 1];
		average_speed = new double[(int)Common.max_seg + 1];
		counter = new int[length];
		//28 classes of road ,max id is 305, set 350 here
		default_class_traffic = new double [350][(int)Common.max_seg + 1];
		//start read traffic from database
		con = Common.getConnection();
		//con.setAutoCommit(false);
		try{
			stmt = con.createStatement();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		
		//read by period
		for(int i=1; i<=Common.max_seg; i++){
			try{
				String traffic_table = Common.real_road_slice_table + i + Date_Suffix;
				//String traffic_table = Common.history_road_slice_table + i + Date_Suffix;
				String sql = "select count(*) from pg_class where relname = '" + traffic_table + "';";
				ResultSet rs = stmt.executeQuery(sql);
				if(rs.next()){
					int count = rs.getInt(1);
					//table not exists, create it
					if(count == 0){
						Common.logger.debug("table " + traffic_table + " does not exist");;
					}
				}
				//read data
				if(class_id == -1){
					sql = "select * from " + traffic_table + " ;";
				}
				else if(class_id == -2){
					sql = "select * from " + traffic_table + " where is_sensed=true;";
				}
				else if(class_id == -3){
					sql = "select * from " + traffic_table + " where is_sensed=false;";
				}
				else{
					sql = "select * from " + traffic_table + " where class_id=" + class_id + ";";
					//temp sql
					/*sql = "select * from " + traffic_table + " where class_id=" 
							+ class_id + " and is_sensed=true;";*/
					
				}
				
				rs = stmt.executeQuery(sql);
				//int[] class_id_counter = new int[350];
				while(rs.next()){
					int gid = rs.getInt("gid");
					//int tmp_class_id = rs.getInt("class_id");
					//Common.logger.debug(gid);
					double speed = rs.getDouble("average_speed");
					road_traffic[gid][i] = speed;
					if(speed > 0){
						counter[gid]++;
						/*class_id_counter[class_id]++;
						default_class_traffic[class_id][i] += speed;*/
					}
					traffic_counter[i]++;
					average_speed[i] += speed;
				}
				if(traffic_counter[i] == 0){
					average_speed[i] = 0;
				}
				else{
					average_speed[i] /= traffic_counter[i];
				}	
				//get average speed of roads in same class
				/*for(int j=0; j<class_id_counter.length; j++){
					int counter = class_id_counter[j];
					if(counter > 0){
						default_class_traffic[j][i] /= counter;
					}
				}*/
			}
			catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback();
			}
			finally{
				con.commit();
			}
			
		}
		for(int i=0; i<Common.roadlist.length; i++){
			if(counter[i] > 50){
				//Common.logger.debug(i + ": " + counter[i]);
			}
			
		}
		Common.dropConnection(con);
	}
	
	
	
	//for offline traffic data, need to transform gid
	public RoadTrafficAnalysis(int seg) throws SQLException{
		String time_table = "allroad_time_nonrush_";
		String road_table = "oneway_test";
		
		int length = Common.roadlist.length;
		//road_traffic = new double[length][(int)Common.max_seg + 1];
		//traffic_counter = new int[(int)Common.max_seg + 1];
		//average_speed = new double[(int)Common.max_seg + 1];
		
		road_traffic = new double[length][seg + 1];
		traffic_counter = new int[seg + 1];
		average_speed = new double[seg + 1];
		counter = new int[length];
		//start read traffic from database
		con = Common.getConnection();
		//con.setAutoCommit(false);
		try{
			stmt = con.createStatement();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		//read map info from gid(splited) to base_gid
		HashMap<Integer, Integer> gid_map = new HashMap<Integer, Integer>();
		
		String sql = "select gid, old_gid from " + road_table + ";";
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()){
			int gid = rs.getInt("gid");
			int old_gid = rs.getInt("old_gid");
			if(gid == old_gid){
				gid_map.put(gid, old_gid * 2);
			}
			else{
				gid_map.put(gid, old_gid * 2 + 1);
			}
		}
		
		//read by period
		//for(int i=1; i<=Common.max_seg; i++){
		for(int i=1; i<=12; i++){
			try{
				String traffic_table = time_table + i;
				sql = "select count(*) from pg_class where relname = '" + traffic_table + "';";
				rs = stmt.executeQuery(sql);
				if(rs.next()){
					int count = rs.getInt(1);
					//table not exists, create it
					if(count == 0){
						Common.logger.debug("table analysed not exists");;
					}
				}
				//read data
				sql = "select * from " + traffic_table + " where reference>0" + ";";
				rs = stmt.executeQuery(sql);
				while(rs.next()){
					int gid = rs.getInt("gid");
					int real_gid = gid_map.get(gid);
					//Common.logger.debug(gid);
					double speed = rs.getDouble("average_speed");
					road_traffic[real_gid][i] = speed;
					if(speed > 0){
						counter[real_gid]++;
					}
					traffic_counter[i]++;
					average_speed[i] += speed;
				}
				average_speed[i] /= traffic_counter[i];
			}
			catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback();
			}
			finally{
				con.commit();
			}
			
		}
		for(int i=0; i<Common.roadlist.length; i++){
			if(counter[i] > 50){
				Common.logger.debug(i + ": " + counter[i]);
			}
			
		}
		Common.dropConnection(con);
	}
	
}
