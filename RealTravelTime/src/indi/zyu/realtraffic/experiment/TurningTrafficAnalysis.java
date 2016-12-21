package indi.zyu.realtraffic.experiment;

import indi.zyu.realtraffic.common.Common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class TurningTrafficAnalysis {
	//store speed of roads for a whole day
	public  int[]      traffic_counter = null;//traffic number for each period
	public  double[]   average_time = null;//average speed of all roads
	public  TurningTimeForExp[] turning_traffic = null;
	private int[]      counter = null;
	private String Date_Suffix;
		
	private Connection con = null;
	private Statement stmt = null;
	
	//for real-time traffic data
	public TurningTrafficAnalysis(String Date_Suffix) throws SQLException{
		//turning_traffic = new double[(int)Common.max_seg + 1][20000];
		traffic_counter = new int[(int)Common.max_seg + 1];
		average_time = new double[(int)Common.max_seg + 1];
		this.Date_Suffix = Date_Suffix;
		//start read traffic from database
		con = Common.getConnection();
		//con.setAutoCommit(false);
		try{
			stmt = con.createStatement();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
			
		
	}
	public void read_simple_turning() throws SQLException{
		//read by period
		for(int i=1; i<=Common.max_seg; i++){
			int pos = 0;
			try{
				String traffic_table = Common.real_turning_slice_table + i + Date_Suffix;
				//String traffic_table = Common.history_road_slice_table + i + Date_Suffix;
				String sql = "select count(*) from pg_class where relname = '" + traffic_table + "';";
				ResultSet rs = stmt.executeQuery(sql);
				if(rs.next()){
					int count = rs.getInt(1);
					//table not exists, create it
					if(count == 0){
						Common.logger.debug("table analysed not exists");;
					}
				}
				//read data
				sql = "select count(*) from " + traffic_table + " where gid != next_gid and time > 40;";
				//sql = "select * from " + traffic_table + " where gid != next_gid;";
							
				rs = stmt.executeQuery(sql);
				while(rs.next()){
					//Common.logger.debug(gid);
					double time = rs.getInt(1);
					//double time = rs.getDouble("time");
					//turning_traffic[i][pos++] = time;
					traffic_counter[i]++;
					average_time[i] += time;
								
				}
				if(traffic_counter[i] == 0){
					average_time[i] = 0;
				}
				else{
					average_time[i] /= traffic_counter[i];
				}
			}
			catch (SQLException e) {
				e.printStackTrace();
				con.rollback();
			}
			finally{
				con.commit();
			}
						
		}
	}
	
	public void read_all_turning() throws SQLException{
		turning_traffic = new TurningTimeForExp[Common.max_seg + 1];
		for(int i=1; i<=Common.max_seg; i++){
			turning_traffic[i] = new TurningTimeForExp(100000);
			try{
				String traffic_table = Common.real_turning_slice_table + i + Date_Suffix;
				//String traffic_table = Common.history_road_slice_table + i + Date_Suffix;
				String sql = "select count(*) from pg_class where relname = '" + traffic_table + "';";
				ResultSet rs = stmt.executeQuery(sql);
				if(rs.next()){
					int count = rs.getInt(1);
					//table not exists, create it
					if(count == 0){
						Common.logger.debug("table analysed not exists");;
					}
				}
				//read data
				sql = "select * from " + traffic_table + " where gid != next_gid;";
							
				rs = stmt.executeQuery(sql);
				double[] ave_turning_time = new double[180000];
				int[] ave_turning_counter = new int[180000];
				while(rs.next()){
					//Common.logger.debug(gid);
					int gid = rs.getInt("gid");
					int next_gid = rs.getInt("next_gid");
					double time = rs.getDouble("time");
					if(time > 0){
						turning_traffic[i].add_turning_time(gid, next_gid, time);
						ave_turning_time[gid] += time;
						ave_turning_counter[gid] ++;
					}
										
				}
				for(int j=0; j<ave_turning_time.length; j++){
					if(ave_turning_counter[j] > 0){
						ave_turning_time[j] /= ave_turning_counter[j];
						turning_traffic[i].ave_turning_time.put(j, ave_turning_time[j]);
					}
				}
			}
			catch (SQLException e) {
				e.printStackTrace();
				con.rollback();
			}
			finally{
				con.commit();
			}
						
		}
	}
}
