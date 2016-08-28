package indi.zyu.realtraffic.updater;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.road.AllocationRoadsegment;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

/** 
 * 2016年5月27日 
 * RealTrafficUpdater.java 
 * author:ZhangYu
 */

//update traffic time and turning time
public class RealTrafficUpdater {
	
	private Connection con = null;
	private Statement stmt = null;
	//private static Lock lock = null;
	
	public RealTrafficUpdater() throws SQLException{
		
		con = Common.getConnection();
		//con.setAutoCommit(false);
		try{
			stmt = con.createStatement();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		
		//create all slice table
		for(int i=1; i<= Common.max_seg; i++){
			
			try{
				//road time
				String road_slice_table = Common.real_road_slice_table + i + Common.Date_Suffix;
				String sql = "DROP TABLE IF EXISTS " + road_slice_table + ";";
				stmt.executeUpdate(sql);
				//create slice table
				sql = "CREATE TABLE " + road_slice_table + "(gid integer, base_gid integer, length integer,"
						+ " class_id integer, time double precision, average_speed double precision, is_sensed boolean);";
				Common.logger.debug(sql);
				stmt.executeUpdate(sql);
				
				//turning time
				String turning_slice_table = Common.real_turning_slice_table + i + Common.Date_Suffix;
				sql = "DROP TABLE IF EXISTS " + turning_slice_table + ";";
				stmt.executeUpdate(sql);
				//create slice table
				sql = "CREATE TABLE " + turning_slice_table + "(gid integer, next_gid integer,"
						+ " time double precision);";
				Common.logger.debug(sql);
				stmt.executeUpdate(sql);
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
	public boolean update(int gid, int seq) throws SQLException{
		try{
			//insert road traffic
			String sql = "Insert into " + Common.real_road_slice_table + seq + Common.Date_Suffix
					+ "(gid, base_gid, length, class_id, time, average_speed, is_sensed) values \n";
			AllocationRoadsegment road = Common.roadlist[gid];
			sql += "(" + road.gid + ", " + road.base_gid + ", " + road.length + ", " + road.class_id + ", " 
			+ road.time + ", " + road.avg_speed + ", true);";
			//Common.logger.debug(sql);
			stmt.executeUpdate(sql);
			
			//update history traffic
			Common.history_traffic_updater.update(gid, seq, road.avg_speed);
			
			//insert turning traffic
			HashMap<Integer, Double> turing_time = road.get_all_turning_time();
			Set<Entry<Integer, Double>> entryset=turing_time.entrySet();
			for(Entry<Integer, Double> m:entryset){
				sql = "Insert into " + Common.real_turning_slice_table + seq + Common.Date_Suffix
						+ "(gid, next_gid, time) values \n";
				sql += "(" + gid + ", " + m.getKey() + ", " + m.getValue() + ");";
				stmt.addBatch(sql);
			}
			stmt.executeBatch();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		    con.rollback();
		    return false;
		}
		finally{
			con.commit();
		}
		return true;
	}
	
	//update inferred traffic
	public boolean update_road(int gid, int seq, double speed) throws SQLException{
		try{
			if(speed <= 0){
				return false;
			}
			//insert road traffic
			String sql = "Insert into " + Common.real_road_slice_table + seq + Common.Date_Suffix
					+ "(gid, base_gid, length, class_id, time, average_speed, is_sensed) values \n";
			AllocationRoadsegment road = Common.roadlist[gid];
			sql += "(" + road.gid + ", " + road.base_gid + ", " + road.length + ", " + road.class_id 
					+ ", " + road.length/speed + ", " + speed + ", false);";
			//Common.logger.debug(sql);
			stmt.executeUpdate(sql);
			
			//update history traffic
			Common.history_traffic_updater.update(gid, seq, speed);
			
		}
		catch (SQLException e) {
		    e.printStackTrace();
		    con.rollback();
		    return false;
		}
		finally{
			con.commit();
		}
		return true;
	}
}
