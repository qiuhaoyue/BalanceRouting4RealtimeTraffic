/** 
* 2016Äê8ÔÂ17ÈÕ 
* HistoryTrafficUpdater.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.updater;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.road.AllocationRoadsegment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HistoryTrafficUpdater {
	private Connection con = null;
	private Statement stmt = null;
	
	public HistoryTrafficUpdater() throws SQLException{
		con = Common.getConnection();
		try{
			stmt = con.createStatement();
			String sql = "select count(*) from pg_class where relname = '" + Common.history_road_slice_table + "1';";
			Common.logger.debug(sql);
			ResultSet rs = stmt.executeQuery(sql);
			if(rs.next()){
				int count = rs.getInt(1);
				//table not exists, create history traffic table
				if(count == 0){
					//create all slice table
					for(int i=1; i<= Common.max_seg; i++){
						//road time
						String road_slice_table = Common.history_road_slice_table + i;
						//create slice table
						sql = "CREATE TABLE " + road_slice_table + "(gid integer, base_gid integer, length integer,"
								+ " class_id integer, time double precision, average_speed double precision);";
						Common.logger.debug(sql);
						stmt.executeUpdate(sql);
						
						//turning time
						/*String turning_slice_table = Common.history_turning_slice_table + i;
						sql = "DROP TABLE IF EXISTS " + turning_slice_table + ";";
						stmt.executeUpdate(sql);
						//create slice table
						sql = "CREATE TABLE " + turning_slice_table + "(gid integer, next_gid integer,"
								+ " time double precision);";
						Common.logger.debug(sql);
						stmt.executeUpdate(sql);*/
					}
				}
			}
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		//check if history traffic table exists, if not, create it
		
	}
	
	public boolean update(int gid, int seq, double speed) throws SQLException{
		try{
			//check whether traffic of the road exists
			//already exists, update
			if(Common.default_traffic[gid][seq] > 0){
				AllocationRoadsegment road = Common.roadlist[gid];
				double new_speed = Common.default_traffic[gid][seq] * Common.history_update_alpha 
						+ speed * (1- Common.history_update_alpha);
				double new_time = road.length / new_speed;
				String sql = "Update " + Common.history_road_slice_table + seq
						+ " set time=" + new_time + ",average_speed=" + new_speed + " where gid=" + gid;
				
				//Common.logger.debug(sql);
				stmt.executeUpdate(sql);
			}
			//insert
			else{
				insert(gid, seq, speed);
			}
			
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
	
	public boolean insert(int gid, int seq, double speed) throws SQLException{
		try{
			//insert road traffic
			String sql = "Insert into " + Common.history_road_slice_table + seq
					+ "(gid, base_gid, length, class_id, time, average_speed) values \n";
			AllocationRoadsegment road = Common.roadlist[gid];
			sql += "(" + road.gid + ", " + road.base_gid + ", " + road.length + ", " + road.class_id + ", " 
			+ road.length/speed + ", " + speed + ");";
			//Common.logger.debug(sql);
			stmt.executeUpdate(sql);
			
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
