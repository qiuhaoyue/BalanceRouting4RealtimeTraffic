package indi.zyu.realtraffic.updater;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** 
 * 2016Äê4ÔÂ28ÈÕ 
 * Updater.java 
 * author:ZhangYu
 */

//reponsible for update gps data.
public class GPSUpdater {
	
	ArrayList<Sample> queue_gps = null;
	int update_thresold = 2000;//max size of queue, if exceed, do update
	Connection con = null;
	Statement stmt = null;
	private static Lock lock = null;
	String gps_table = "";
	
	public boolean addGPS(ArrayList<Sample> slice){
		if(slice.isEmpty() || slice == null){
			return false;
		}
		lock.lock();
		try{
			queue_gps.addAll(slice);
		}
		finally{
			lock.unlock();
		}	
		return true;		
	}
	
	public GPSUpdater(final int update_thresold, String gps_table) throws SQLException{
		this.update_thresold = update_thresold;
		this.gps_table = gps_table;
		lock = new ReentrantLock();
		queue_gps = new ArrayList<Sample>();
		
		//check whether gps table exists, if not, create it.
		con = Common.getConnection();
		con.setAutoCommit(false);
		try{
			stmt = con.createStatement();
			String sql = "select count(*) from pg_class where relname = '" + gps_table + "';";
			Common.logger.debug(sql);
			ResultSet rs = stmt.executeQuery(sql);
			if(rs.next()){
				int count = rs.getInt(1);
				//table not exists, create it
				if(count == 0){
					sql = "CREATE TABLE " + gps_table + "(suid bigint, utc bigint, lat double precision, lon double precision,"
							+ "head bigint, stop integer, gid integer, Edge_offset double precision, route text,"
							+ "interval double precision, pre_gid integer, pre_offset double precision);";
					Common.logger.debug(sql);
					//Statement tmp_stmt = con.createStatement();
					//int i = tmp_stmt.executeUpdate(sql);
					stmt.executeUpdate(sql);
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
		
		Timer timer = new Timer();
		timer.schedule(new TimerTask(){
			public void run(){
	            if(queue_gps.size() > update_thresold){
	            	try{
	            		update();
	            	}
	            	catch (SQLException e) {
	        			// TODO Auto-generated catch block
	        			e.printStackTrace();
	        		}
	            }       
	        }  
		}, 0, 20000);
	}
	
	private void update() throws SQLException{
		String sql="";
		ArrayList<Sample> copy = new ArrayList<Sample>();
		lock.lock();
		try{			
			copy.addAll(queue_gps);
			queue_gps.clear();
		}
		finally{
			lock.unlock();
		}
		try{
			for(Sample i : copy){
				sql = "Insert into " + gps_table + " (suid, utc, lat, lon, head, "
						+ "stop, gid, edge_offset, route, interval, pre_gid, pre_offset) values \n";
				sql = sql + i.getAttributeForInsert() + ";";
				//Common.logger.debug(sql);
				stmt.addBatch(sql);
			}
			stmt.executeBatch();
			//Common.logger.debug("gps updater insert!");
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			con.commit();
		}
		
	}
}
