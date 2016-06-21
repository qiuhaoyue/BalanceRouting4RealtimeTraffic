package indi.zyu.realtraffic.updater;

import indi.zyu.realtraffic.common.Common;

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
 * 2016Äê5ÔÂ5ÈÕ 
 * TravelTimeSliceUpdater.java 
 * author:ZhangYu
 */

public class TravelTimeSliceUpdater {
	private ArrayList<String> queue_traffic = null;
	private int update_thresold = 2000;//max size of queue, if exceed, do update
	private int slice_num = 96;
	private Connection con = null;
	private Statement stmt = null;
	private static Lock lock = null;
	
	public boolean addTraffic(ArrayList<String> slice){
		if(slice.isEmpty() || slice == null){
			Common.logger.debug("slice empty or null!");
			return false;
		}
		lock.lock();
		try{
			queue_traffic.addAll(slice);
			Common.logger.debug("number of slice update record: " + queue_traffic.size());
		}
		finally{
			lock.unlock();
		}	
		return true;
		
	}
	
	TravelTimeSliceUpdater(final int update_thresold) throws SQLException{
		this.update_thresold = update_thresold;
		lock = new ReentrantLock();
		queue_traffic = new ArrayList<String>();
		
		con = Common.getConnection();
		con.setAutoCommit(false);
		try{
			stmt = con.createStatement();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		
		//create all slice table
		for(int i=1; i<= slice_num; i++){
			String slice_table = Common.traffic_slice_table + i + Common.Date_Suffix;
			try{
				String sql = "DROP TABLE IF EXISTS " + slice_table + ";";
				stmt.executeUpdate(sql);
				//create slice table
				sql = "CREATE TABLE " + slice_table + "(seq bigint, gid integer, next_gid integer,"
						+ " time double precision, percent double precision, interval double precision,"
						+ " tmstp bigint, suid bigint, utc bigint, start_pos double precision);";
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
		
		Timer timer = new Timer();
		timer.schedule(new TimerTask(){
			public void run(){
	            if(queue_traffic.size() > update_thresold){
	            	try{
	            		update();
	            	}
	            	catch (SQLException e) {
	        			e.printStackTrace();
	        		}
	            }       
	        }  
		}, 0, 20000);
	}
	
	private void update() throws SQLException{
		ArrayList<String> copy = new ArrayList<String>();
		lock.lock();
		try{			
			copy.addAll(queue_traffic);
			queue_traffic.clear();
		}
		finally{
			lock.unlock();
		}
		try{
			for(String i : copy){
				stmt.addBatch(i);
			}
			stmt.executeBatch();
			Common.logger.debug("traffic slice updater insert!");
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			Common.logger.error("update travel slice failed.");
		}
		finally{
			con.commit();
		}
		
	}
	public String toString(){
		String result = "";
		for(String record: queue_traffic){
			result = result + "\n" + record;
		}
		return result;
	}
}
