package indi.zyu.realtraffic.main;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.process.TaxiInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

/** 
 * 2016Äê3ÔÂ11ÈÕ 
 * TravelTime.java 
 * author:ZhangYu
 */

public class RealTravelTime {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws SQLException, InterruptedException {
		Connection con = null;
    	Statement stmt = null;
		ResultSet rs = null;
		
		Common.logger.debug("start!");
		SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
		//Common.Date_Suffix = (new SimpleDateFormat("_yyyy_MM_dd")).format(new java.util.Date());
		Common.Date_Suffix ="_2010_04_06";
		
		try{
			con = Common.getConnection();
			if (con == null) {
				Common.logger.error("Failed to make connection!");
				return;
			}
			stmt = con.createStatement();
			String sql = "select min(utc),max(utc) from " + Common.ValidSampleTable + ";";
			rs = stmt.executeQuery(sql);
			if(rs.next()){
				Common.start_utc = rs.getLong(1);
				Common.end_utc = rs.getLong(2);
			}
			
			Common.logger.debug("start utc: " + Common.start_utc + "; end utc: " + Common.end_utc);
		}
		catch (SQLException e) {
		    e.printStackTrace();
		    con.rollback();
		}
		
		Common.init(40000);
		//clear previous travel table 
		//Common.clear_travel_table("_2016_08_19");
		

    	String start_time = tempDate.format(new java.util.Date());
    	Common.logger.debug("-----Real travel time process start:	"+start_time+"-------!");
    	Common.logger.debug("-----get road info:	-------!");
    	
    	//Common.change_scheme_roadmap(Common.OriginWayTable);//add some column
    	Common.init_roadlist();//initialize roadlist

    	Common.logger.debug("-----start simulate gps point:	-------!");
    	
		int counter = Common.emission_step * Common.emission_multiple;//to control generate rate
		try {
					
			String sql = "select * from " + Common.ValidSampleTable + " order by utc;"; //+ " limit 5000;";
			//String sql = "select * from " + Common.SingleSampleTable + " limit 2000";
			Common.logger.debug(sql);
    		rs = stmt.executeQuery(sql);
    		
    		Common.logger.debug("select finished");
    		
    		//start process gps point
    		while(rs.next()){
    			//counter += 5;
    			long utc = rs.getLong("utc");
    			//to control data emission speed
    			long interval = utc - Common.start_utc;
    			
    			//wait until utc of gps is in the time range
    			while(interval >= counter){
					counter += Common.emission_step * Common.emission_multiple;
    				Thread.sleep(Common.emission_step * 1000);
    				//Common.logger.debug("Date: " + gps.utc);
    				Common.logger.debug("time: " + counter);
				}
    			//utc of gps is in the time range, process the point
    			Sample gps = new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getLong("lat"), 
    		    		rs.getLong("lon"), (int)rs.getLong("head"), rs.getLong("speed"), rs.getLong("distance"));
    			
    			int suid = (int) gps.suid;
    			if(Common.taxi[suid] == null){
    				Common.taxi[suid] = new TaxiInfo();
    				int number = suid % Common.thread_number;
    				Common.thread_pool[number].put_suid(suid);
    			}
    			Common.taxi[suid].add_gps(gps);
    			
    		}
    		//wait for all gps point processed
    		Thread.sleep(100 * 1000);
    		
    		//flush all traffic to simulate a new day
    		for(int i=0;i<Common.roadlist.length;i++){
    		    Common.roadlist[i].flush();
    		}
		}
		catch (SQLException e) {
		    e.printStackTrace();
		    Common.logger.error("generate gps point error!");
		    con.rollback();
		}
		/*catch (InterruptedException e) {
		    e.printStackTrace();
		    con.rollback();
		}*/
		finally{
			con.commit();
		}
		//wait until all thread has finished
		/*while(true){
			if (Common.thread_counter.get() == 0){
				break;
			}
			Thread.sleep(10000);
		}*/
		
		String end_time = tempDate.format(new java.util.Date());

    	Common.logger.debug("-----Real travel time process finished:	"+end_time+"-------!");
    	Common.logger.debug("process time: " + start_time + " - " + end_time + "counter: " + counter);

    	//Common.logger.debug("same road estimation: " + Common.same_road_count.get());
    	//Common.logger.debug("diff road estimation: " + Common.diff_road_count.get());
	}
	
}
