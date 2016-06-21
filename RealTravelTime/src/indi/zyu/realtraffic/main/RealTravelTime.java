package indi.zyu.realtraffic.main;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.process.ProcessThread;
import indi.zyu.realtraffic.process.TaxiInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.esri.core.geometry.Point;

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
		
		Common.logger.debug("start!");
		SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Common.Date_Suffix = (new SimpleDateFormat("_yyyy_MM_dd")).format(new java.util.Date());
		Common.init(40000, 10);
		
		//clear previous travel table 
		Common.clear_travel_table("_2016_06_18");

    	String datetime = tempDate.format(new java.util.Date());
    	Common.logger.debug("-----Real travel time process start:	"+datetime+"-------!");
    	Common.logger.debug("-----get road info:	-------!");
    	
    	//Common.change_scheme_roadmap(Common.OriginWayTable);//add some column
    	Common.init_roadlist2();//initialize roadlist
    	//Common.update_roadlist("allroad_time_nonrush_1", true);//initialize road speed
    	Common.logger.debug("-----start simulate gps point:	-------!");
    	
    	//ProcessGPS processor = new ProcessGPS(15, 3, 5, 40000, 90);
    	//ProcessGPS processor = new ProcessGPS(15, 3, 5, 40000, 10);
    	Connection con = null;
    	Statement stmt = null;
		ResultSet rs = null;
		con = Common.getConnection();
		if (con == null) {
			Common.logger.error("Failed to make connection!");
			return;
		}
		
		try {
			stmt = con.createStatement();
			String sql = "select * from " + Common.ValidSampleTable + ";"; //+ " limit 5000;";
			//String sql = "select * from " + Common.SingleSampleTable + " limit 2000";
			Common.logger.debug(sql);
    		rs = stmt.executeQuery(sql);
    		int counter = 0;//to control generate rate
    		
    		//start aggregate timer;
    		//TravelTimeAggregate aggregater = new TravelTimeAggregate(15);
    		Common.logger.debug("select finished");
    		//start process gps point
    		while(rs.next()){
    			counter++;
    			Sample gps = new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getLong("lat"), 
    		    		rs.getLong("lon"), (int)rs.getLong("head"), rs.getLong("speed"), rs.getLong("distance"));
    			
    			int suid = (int) gps.suid;
    			if(Common.taxi[suid] == null){
    				Common.taxi[suid] = new TaxiInfo();	
    			}
    			//Common.taxi[suid].add_gps(gps);
    			//preprocess
    			/*if(Common.taxi[suid].preprocess(gps)){
    				Common.fixedThreadPool.execute(new ProcessThread(suid, gps));
    			}*/
    			Common.ThreadPool.execute(new ProcessThread(suid, gps));
    			
    			if(counter % 1000 == 0){
    				Thread.sleep(20*1000);
    				Common.logger.debug("Date: " + gps.utc);
    				//Common.logger.debug("gps number: " + counter);
    			}
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
			//processor.clear();
		}
		//wait until all thread has finished
		while(true){
			if (Common.ThreadPool.isTerminated()){
				break;
			}
			/*if (processor.cachedThreadPool.isTerminated()){
				break;
			}*/
			Thread.sleep(10000);
		}
		
		datetime = tempDate.format(new java.util.Date());
		/*Common.logger.debug(Common.taxi[5434].matched_count + "/" + Common.taxi[5434].total_count);
		Common.logger.debug("real match gid_list: " + Common.taxi[5434].gid_list.toString());
		Common.logger.debug("size: " + Common.taxi[5434].gid_list.size());
		List<MatcherCandidate> sequence = Common.taxi[5434].state.sequence();
		Common.logger.debug("sequence: size" + sequence.size());
		String str = "";
		for(int i=0; i< sequence.size(); i++){
			MatcherCandidate estimate = sequence.get(i);
			int id = (int)estimate.point().edge().id();
			str += id + ",";
		}
		Common.logger.debug(str);*/
		//Common.taxi[5434].state.sequence()
    	Common.logger.debug("-----Real travel time process finished:	"+datetime+"-------!");
    	/*Common.logger.debug("match time: " + Common.match_time 
				+ ";estimate road time " + Common.estimate_road_time + ",counter " 
				+ Common.estimate_road_counter + ";turing time " + Common.estimate_turning_time
				+ ",counter " + Common.estimate_turning_counter);	*/
	}
}
