/** 
 * 2016年3月11日 
 * RemoveStopPoint.java 
 * author:ZhangYu
 */

package indi.zyu.realtraffic.process;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.old.MapMatching;

import java.sql.*;
import java.util.ArrayList;

public class RemoveStopPoint {
	
	//将定位无效的点筛去,有效记录存入match_table
	public static void create_matchtable(String database, String sample_table, String match_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		con = Common.getConnection();
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    
		    try{
	    		String sql="create TABLE "+match_table+" as select * from "+sample_table
	    				+" where ostdesc not like '%定位无效%';";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
			    sql="CREATE INDEX suid_idx ON " + match_table + " (suid);";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
			    sql="CREATE INDEX suid_utc_idx ON " + match_table + " (suid,utc);";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback();
			}
			finally{
				con.commit();
			}
		    
		    try{
	    		String sql="ALTER TABLE "+match_table+" add column stop integer;";
	    		System.out.println(sql);
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
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    Common.dropConnection(con);
		}
		//System.out.println("get_suids finished!");
	}
	
	//插入stop列值,将车的时间间隔以及距离间隔作为判断标准
	public static void label_stop( ArrayList<Sample> trajectory){
		
		double interval_threshold = 300.0;
		double distance_threshold = 50.0;
		double temp_distance_threshold = MapMatching.sigma_with_stop*2;
		
		try {
			
			String sql="";
			int start_pos=0;
			int cur_pos=0;
			double start_time=trajectory.get(start_pos).utc.getTime()/1000;
			double current_time=0;
			double distance=0;
			Sample cur_sample=null, pre_sample=null;
			int pre_start_pos=0;
			    
			for(cur_pos=1; cur_pos<trajectory.size(); cur_pos++){
				cur_sample=trajectory.get(cur_pos);
			    current_time=cur_sample.utc.getTime()/1000;
			    pre_start_pos=start_pos;
			    start_pos=cur_pos;
			    	
				//得到距当前点距离超过阈值的点作为起始点
			    //get the distance_threshold-based clustering
			    for(int j=cur_pos-1; j>=pre_start_pos; j--){
			    	pre_sample=trajectory.get(j);
		    		distance=0.0;
				    distance = Common.calculate_dist(pre_sample.lat,pre_sample.lon,
				    		cur_sample.lat, cur_sample.lon);

				    if(distance>distance_threshold){
				    	break;
				    }
				    start_pos=j;
			    }
			    //all points before pre_start_pos not exceed distance_threshold
			    /*if(start_pos == cur_pos){
			    	start_pos = pre_start_pos;
			    }*/
			    	
			    //test whether interval of cluster exceeds the interval_threshold;
			    start_time=trajectory.get(start_pos).utc.getTime()/1000;
			    if(current_time-start_time>interval_threshold){
			    	for(int j=cur_pos; j>=start_pos; j--){
				    	cur_sample=trajectory.get(j);
				    	if(cur_sample.stop != Sample.LONG_STOP){
					    	cur_sample.stop=Sample.LONG_STOP;
					    	trajectory.set(j, cur_sample);
				    	}
				    }
			    }
			}
		}	
		catch (Exception e) {
		    e.printStackTrace();
		}
		//System.out.println("label_stop finished!");
	}
}
