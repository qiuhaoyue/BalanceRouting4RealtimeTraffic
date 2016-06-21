package indi.zyu.realtraffic.traffic;

import indi.zyu.realtraffic.common.Common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Timer;
import java.util.TimerTask;

/** 
 * 2016Äê5ÔÂ5ÈÕ 
 * TravelTimeAggregate.java 
 * author:ZhangYu
 */

//timly aggregate traffic slice and generate travel time of each road.
public class TravelTimeAggregate {
	
	private Connection con = null;
	private Statement stmt = null;
	private int interval = 0;//by minute
	private int seq = 1;
	
	TravelTimeAggregate(int interval){
		this.interval = interval;
		con = Common.getConnection();
		try{
			stmt = con.createStatement();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		//start timer to aggregate traffic every certain time
		Timer timer = new Timer();
		timer.schedule(new TimerTask(){
			public void run(){
				aggregate_time(Common.traffic_slice_table + seq + Common.Date_Suffix, Common.traffic_total_table + seq);
				Common.update_roadlist(Common.traffic_total_table + seq, true);//update road speed
				seq++;
	        }  
		}, 15 * 60 * 1000, interval * 60 * 1000);
	}
	
	private void aggregate_time(String allocation_table, String aggregation_table){
		ResultSet rs = null;
		Savepoint spt=null;
		//String roadmap_table = Common.OneWayTable;
		String roadmap_table = Common.OriginWayTable;
		try{
			
			try{						
				spt = con.setSavepoint("svpt1");
			    String sql="DROP TABLE IF EXISTS "+ aggregation_table +";";
			    Common.logger.debug(sql);
			    stmt.executeUpdate(sql);
				//Statement tmp_stmt = con.createStatement();
				//int i = tmp_stmt.executeUpdate(sql);
			}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			//insert the turning_specific travel time
			try{
	    		spt = con.setSavepoint("svpt1");
	    		
	    		String sql="\n create table "+ aggregation_table +" as ( select temp2.gid, temp2.next_gid, temp2.count as reference, temp2.time, " +
	    				"roadmap.length/temp2.time*1000 as average_speed, roadmap.class_id, roadmap.name, roadmap.length, roadmap.to_cost, roadmap.reverse_cost \n"+
	    				"from ( select temp.gid, temp.next_gid, temp.count, temp.weight_time/temp.weight as time from ( \n"+
	    				"select gid, next_gid, count(*) as count, sum(time*interval) as weight_time, sum(interval*percent) as weight \n"+
	    				"from "+ allocation_table +" group by gid, next_gid) as temp \n" +
	    				"where temp.weight!=0 and count>2 order by gid, next_gid) as temp2, "+roadmap_table+" as roadmap where temp2.gid=roadmap.gid \n"+
	    				"order by roadmap.class_id,gid,next_gid );";
	    		Common.logger.debug(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			//delete those having no specific turnings
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="delete from "+ aggregation_table + " where next_gid is null;";
	    		Common.logger.debug(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			//use the average of all samples as default value
			try{
	    		spt = con.setSavepoint("svpt1");
	    		
	    		String sql="\n insert into "+ aggregation_table +" (gid, reference, time, average_speed, class_id, name, length, to_cost, reverse_cost ) \n" +
	    				"( select temp2.gid, temp2.count as reference, temp2.time, " +
	    				"roadmap.length/temp2.time*1000 as average_speed, roadmap.class_id, roadmap.name, roadmap.length, roadmap.to_cost, roadmap.reverse_cost \n"+
	    				"from ( select temp.gid, temp.count, temp.weight_time/temp.weight as time from ( \n"+
	    				"select gid, count(*) as count, sum(time*interval) as weight_time, sum(interval*percent) as weight \n"+
	    				"from "+ allocation_table +" group by gid) as temp \n" +
	    				"where temp.weight!=0 and count>2 order by gid) as temp2, "+roadmap_table+" as roadmap where temp2.gid=roadmap.gid \n"+
	    				");";
	    		
	    		//System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			//use the default value of other same-level road of generate the default value of the road segment whose travel time is missing.
			try{
	    		spt = con.setSavepoint("svpt1");
	    		/*
	    		insert into ring2_time_rush (gid, reference, average_speed, class_id, name, length, to_cost, reverse_cost ) 
	    		(select gid, 0, temp.speed, temp.class_id, name, length, to_cost, reverse_cost 
	    		   from ring2_roads, (select avg(average_speed) as speed, class_id from ring2_time_rush where next_gid is null and reference >0 group by class_id) as temp 
	    		   where (ring2_roads.gid not in (select gid from ring2_time_rush where next_gid is null and reference >0)) and ring2_roads.class_id=temp.class_id
	    		)*/
	    		String sql="\n insert into "+ aggregation_table +" (gid, reference, average_speed, class_id, name, length, to_cost, reverse_cost ) \n" +
	    				"(select gid, 0, temp.speed, temp.class_id, name, length, to_cost, reverse_cost \n" +
	    				"from "+roadmap_table+", (select avg(average_speed) as speed, class_id from "+aggregation_table+" where next_gid is null and reference >0 group by class_id ) as temp \n"+
	    				"where ("+roadmap_table+".gid not in (select gid from "+aggregation_table+" where next_gid is null and reference >0)) and "+roadmap_table+".class_id=temp.class_id \n"+ ");";
	    			    		
	    		//System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
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
		//Common.dropConnection(con);
		Common.logger.debug("aggregate_time finished!: " + seq);
	}
}
