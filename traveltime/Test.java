import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;
import java.lang.Math;
 

public class Test {
	
	public static final long max_utc=1231257600L;
	public static final long min_utc=1231171200L;
	
	//将partitions表中的分表合并至merged_table
	public static void create_labeled_gps(String database, ArrayList<String> partitions, String merged_table){
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ merged_table +";";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			if(partitions!=null && partitions.size()>0){
				String part_table=partitions.get(0);
				try{
		    		spt = con.setSavepoint("svpt2");
		    		String sql="create table "+ merged_table +" as select * from "+part_table+";";
		    		System.out.println(sql);
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					con.commit();
				}
				
				for(int i=1;i<partitions.size();i++){
					part_table=partitions.get(i);
					try{
			    		spt = con.setSavepoint("svpt3");
			    		String sql="insert into "+ merged_table +" select * from "+part_table+";";
			    		System.out.println(sql);
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
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+merged_table+"_suidutc_idx ON "+ merged_table +"(suid, utc);";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+merged_table+"_suid_idx ON "+ merged_table +"(suid);";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+merged_table+"_utc_idx ON "+ merged_table +"(utc);";
	    		System.out.println(sql);
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
	}
	
	//从merged_table中筛选出符合位置范围的点,存入matching_table
	public static void filter_samples(String database, String merged_table, String matching_table, long start_utc, long end_utc, long min_x, long max_x, long min_y, long max_y){
		
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ matching_table +";";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
		    	spt = con.setSavepoint("svpt2");
		    	String sql="create table "+ matching_table +" as select * from "+ merged_table +" where utc>="+start_utc+" and utc<"+end_utc
		    			+" and lon>="+min_x+" and lon<"+max_x+" and lat>="+min_y+" and lat<"+max_y+";";
		    	System.out.println(sql);
		    	stmt.executeUpdate(sql);
		    }
		    catch (SQLException e) {
				e.printStackTrace();
				con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="alter table "+matching_table+" add column gid integer;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="alter table "+matching_table+" add column edge_offset double precision;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="alter table "+matching_table+" add column route text;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+matching_table+"_suidutc_idx ON "+ matching_table +"(suid, utc);";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+matching_table+"_suid_idx ON "+ matching_table +"(suid);";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+matching_table+"_utc_idx ON "+ matching_table +"(utc);";
	    		System.out.println(sql);
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
	}
	
	public static void get_oneway_map(String database, String dualway_map, String oneway_map){
		
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ oneway_map +";";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
		    	spt = con.setSavepoint("svpt2");
		    	String sql="create table "+ oneway_map +" as select * from "+ dualway_map +";";
		    	System.out.println(sql);
		    	stmt.executeUpdate(sql);
		    }
		    catch (SQLException e) {
				e.printStackTrace();
				con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt3");
	    		String sql="CREATE INDEX "+oneway_map+"_gid_idx ON "+ oneway_map +"(gid);";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+oneway_map+"_geom_idx ON "+ oneway_map +"(the_geom);";
	    		System.out.println(sql);
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
		
		RoadCostUpdater.updateWeight(database, oneway_map);
		
		SpliteDualWay.splitways(database, oneway_map);
	}

	//intersection_update("mydb", "oneway_test", "nodes", "intersection_test");
public static void get_oneway_intersections(String database, String oneway_map, String node_table, 
		String intersection_table, String oneway_intersection){
		
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ oneway_intersection +";";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			finally{
				con.commit();
			}
			
			try{
		    	spt = con.setSavepoint("svpt2");
		    	String sql="create table "+ oneway_intersection +" as select * from "+ intersection_table +";";
		    	System.out.println(sql);
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
		
		Intersection.intersection_update(database, oneway_map, node_table, oneway_intersection);
	}
	
	public static void main(String[] args){
		
		//20090106 1:00pm-2:00pm 1231218000-1231221600  522743() samples
		//20090106 5:00-6:00pm 1231232400-1231236000 622989() samples
		
		//Inside 2nd Ring Road, 116.3399 --- 116.4422  39.9505--39.8648
		/*
		long utc=((long)1231236000)*1000;
		System.out.println("TIME second:"+ utc);
		Date date=new Date(utc);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		TimeZone zone=TimeZone.getTimeZone("GMT+8");
		format.setTimeZone(zone);
		System.out.println("TIME:"+format.format(date));
		
		String s="12,14,";
		String[] route_gids=s.split(",");
		for(int i=0;i<route_gids.length;i++){
			System.out.println("|"+route_gids[i]+"|");
		}
		System.out.println("|finished!|");*/
		
		String merged_name="lalala";
		long min_utc=1270483200;
		long max_utc=1270569600;
		if(args.length>0){
			merged_name = args[0];
			min_utc=Long.parseLong(args[1]);
			max_utc=Long.parseLong(args[2]);
		}
		String database="routing";
		String matching_table="labeled_"+merged_name;
		
		ArrayList<String> taxi_list=new ArrayList<String>();
		taxi_list.clear();
		
		String oneway_table="oneway_matching";
		String dualway_table="ways";
		
		String oneway_intersection="oneway_intersection";
		String intersection_table="intersections";
		String node_table="nodes";
		
		String oneway_mining="ring5_miningroads"+merged_name;
		String oneway_initialspeed="ring5_roads_initspeed"+merged_name;
		
		//merge the partitions that have finished stop_labeling and create indexes;
		/*ArrayList<String> partitions=new ArrayList<String>();
		String table_root="gps_part_";
		String merged_name="labeled_gps";
		for(int i=1;i<=11;i++){
			String table_name=table_root+i;
			partitions.add(table_name);
		}
		Test.create_labeled_gps(database, partitions, merged_name);*/
		
		//Filter the sample according to time and area constrains.
		long start_utc=min_utc; 
		long end_utc=max_utc;
		
		//2nd_ring area;
		/*long min_x=11633990L;
		long max_x=11644220L;
		long min_y=3986480L;
		long max_y=3995050L;*/
		
		//all area
		long min_x=11618690L;
		long max_x=11656400L;
		long min_y=3974050L;
		long max_y=4003970L;
		
		//String matching_table="ring2_samples";
		SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    String datetime = tempDate.format(new java.util.Date());
	    System.out.println("-----------filter_samples process start:	"+datetime+"------------!!!!!!");
	    
		//Test.filter_samples(database, merged_name, matching_table, start_utc, end_utc, min_x, max_x, min_y, max_y);
		
		//prepare the one-way roadmap_table for map-matching(update weight and transform to oneway);
		/*String oneway_table="oneway_matching";
		String dualway_table="ways";
		Test.get_oneway_map(database, dualway_table, oneway_table);*/
		
		
		//modify the intersections according to oneway_table;
		//String oneway_table="oneway_matching";
		/*String oneway_intersection="oneway_intersection";
		String intersection_table="intersections";
		String node_table="nodes";
		Test.get_oneway_intersections(database, oneway_table, node_table, intersection_table, oneway_intersection);*/
		
		//map-matching process
		//String oneway_table="oneway_matching";
		//String oneway_intersection="oneway_intersection";
		
	    /*
		datetime = tempDate.format(new java.util.Date());
	    System.out.println("Mapmatching process start:	"+datetime+"!!!!!!");
		taxi_list.clear();
		DBconnector.get_suids(database, matching_table, taxi_list);
		for(int i=0; i<taxi_list.size();i++){
			System.out.print((i+1)+"/"+taxi_list.size()+"	");
			DBconnector.mapMatching(database, taxi_list.get(i), matching_table, oneway_table, oneway_intersection);
		}*/
		
		//travel_time_allocation && aggregation
		datetime = tempDate.format(new java.util.Date());
	    System.out.println("------------Prepare for travel_time_allocation process start:	"+datetime+"!!!!!!------------------");
		
	    /*
		TravelTimeAllocation.filter_roads(database, oneway_table, oneway_mining, min_x, max_x, min_y, max_y);
		TravelTimeAllocation.get_suids(database, matching_table, taxi_list);
		TravelTimeAllocation.preprocess_intervals(database, matching_table, taxi_list);*/
		
		//get the initial speed and reference_count statistics from map-matching result
		//ArrayList<String> gid_list=new ArrayList<String>();
		/*RoadPassStat.change_scheme(database, oneway_mining);
		RoadPassStat.create_index_on_samples(database, matching_table);*/
		/*RoadPassStat.passStat(database, matching_table, oneway_mining);
		HashMap<Integer, RoadClass> hmp_class2speed= new HashMap<Integer, RoadClass>();
		RoadPassStat.get_class2speed(database, oneway_mining, hmp_class2speed);
		RoadPassStat.improve_speed(database, oneway_mining, oneway_initialspeed, hmp_class2speed);
		*/
		//for different period, use different Travel_time
		
		datetime = tempDate.format(new java.util.Date());
	    System.out.println("------------travel_time_allocation process start:	"+datetime+"!!!!!!----------------");
	    
		AllocationRoadsegment[] roadlist=TravelTimeAllocation.get_roadlist(database, oneway_initialspeed, false);
		long period=900L;
		long cur_utc=start_utc;
		long period_end_utc=cur_utc+period;
		long max_seg=(end_utc-cur_utc)/period;
		long seq_num=0;

		//ArrayList<String> taxi_list=new ArrayList<String>();
		String allocation_table = merged_name+"_allocation_";
		String time_table = merged_name+"ring5_time_";
		while(period_end_utc <= end_utc){
			seq_num=max_seg-(end_utc-period_end_utc)/period;
			//if(seq_num>=48){
				TravelTimeAllocation.allocate_time(database, roadlist, matching_table, allocation_table+seq_num, taxi_list, cur_utc, period_end_utc);
				TravelTimeAllocation.aggregate_time(database, oneway_initialspeed, allocation_table+seq_num, time_table+seq_num, cur_utc, period_end_utc);
				//roadlist=TravelTimeAllocation.get_roadlist(database, time_table+seq_num, true);
			//}
			cur_utc=period_end_utc;
			period_end_utc=cur_utc+period;
			datetime = tempDate.format(new java.util.Date());
			System.out.println("---------["+seq_num+"/"+max_seg+"] intervals finished at:	"+datetime+"!!!!----------");
		}
		
		System.out.println("Test Finished !!!");
	}
}
