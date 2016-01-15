import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;


public class Stat_analyze {

	public static void sigtable_eval(String database, ArrayList<String> roadmap_tables, String obj_table, ArrayList<String> columns, 
			String condition, String group_con, String order_con, long start_utc, long interval){
		
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;
		
		DecimalFormat df=new DecimalFormat();
		df.setMaximumFractionDigits(6);

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			
			stmt = con.createStatement();
			int interval_count=roadmap_tables.size();
			//int max_interval=interval_count-1;
			long[] start_utcs=new long[interval_count];
			long[] end_utcs=new long[interval_count];
			//ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
			//AllocationRoadsegment[] static_roadlist=TripFinder.get_static_roadlist(database, static_roadmap);
			
			//For each interval calculate the traffic time for road segments
			/*for(int i=0; i<roadmap_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				AllocationRoadsegment[] roadlist = TripFinder.get_roadlist(database, roadmap_tables.get(i), time_col, traffic_col, false);
				System.out.println("Reading traffic data from "+roadmap_tables.get(i));
				time_list.add(roadlist);
			}*/
			
			//get the trips list in the given interval and do the evaluation
			for(int seq=0; seq<roadmap_tables.size(); seq++){
				start_utcs[seq]=start_utc+seq*interval;
				end_utcs[seq]=start_utcs[seq]+interval;
				try{
					
					//System.out.println("Evaluating trips from interval["+seq+"/ "+roadmap_tables.size()+"]...");
					
		    		spt = con.setSavepoint("svpt1");
		    		String cols=" ";
		    		for(int i=0; i<columns.size(); i++){
		    			cols+=columns.get(i);
		    			if(i!=columns.size()-1){
		    				cols+=", ";
		    			}
		    		}
		    		String sql="select "+cols+" from "+ obj_table +" where "+condition+" and start_utc>="+start_utcs[seq]+
							" and start_utc<"+end_utcs[seq]+ group_con+ order_con+"; ";
		    		/*String sql="select "+cols+" from "+ obj_table +" where "+condition+" and utc>="+start_utcs[seq]+
							" and utc<"+end_utcs[seq]+ group_con+ order_con+"; ";*/
		    		//System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
			    	while(rs.next()){
			    		cols=""+seq+", ";
			    		for(int i=0; i<columns.size(); i++){
			    			cols+=rs.getDouble(i+1);
			    			if(i!=columns.size()-1){
			    				cols+=", ";
			    			}
			    		}
			    		System.out.println(cols);
			    	}
			    	
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
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		DBconnector.dropConnection(con);
		//System.out.println("aggregate_time finished!");
	}
	
public static void multitable_eval(String database, ArrayList<String> roadmap_tables, ArrayList<String> columns,
		String union_table, String condition, String group_con, String order_con, long start_utc, long interval){
		
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;
		
		DecimalFormat df=new DecimalFormat();
		df.setMaximumFractionDigits(6);

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			
			stmt = con.createStatement();
			int interval_count=roadmap_tables.size();
			//int max_interval=interval_count-1;
			long[] start_utcs=new long[interval_count];
			long[] end_utcs=new long[interval_count];
			//ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
			//AllocationRoadsegment[] static_roadlist=TripFinder.get_static_roadlist(database, static_roadmap);
			
			//For each interval calculate the traffic time for road segments
			/*for(int i=0; i<roadmap_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				AllocationRoadsegment[] roadlist = TripFinder.get_roadlist(database, roadmap_tables.get(i), time_col, traffic_col, false);
				System.out.println("Reading traffic data from "+roadmap_tables.get(i));
				time_list.add(roadlist);
			}*/
			
			//get the trips list in the given interval and do the evaluation
			for(int seq=0; seq<roadmap_tables.size(); seq++){
				start_utcs[seq]=start_utc+seq*interval;
				end_utcs[seq]=start_utcs[seq]+interval;
				try{
					
					//System.out.println("Evaluating trips from interval["+seq+"/ "+roadmap_tables.size()+"]...");
					
		    		spt = con.setSavepoint("svpt1");
		    		String cols=" ";
		    		for(int i=0; i<columns.size(); i++){
		    			cols+=columns.get(i);
		    			if(i!=columns.size()-1){
		    				cols+=", ";
		    			}
		    		}
		    		String sql="select "+cols+" from "+ roadmap_tables.get(seq)+" as a, "+union_table+" where "+condition+ group_con + order_con+ "; ";
		    		//System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
			    	while(rs.next()){
			    		cols=""+seq+", ";
			    		for(int i=0; i<columns.size(); i++){
			    			cols+=rs.getDouble(i+1);
			    			if(i!=columns.size()-1){
			    				cols+=", ";
			    			}
			    		}
			    		System.out.println(cols);
			    	}
			    	
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
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		DBconnector.dropConnection(con);
		//System.out.println("aggregate_time finished!");
	}
	/*
	
select avg(eebksp_traffic-trsp_traffic), avg(eebksp_time), avg(trsp_traffic), avg(trsp_time) 
from ring5_traffictime40, ring5_roads_initspeed 
where ring5_traffictime40.gid = ring5_roads_initspeed.gid and ring5_traffictime40.class_id=108
and ring5_traffictime40.reference>0 and next_gid is not null
limit 100 


select ring5_traffictime40.gid, ring5_traffictime40.name, ring5_traffictime40.class_id, eebksp_traffic-trsp_traffic as traffic, (eebksp_traffic-trsp_traffic)/trsp_traffic as tr_percent
,(eebksp_time-trsp_time) as time, (eebksp_time-trsp_time)/trsp_time as t_percent

from ring5_traffictime40, oneway_matching 
where ring5_traffictime40.gid=oneway_matching.gid and
next_gid is null and oneway_matching.taxi_ratio>19
order by eebksp_traffic-trsp_traffic,ring5_traffictime40.class_id
	 */
	
	public static void main(String[] args){
		String database="mydb";
		String sample_table="labeled_gps";
		String roadmap_table="ring5_traffictime";
		String trip_table="ring5_trip";
		int min_sample_thres=2;
		String static_roadmap="ring5_roads_initspeed";
		String footprint_table="ring5_footprint_eebksp";
		String secondary_footprint="nontrip_footprint";
		
		double max_tempstop_precent=1;
		int min_trip_interval=1;

		/*
		String trip_extention="_";
		trip_extention+=(int)(max_tempstop_precent*10);
		trip_extention+="_"+min_trip_interval;
		trip_table+=trip_extention;*/
		
		//TripFinder.get_trips("mydb", sample_table, trip_table, min_sample_thres, min_trip_interval, max_tempstop_precent);
		
		long pre_time=0;
		long cur_time=0;
		Calendar cal =null;  
		
		//PathPlanner.make_time_table_routable(database, aggregated_time_table, roadmap_table);
		cal =Calendar.getInstance();
		cur_time=cal.getTime().getTime();
		pre_time=cur_time;
		
		
		int default_interval=240;
		String all_trip_table="ring5_all_trip";
		/*TripFinder.get_all_trips("mydb", sample_table, all_trip_table, min_sample_thres, min_trip_interval, default_interval, max_tempstop_precent);
		
		cal =Calendar.getInstance();
		cur_time=cal.getTime().getTime();
		System.out.print("TIME_ON_GET_ALL_TRIPS:"+(cur_time-pre_time));
		pre_time=cur_time;*/
		
		//for different period, use different Travel_time
		long cur_utc=1231172100L; //1231171200L is min(utc)
		long period=900L;
		int base_interval=2;
		
		int start_interval=3;
		cur_utc+=(start_interval-base_interval)*period;
		//ArrayList<String> time_tables=new ArrayList<String>();
		
		ArrayList<String> roadmap_tables=new ArrayList<String>();
		for(int i=start_interval; i<=54; i++){
			roadmap_tables.add(roadmap_table+""+i);
		}
		
		/*
		TripFinder.time_evaluation("mydb", roadmap_tables, roadmap_table, static_roadmap, all_trip_table, cur_utc, period, 1);*/
		
		String obj_table="ring5_all_trip";
		
		ArrayList<String> columns=new ArrayList<String> ();
		
		//eebksp_considerall_time_1
		//eebksp_considerall_traffic_1
		//eebksp_considerall_traffic_1_taxi
		
		//trsp_time_3 double precision,
		//trsp_traffic_3 double precision,
		//trsp_traffic_3_taxi double precision,
		
		//String time_col=amplifier+"_"+type+"_time";
		//String traffic_col=amplifier+"_"+type+"_traffic";
		obj_table="ring5_all_trip_1";
		columns.clear();
		columns.add("count(*)");
		columns.add("avg(end_utc-start_utc)");
		//columns.add("avg(actual_time_simulate)");
		//columns.add("avg(trsp_plan)");
		//columns.add("avg(eebksp_plan)");
		//columns.add("avg(eebksp_considerallplan)");
		columns.add("avg(trsp_simulate_pre)");
		columns.add("avg(eebksp_considerall_simulate_pre)");
		//columns.add("avg(trsp_simulate_90/eebksp_considerall_simulate_90)");
		//columns.add("avg(eebksp_considerall_simulate_90)/avg(trsp_simulate_90)");
		//columns.add("avg(rksp_simulate)");
		//columns.add("avg(actual_traffic_simulate)");
		String condition=" (valid is null or valid>=0 ) and (end_utc-start_utc)>300" +
				"and eebksp_considerall_simulate_pre < 1.5*(end_utc-start_utc) and trsp_simulate_pre<10*(end_utc-start_utc) ";
		String group_condition=" ";
		String order_condition=" ";
		
		//String b_second_ring="and b.x1>116.3404 and b.x1<116.4417 and b.y1<39.9504 and b.y1>39.8651 and b.x2>116.3404 and b.x2<116.4417 and b.y2<39.9504 and b.y2>39.8651 ";
		//String c_second_ring="and c.x1>116.3404 and c.x1<116.4417 and c.y1<39.9504 and c.y1>39.8651 and c.x2>116.3404 and c.x2<116.4417 and c.y2<39.9504 and c.y2>39.8651 ";
		
		//String b_second_ring="and b.x1>116.3026 and b.x1<116.4585 and b.y1<39.9692 and b.y1>39.8547 and b.x2>116.3026 and b.x2<116.4585 and b.y2<39.9692 and b.y2>39.8547 ";
		//String c_second_ring="and c.x1>116.3026 and c.x1<116.4585 and c.y1<39.9692 and c.y1>39.8547 and c.x2>116.3026 and c.x2<116.4585 and c.y2<39.9692 and c.y2>39.8547 ";
				
		String b_second_ring="and b.x1>116.3418 and b.x1<116.4585 and b.y1<39.9616 and b.y1>39.8655 and b.x2>116.3418 and b.x2<116.4585 and b.y2<39.9616 and b.y2>39.8655 ";
		String c_second_ring="and c.x1>116.3418 and c.x1<116.4585 and c.y1<39.9616 and c.y1>39.8655 and c.x2>116.3418 and c.x2<116.4585 and c.y2<39.9616 and c.y2>39.8655 ";
		
		condition=" (valid is null or valid>=0 ) and a.start_gid=b.gid and a.start_gid=c.gid "+b_second_ring + c_second_ring ;
		obj_table=" ring5_all_trip_1 as a, ring5_roads_initspeed as b, ring5_roads_initspeed as c ";
		
		//Stat_analyze.sigtable_eval(database, roadmap_tables, obj_table, columns, condition, group_condition, order_condition,cur_utc, period);
		System.out.println("\n");
		System.out.println("\n");
		
		obj_table="ring5_all_trip_3";
		columns.clear();
		columns.add("count(*)");
		//columns.add("avg(end_utc-start_utc)");
		//columns.add("avg(actual_time_simulate)");
		//columns.add("avg(trsp_plan)");
		//columns.add("avg(eebksp_plan)");
		//columns.add("avg(eebksp_considerallplan)");
		columns.add("avg(trsp_simulate_pre)");
		columns.add("avg(eebksp_considerall_simulate_pre)");
		//columns.add("avg(trsp_simulate_90/eebksp_considerall_simulate_90)");
		//columns.add("avg(eebksp_considerall_simulate_90)/avg(trsp_simulate_90)");
		//columns.add("avg(rksp_simulate)");
		//columns.add("avg(actual_traffic_simulate)");
		//condition=" (valid is null or valid>=0 ) and eebksp_considerall_simulate_pre < 2*(end_utc-start_utc) and 10*(end_utc-start_utc)>trsp_simulate_pre";
		group_condition=" ";
		order_condition=" ";
		//String b_second_ring="and b.x1>116.3404 and b.x1<116.4417 and b.y1<39.9504 and b.y1>39.8651 and b.x2>116.3404 and b.x2<116.4417 and b.y2<39.9504 and b.y2>39.8651 ";
		//String c_second_ring="and c.x1>116.3404 and c.x1<116.4417 and c.y1<39.9504 and c.y1>39.8651 and c.x2>116.3404 and c.x2<116.4417 and c.y2<39.9504 and c.y2>39.8651 ";
		
		condition=" (valid is null or valid>=0 ) and a.start_gid=b.gid and a.start_gid=c.gid "+b_second_ring + c_second_ring ;
		
		obj_table=" ring5_all_trip_3 as a, ring5_roads_initspeed as b, ring5_roads_initspeed as c ";
		
		//Stat_analyze.sigtable_eval(database, roadmap_tables, obj_table, columns, condition, group_condition, order_condition,cur_utc, period);
		System.out.println("\n");
		System.out.println("\n");
		
		obj_table="ring5_all_trip_6";
		columns.clear();
		columns.add("count(*)");
		//columns.add("avg(end_utc-start_utc)");
		//columns.add("avg(actual_time_simulate)");
		//columns.add("avg(trsp_plan)");
		//columns.add("avg(eebksp_plan)");
		//columns.add("avg(eebksp_considerallplan)");
		columns.add("avg(trsp_simulate_pre)");
		columns.add("avg(eebksp_considerall_simulate_pre)");
		//columns.add("avg(trsp_simulate_90/eebksp_considerall_simulate_90)");
		//columns.add("avg(eebksp_considerall_simulate_90)/avg(trsp_simulate_90)");
		//columns.add("avg(rksp_simulate)");
		//columns.add("avg(actual_traffic_simulate)");
		//condition=" (valid is null or valid>=0 ) and eebksp_considerall_simulate_pre < 2*(end_utc-start_utc) and 10*(end_utc-start_utc)>trsp_simulate_pre";
		group_condition=" ";
		order_condition=" ";
		
		//String b_second_ring="and b.x1>116.3404 and b.x1<116.4417 and b.y1<39.9504 and b.y1>39.8651 and b.x2>116.3404 and b.x2<116.4417 and b.y2<39.9504 and b.y2>39.8651 ";
		//String c_second_ring="and c.x1>116.3404 and c.x1<116.4417 and c.y1<39.9504 and c.y1>39.8651 and c.x2>116.3404 and c.x2<116.4417 and c.y2<39.9504 and c.y2>39.8651 ";
		
		condition=" (valid is null or valid>=0 ) and a.start_gid=b.gid and a.start_gid=c.gid "+b_second_ring + c_second_ring;
		
		obj_table=" ring5_all_trip_6 as a, ring5_roads_initspeed as b, ring5_roads_initspeed as c ";
		//Stat_analyze.sigtable_eval(database, roadmap_tables, obj_table, columns, condition, group_condition, order_condition,cur_utc, period);
		System.out.println("\n");
		System.out.println("\n");
		
		/*String time_col0="trsp_time_pre";//
		String traffic_col0="trsp_traffic_pre";//
		String taxi_traffic_col0="trsp_traffic_pre_taxi";//*/
		
		String time_col0="eebksp_considerall_time_1";//
		String traffic_col0="eebksp_considerall_traffic_1";//
		String taxi_traffic_col0="eebksp_considerall_traffic_1_taxi";//
		
		String time_col1="eebksp_considerall_time_3";//
		String traffic_col1="eebksp_considerall_traffic_3";//
		String taxi_traffic_col1="eebksp_considerall_traffic_3_taxi";//
		
		String time_col2="eebksp_considerall_time_6";//
		String traffic_col2="eebksp_considerall_traffic_6";//
		String taxi_traffic_col2="eebksp_considerall_traffic_6_taxi";//
		
		String time_col3="trsp_time_1";//
		String traffic_col3="trsp_traffic_1";//
		String taxi_traffic_col3="trsp_traffic_1_taxi";//
		
		String time_col4="trsp_time_3";//
		String traffic_col4="trsp_traffic_3";//
		String taxi_traffic_col4="trsp_traffic_3_taxi";//
		
		String time_col5="trsp_time_6";//
		String traffic_col5="trsp_traffic_6";//
		String taxi_traffic_col5="trsp_traffic_6_taxi";//

		//eebksp_considerall_time_1
		//eebksp_considerall_traffic_1
		//eebksp_considerall_traffic_1_taxi
		
		//trsp_time_3 double precision,
		//trsp_traffic_3 double precision,
		//trsp_traffic_3_taxi double precision,
		
		//String time_col=amplifier+"_"+type+"_time";
		//String traffic_col=amplifier+"_"+type+"_traffic";
		
		/*
		String time_col2="eebksp_time_all_pre";//
		String traffic_col2="eebkrsp_traffic_all_pre";//
		String taxi_traffic_col2="eebkrsp_traffic_all_pre_taxi";//*/
		
		
		columns.clear();
		columns.add("count(*)");
		//columns.add("avg(capacity)/4");
		//columns.add("avg("+taxi_traffic_col3+")");//traffic_col
		//columns.add("avg("+taxi_traffic_col0+")");//traffic_col

		//columns.add("avg("+taxi_traffic_col4+")");//traffic_col
		//columns.add("avg("+taxi_traffic_col1+")");//traffic_col

		//columns.add("avg("+taxi_traffic_col5+")");//traffic_col
		//columns.add("avg("+taxi_traffic_col2+")");//traffic_col
		
		
		//columns.add("avg("+traffic_col3+")");//traffic_col
		//columns.add("avg("+traffic_col0+")");//traffic_col

		//columns.add("avg("+traffic_col4+")");//traffic_col
		//columns.add("avg("+traffic_col1+")");//traffic_col

		//columns.add("avg("+traffic_col5+")");//traffic_col
		//columns.add("avg("+traffic_col2+")");//traffic_col
		
		//columns.add("avg("+taxi_traffic_col0+")");//traffic_col
		//columns.add("avg("+taxi_traffic_col0+")");//traffic_col
		//columns.add("avg("+taxi_traffic_col0+")");//traffic_col
		//columns.add("avg("+taxi_traffic_col0+")");//traffic_col
		//columns.add("avg("+traffic_col1+")");//traffic_col
		//columns.add("avg("+traffic_col2+")");//traffic_col
		//columns.add("avg("+taxi_traffic_col2+")");//traffic_col
		//columns.add("avg("+traffic_col3+")");//traffic_col
		
		//columns.add("avg("+traffic_col0+")/avg("+traffic_col1+")");//time_col
		//columns.add("avg("+traffic_col2+")/avg("+traffic_col3+")");//time_col
		//columns.add("avg("+traffic_col2+")/avg("+traffic_col4+")");//time_col
		//columns.add("(avg("+taxi_traffic_col2+")-avg("+taxi_traffic_col0+"))/avg("+taxi_traffic_col0+")");//time_col
		//columns.add("(avg("+traffic_col2+")-avg("+traffic_col0+")*0.4)/avg("+traffic_col2+")");//time_col
		
		//columns.add("avg("+time_col0+")");//time_col
		//columns.add("avg("+time_col1+")");//time_col
		//columns.add("avg("+time_col2+")");//time_col
		//columns.add("avg("+time_col3+")");//time_col
		/*columns.add("avg("+time_col3+")");//time_col
		columns.add("avg("+time_col4+")");//time_col*/
		
		//columns.add("avg("+traffic_col0+"*"+time_col0+")");//time_col
		//columns.add("avg("+traffic_col1+"*"+time_col1+")");//time_col
		//columns.add("avg("+traffic_col2+"*"+time_col2+")");//time_col
		//columns.add("avg("+traffic_col3+"*"+time_col3+")");//time_col
		
		
		//columns.add("avg("+traffic_col0+")/avg("+traffic_col3+")");//time_col
		//columns.add("avg("+traffic_col1+")/avg("+traffic_col4+")");//time_col
		//columns.add("avg("+traffic_col2+")/avg("+traffic_col5+")");//time_col
		
		//columns.add("avg("+time_col0+")/avg("+time_col3+")");//time_col
		//columns.add("avg("+time_col1+")/avg("+time_col4+")");//time_col
		//columns.add("avg("+time_col2+")/avg("+time_col5+")");//time_col
		
		columns.add("avg("+time_col0+"*"+traffic_col0+")");//time_col
		columns.add("avg("+time_col3+"*"+traffic_col3+")");//time_col
		columns.add("avg("+time_col1+"*"+traffic_col1+")");//time_col
		columns.add("avg("+time_col4+"*"+traffic_col4+")");//time_col
		columns.add("avg("+time_col2+"*"+traffic_col2+")");//time_col
		columns.add("avg("+time_col5+"*"+traffic_col5+")");//time_col
		
		//columns.add("avg("+traffic_col1+"*"+time_col1+")/avg("+traffic_col3+"*"+time_col3+")");//time_col
		
		String second_ring="and x1>116.3404 and x1<116.4417 and y1<39.9504 and y1>39.8651 and x2>116.3404 and x2<116.4417 and y2<39.9504 and y2>39.8651 ";
		condition=" a.reference>10 and a.gid=b.gid and next_gid is null " +
				"and ("+taxi_traffic_col5+">0 " +"or "+taxi_traffic_col4+">0) "+second_ring;
		String union_table="ring5_roads_initspeed as b";
		group_condition="";
		order_condition="";
		Stat_analyze.multitable_eval(database, roadmap_tables, columns, union_table, condition, group_condition, order_condition, cur_utc, period )	;
		
	}
}
