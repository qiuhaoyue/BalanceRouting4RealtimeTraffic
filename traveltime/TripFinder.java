import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.TimeZone;
import java.lang.Math;
import java.util.HashMap;
import java.text.DecimalFormat;
 
public class TripFinder {
	
	public static void dropConnection(Connection con){
		try{
			if(con!=null){
				con.close();
			}
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
	}
	
	public static Connection getConnection(String database, String username, String pwd){
		Connection con=null;
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			e.printStackTrace();
			return null;
		}
		//System.out.println("PostgreSQL JDBC Driver Registered!");
		try {
			con = DriverManager.getConnection(database, username, pwd);
			con.setAutoCommit(false);
			
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}	
		return con;
	}
	
	public static void get_suids(String database, String sample_table, ArrayList<String> taxi_list, int min_sample_thres){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    String sql="select suid, count(*) from " + sample_table +" where ostdesc like '%重车%' group by suid order by count desc; ";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    if(taxi_list==null){
		    	return;
		    }
		    taxi_list.clear();
		    while(rs.next()){
		    	if(rs.getLong("count")<min_sample_thres){
		    		break;
		    	}
		    	taxi_list.add(rs.getString("suid"));
		    }
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    DBconnector.dropConnection(con);
		}
		//System.out.println("get_suids finished!");
	}
	
	public static void get_trips(String database, String sample_table, String trips_table, int min_sample_thres, int min_trip_interval, double max_tempstop_percent){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;
		
		DecimalFormat df=new DecimalFormat();
		df.setMaximumFractionDigits(6);
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
		}
		
		try {
			stmt = con.createStatement();
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ trips_table +";" ;
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
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="create table "+ trips_table +"(suid bigint, start_gid integer, start_pos double precision, start_utc bigint," +
	    				" end_gid integer, end_pos double precision, end_utc bigint, route text, valid integer);" ;
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
			
			//import samples from database;
		    String sql="select * from "+sample_table+" where suid in " +
		    		"(select suid from (select suid, count(*) from "+sample_table+" where ostdesc like '%重车%' group by suid ) as temp " +
		    				"where temp.count>= "+min_sample_thres+") order by suid, utc;";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);

		    ArrayList<Sample> samplelist= new ArrayList<Sample>();
			while(rs.next()){
			    Sample cur_sample=new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getInt("gid"), rs.getDouble("edge_offset"),
		    			rs.getString("route"), rs.getInt("stop"), Math.round(rs.getDouble("interval")));
		    	cur_sample.pre_gid=rs.getInt("pre_gid");
		    	cur_sample.pre_offset=rs.getDouble("pre_offset");
		    	String ostdesc=rs.getString("ostdesc");
		    	cur_sample.passenager=0;
		    	if(ostdesc.contains("重车")){
		    		cur_sample.passenager=1;
		    	}
		    	samplelist.add(cur_sample);
			}
			
			Sample cur_sample=null;
			Sample start_sample=null;
			Sample pre_sample=null;
			int trip_start=-1;
			boolean has_passenager=false;
			long cur_suid=-1;
			for(int j=0;j<samplelist.size(); j++){
				try{
					pre_sample=cur_sample;
					cur_sample=samplelist.get(j);
					if(cur_sample.suid==23777 && cur_sample.utc.getTime()/1000==1231221503L){
						//System.out.println("here");
					}
					if(cur_sample.suid != cur_suid || cur_sample.stop==1 || cur_sample.gid==0 || cur_sample.passenager==0){
						//end a trip
						cur_suid=cur_sample.suid;
						start_sample=null;
						if( has_passenager && (trip_start>=0 && trip_start<samplelist.size())){
							start_sample=samplelist.get(trip_start);
							long interval=(pre_sample.utc.getTime()-start_sample.utc.getTime())/1000;
							double tempstop_time=0;
							if(interval>=min_trip_interval){
								ArrayList<Integer> gidlist=new ArrayList<Integer>();
								int cur_index=-1;
								String[] route_gids=null;
								cur_index++;
								gidlist.add(start_sample.gid);
								for(int i=trip_start+1; i<j;i++){
									if(samplelist.get(i).stop==2){
										tempstop_time+=samplelist.get(i).interval;
									}
									String step_route=samplelist.get(i).route;
									if(step_route==null){
										continue;
									}
									route_gids=step_route.split(",");
									for(int k=0;k<route_gids.length;k++){
										try{
											if(route_gids[k].equals("")) continue;
											int gid=Integer.parseInt(route_gids[k]);
											if(gid<0){
												break;
											}
											
											if( k==0 && cur_index>=0 && gid == gidlist.get(cur_index)){
												continue;
											}
											else{
												cur_index++;
												gidlist.add(gid);
											}
										}
										catch(Exception e){
											e.printStackTrace();
											//break;
										}
									}
								}
								
								if(tempstop_time/interval <= max_tempstop_percent){
									try{
								    	spt = con.setSavepoint("svpt1");
								    	String route="";
								    	for(int k=0;k<gidlist.size();k++){
								    		route+=gidlist.get(k);
								    		if(k<gidlist.size()-1){
								    			route+=",";
								    		}
								    	}
								    	
								    	sql="insert into "+trips_table+"(suid, start_gid, start_pos, start_utc, end_gid, end_pos, end_utc, route, valid)" 
								    			+" values("+start_sample.suid+", "+start_sample.gid+", "+df.format(start_sample.offset)+", "+start_sample.utc.getTime()/1000+", "
								    			+pre_sample.gid+", "+df.format(pre_sample.offset)+", "+pre_sample.utc.getTime()/1000+", '"+route+"', 1);";
										
								    	//System.out.println("["+j+"/"+samplelist.size()+"]"+sql);
										
										stmt.executeUpdate(sql);
								    }
								    catch (SQLException e) {
										e.printStackTrace();
										con.rollback(spt);
									}
									catch (Exception e) {
										e.printStackTrace();
									}
									finally {
										con.commit();				
									}
								}
							}
						}
						
						//last trip finished
						trip_start=-1;
						has_passenager=false;
						
						if(cur_sample.stop!=1 && cur_sample.gid!=0 && cur_sample.passenager!=0){//not stop && has passenager;
							trip_start=j;
							has_passenager=true;
						}
					}
					else{//cur_sample.suid == cur_suid && cur_sample.stop!=1 && cur_sample.passenager==1
						if(!has_passenager){
							trip_start=j;
							has_passenager=true;
						}
					}
				}
				catch (SQLException e) {
				    e.printStackTrace();
				}
				catch (Exception e) {
				    e.printStackTrace();
				}
			}
			
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    DBconnector.dropConnection(con);
		}
		//System.out.println("get_roadlist finished!");
	}
	
	public static AllocationRoadsegment[] get_roadlist(String database, String time_table, boolean has_nextgid){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		int max_gid=0;
		AllocationRoadsegment[] roadlist=null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return null;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    String sql="";
		    if(!has_nextgid){
		    	sql="select gid from "+time_table+" order by gid desc limit 1";
		    }
		    else{
		    	sql="select gid from "+time_table+" where next_gid is null order by gid desc limit 1";
		    }
		    rs = stmt.executeQuery(sql);
		    while(rs.next()){
		    	max_gid=rs.getInt("gid");
		    }
		    
		    if(max_gid<=0){
		    	return null;
		    }
		    
		    roadlist=new AllocationRoadsegment[max_gid+1];
		    for(int i=0;i<roadlist.length;i++){
		    	roadlist[i]=new AllocationRoadsegment();
		    }
		    
		    //read the default value for road segment
		    if(!has_nextgid){
		    	sql="select * from "+time_table+" order by gid desc";
		    }
		    else{
		    	sql="select * from "+time_table+" where next_gid is null order by gid desc";
		    }
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    if(!has_nextgid){
			    while(rs.next()){
			    	int cur_gid=rs.getInt("gid");
			    	AllocationRoadsegment cur_road=new AllocationRoadsegment(cur_gid,rs.getDouble("max_speed"), rs.getDouble("average_speed"), rs.getInt("reference"));
			    	cur_road.reverse_cost=rs.getDouble("reverse_cost");
			    	cur_road.to_cost=rs.getDouble("to_cost");
			    	cur_road.length=rs.getDouble("length");
			    	roadlist[cur_gid]=cur_road;
			    }
		    }else{
		    	while(rs.next()){
			    	int cur_gid=rs.getInt("gid");
			    	AllocationRoadsegment cur_road=new AllocationRoadsegment(cur_gid, 0, rs.getDouble("average_speed"), rs.getInt("reference"));
			    	cur_road.reverse_cost=rs.getDouble("reverse_cost");
			    	cur_road.to_cost=rs.getDouble("to_cost");
			    	cur_road.length=rs.getDouble("length");
			    	roadlist[cur_gid]=cur_road;
			    }
		    }
		    
		    //read turning-specific value for road segment
		    if(has_nextgid){
			    sql="select * from "+ time_table +" where next_gid is not null order by gid, next_gid desc";
			    //System.out.println(sql);
			    rs = stmt.executeQuery(sql);
		    	while(rs.next()){
			    	int cur_gid=rs.getInt("gid");
			    	AllocationRoadsegment cur_road=roadlist[cur_gid];
			    	cur_road.add_turning_speed(rs.getInt("next_gid"), rs.getDouble("average_speed"));
			    	roadlist[cur_gid]=cur_road;
			    }
		    }
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    DBconnector.dropConnection(con);
		}
		//System.out.println("get_roadlist finished!");
		return roadlist;
	}
	
	public static void time_evaluation(String database, ArrayList<String> roadmap_tables, String aggregation_table, String trip_table, long start_utc, long interval, int min_trip_count){
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
			int max_interval=interval_count-1;
			long[] start_utcs=new long[interval_count];
			long[] end_utcs=new long[interval_count];
			ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
					
			for(int i=0; i<roadmap_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				time_list.add(TripFinder.get_roadlist(database, roadmap_tables.get(i), true));
				
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="alter table "+ roadmap_tables.get(i) +" add column start_utc bigint;";
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
				
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="alter table "+ roadmap_tables.get(i) +" add column end_utc bigint;";
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
				
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="update "+ roadmap_tables.get(i) +" set start_utc="+start_utcs[i]+", end_utc="+end_utcs[i]+";";
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
				
				if(i==0){
					try{
			    		spt = con.setSavepoint("svpt1");
			    		String sql="drop table "+ aggregation_table +";";
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
					try{
			    		spt = con.setSavepoint("svpt1");
			    		String sql="create table "+ aggregation_table +" as select * from "+roadmap_tables.get(i)+";";
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
				else{
					try{
			    		spt = con.setSavepoint("svpt1");
			    		String sql="insert into "+ aggregation_table +" select * from "+roadmap_tables.get(i)+";";
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
			}
			
			//insert the turning_specific travel time
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="select * from "+trip_table+" where suid in " +
	    				"(select suid from (select suid, count(*) from "+ trip_table +" group by suid) as temp where count>="+min_trip_count+") order by suid";
	    		//System.out.println(sql);
	    		rs=stmt.executeQuery(sql);
	    		
	    		//System.out.println("cur_suid,	total_actual,	total_simulate,	relative_error");
	    		
	    		long cur_suid=-1;
	    		double total_actual=0;
	    		double total_simulate=0;
	    		
	    		double trip_actual=0;
	    		double trip_simulate=0;
	    		
	    		String route;
	    		int start_gid=-1;
	    		int end_gid=-1;
	    		double start_pos=-1.0;
	    		double end_pos=-1.0;
	    		
	    		ArrayList<Long> invalid_suids=new ArrayList<Long>();
	    		ArrayList<Long> invalid_utcs=new ArrayList<Long>();
	    		
		    	while(rs.next()){
		    		long suid=rs.getLong("suid");
		    		if(cur_suid!=-1 && suid!=cur_suid){
		    			//System.out.println(cur_suid+","+total_actual+","+ total_simulate+","+ (total_simulate-total_actual)*1.0/total_actual);
		    			cur_suid=suid;
		    			total_actual=0;
		    			total_simulate=0;
		    		}
		    		cur_suid=suid;
		    		trip_actual=0;
	    			trip_simulate=0;
	    			
			    	long cur_utc=rs.getLong("start_utc");
			    	long end_utc=rs.getLong("end_utc");
			    	trip_actual += end_utc-cur_utc;
			    	
			    	start_gid=rs.getInt("start_gid");
			    	end_gid=rs.getInt("end_gid");
			    	start_pos=rs.getDouble("start_pos");
			    	end_pos=rs.getDouble("end_pos");
			    	
			    	route=rs.getString("route");
			    	String[] roads=null;
			    	int gid=-1;
			    	int next_gid=-1;
			    	int pre_interval=-1;
			    	AllocationRoadsegment[] time=null;
			    	double step_simulate=0.0;
			    	boolean simulation_interupted=false;
			    	if(route!=null){
			    		roads=route.split(",");
			    		for(int i=0;i<roads.length;i++){
			    			
			    			if(!roads[i].equals("")){
			    				gid=Integer.parseInt(roads[i]);
			    			}
			    			else{
			    				continue;
			    			}
			    			if(i+1<roads.length && !roads[i+1].equals("")){
			    				next_gid=Integer.parseInt(roads[i+1]);
			    			}
			    			else{
			    				next_gid=-1;
			    			}
			    			
			    			int cur_interval=(int)((cur_utc+trip_simulate-start_utc)/interval);
			    			if(cur_interval>max_interval){
			    				cur_interval=max_interval;
			    			}
			    			
			    			if(cur_interval!=pre_interval){
			    				time=time_list.get(cur_interval);
			    				pre_interval=cur_interval;
			    			}
			    			
			    			if(gid==31002){
			    				//System.out.println("here");
			    			}
			    			
			    			if(i==0 && gid==start_gid){
			    				if(i==roads.length-1 && gid==end_gid){
			    					step_simulate = time[gid].get_traveltime(next_gid)*Math.abs(start_pos-end_pos);
			    					if(step_simulate>0){
				    					trip_simulate+= step_simulate;
				    				}
			    					else{
				    					simulation_interupted=true;
				    					break;
				    				}
			    				}
			    				else{
			    					step_simulate = time[gid].get_traveltime(start_pos, false, next_gid);
			    					if(step_simulate>0){
			    						trip_simulate+= step_simulate;
				    				}
			    					else{
				    					simulation_interupted=true;
				    					break;
				    				}
			    				}
			    			}
			    			else if(i==roads.length-1 && gid==end_gid){
			    				step_simulate = time[gid].get_traveltime(start_pos, true, next_gid);
			    				if(step_simulate>0){
			    					trip_simulate+= step_simulate;
			    				}
			    				else{
			    					simulation_interupted=true;
			    					break;
			    				}
			    			}
			    			else{
			    				step_simulate = time[gid].get_traveltime(next_gid);
			    				if(step_simulate>0){
			    					trip_simulate+= step_simulate;
			    				}
			    				else{
			    					simulation_interupted=true;
			    					break;
			    				}
			    			}
			    			//System.out.println(gid+",	"+trip_simulate+",	interval:"+(cur_interval+1));
			    			
			    		}
			    		if(!simulation_interupted){
			    			total_actual+=trip_actual;
			    			total_simulate=trip_simulate;
			    			System.out.println(cur_suid+","+rs.getLong("start_utc")+","+rs.getLong("end_utc")+","+trip_actual+","+ trip_simulate+","
			    					+ df.format((trip_simulate-trip_actual)*1.0/trip_actual));
			    		}
			    		else{
			    			//store to invalidate it in the database
			    			invalid_suids.add(cur_suid);
			    			invalid_utcs.add(rs.getLong("start_utc"));
			    		}
			    	}
			    }
		    	
		    	for(int i=0;i<invalid_suids.size();i++){
		    		
			    	try{
			    		spt = con.setSavepoint("svpt1");
			    		sql="update "+ trip_table +" set valid=0 where suid=" +invalid_suids.get(i) +" and start_utc="+ invalid_utcs.get(i)+";";
			    		System.out.println("["+i+"/"+invalid_suids.size()+"]:"+sql);
			    		if(stmt.executeUpdate(sql)!=1){
			    			System.err.println("Error: remove multiple trips or zero trips !!!!!!");
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
		DBconnector.dropConnection(con);
		//System.out.println("aggregate_time finished!");
	}
	
	public static void main(String[] args){
		
		if(args.length>0){
			try {
				
				String type=args[0];
				String sample_table="ring2_samples_"+type;
				String roadmap_table="ring2_time_"+type;
				String trip_table="ring2_trip_"+type;
				int min_sample_thres=2;
				
				double max_tempstop_precent=Double.parseDouble(args[1]);//0.2;
				int min_trip_interval=Integer.parseInt(args[2]);//600;
				
				TripFinder.get_trips("mydb", sample_table, trip_table, min_sample_thres, min_trip_interval, max_tempstop_precent);
				
				//for different period, use different Travel_time
				long cur_utc=Long.parseLong(args[3]);//1231218000L;
				long period=900L;
				
				ArrayList<String> roadmap_tables=new ArrayList<String>();
				for(int i=1; i<=4; i++){
					roadmap_tables.add(roadmap_table+"_"+i);
				}
				TripFinder.time_evaluation("mydb", roadmap_tables, roadmap_table, trip_table, cur_utc, period, 1);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else{
			try {
				String sample_table="ring2_samples_rush";
				String roadmap_table="ring2_time_rush";
				String trip_table="ring2_trip_rush";
				int min_sample_thres=2;
				double max_tempstop_precent=0.1;
				int min_trip_interval=600;
				TripFinder.get_trips("mydb", sample_table, trip_table, min_sample_thres, min_trip_interval, max_tempstop_precent);
				
				//for different period, use different Travel_time
				long cur_utc=1231232400L;
				long period=900L;
				
				ArrayList<String> roadmap_tables=new ArrayList<String>();
				for(int i=1; i<=4; i++){
					roadmap_tables.add(roadmap_table+"_"+i);
				}
				TripFinder.time_evaluation("mydb", roadmap_tables, roadmap_table, trip_table, cur_utc, period, 1);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}