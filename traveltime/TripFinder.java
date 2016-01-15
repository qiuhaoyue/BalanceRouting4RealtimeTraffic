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
	    				" end_gid integer, end_pos double precision, end_utc bigint, route text, stop_rate double precision);" ;
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
			
			ArrayList<Long> suids=new ArrayList<Long>();
			//import samples from database;
		    String sql= "select suid from (select suid, count(*) from "+sample_table+" where ostdesc like '%重车%' group by suid ) as temp " +
		    				"where temp.count>= "+min_sample_thres+" order by suid;";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    while(rs.next()){
		    	suids.add(rs.getLong("suid"));
		    }
		    
		    for(int tc=0; tc<suids.size();tc++){
		    	sql= "select * from "+sample_table+" where suid ="+ suids.get(tc) +" order by utc;";
			    System.out.println("[ "+tc+" / "+suids.size()+" ]:"+sql);
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
				for(int j=0; j<samplelist.size(); j++){
					try{
						pre_sample=cur_sample;
						cur_sample=samplelist.get(j);
						/*if(cur_sample.suid==23777 && cur_sample.utc.getTime()/1000==1231221503L){
							//System.out.println("here");
						}*/
						if( cur_sample.suid != cur_suid || cur_sample.stop==1 || cur_sample.gid==0 || cur_sample.passenager==0 
								|| (( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && cur_sample.stop != 2 ) || ( j == samplelist.size()-1 )){
							// possibly the end of a trip
							if( j == samplelist.size()-1 ){
								pre_sample=cur_sample;
							}
							cur_suid=cur_sample.suid;
							start_sample=null;
							if( has_passenager && (trip_start>=0 && trip_start < samplelist.size())){
								start_sample=samplelist.get(trip_start);
								long interval=(pre_sample.utc.getTime()-start_sample.utc.getTime())/1000;
								double tempstop_time=0;
								if(interval>=min_trip_interval){
									ArrayList<Integer> gidlist=new ArrayList<Integer>();
									int cur_index=-1;
									String[] route_gids=null;
									cur_index++;
									gidlist.add(start_sample.gid);
									if( j == samplelist.size()-1 ){
										j++;
									}
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
									
									double stop_rate=tempstop_time/interval;
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
									    	
									    	sql="insert into "+trips_table+"(suid, start_gid, start_pos, start_utc, end_gid, end_pos, end_utc, route, stop_rate)" 
									    			+" values("+start_sample.suid+", "+start_sample.gid+", "+df.format(start_sample.offset)+", "+start_sample.utc.getTime()/1000+", "
									    			+pre_sample.gid+", "+df.format(pre_sample.offset)+", "+pre_sample.utc.getTime()/1000+", '"+route+"', "+stop_rate+");";
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
							
							if( cur_sample.stop != 1 && cur_sample.gid != 0 && cur_sample.passenager != 0 ){//not stop && has passenager;
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
		    try{
	    		spt = con.setSavepoint("svpt1");
	    		sql="CREATE INDEX "+trips_table+"_suidutc_idx ON "+ trips_table +"(suid, start_utc);";
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
		finally {
		    DBconnector.dropConnection(con);
		}
		//System.out.println("get_roadlist finished!");
	}
	
	public static void get_all_trips(String database, String sample_table, String trips_table, int min_sample_thres, int min_trip_interval, int default_interval, double max_tempstop_percent){
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
	    				" end_gid integer, end_pos double precision, end_utc bigint, route text, stop_rate double precision, type integer);" ;
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
			
			ArrayList<Long> suids=new ArrayList<Long>();
			//suids.add(1723L);
			
			//import samples from database;
		    String sql= "select suid from (select suid, count(*) from "+sample_table+" group by suid ) as temp " +
		    				"where temp.count>= "+min_sample_thres+" order by suid;";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    while(rs.next()){
		    	suids.add(rs.getLong("suid"));
		    }
		    
		    for(int tc=0; tc<suids.size();tc++){
		    	
		    	//load the samples
		    	sql= "select * from "+sample_table+" where suid ="+ suids.get(tc) +" order by utc;";
			    System.out.println("[ "+tc+" / "+suids.size()+" ]:"+sql);
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
				
				//get the trips;
				Sample cur_sample=null;
				Sample start_sample=null;
				Sample pre_sample=null;
				int trip_start=-1;
				boolean has_passenager=false; //0 for inital, 1 for empty, 2 for passanger
				long cur_suid=-1;
				long trip_interval=0;
				for(int j=0; j<samplelist.size(); j++){
					try{
						pre_sample=cur_sample;
						cur_sample=samplelist.get(j);
						int end_index=j; //end_index is the first sample that is not in current trip;
						
						if(cur_sample.suid==1723 && cur_sample.utc.getTime()/1000==1231203150L){
							System.out.println("here");
						}
						
						if(trip_start>=0 && trip_start < samplelist.size()){
							trip_interval+=cur_sample.interval;
						}
						else{
							trip_interval=-1;
						}
						
						if(has_passenager==false){
							
							//ending conditions of no_passenager trip
							if( trip_start>=0 && (cur_sample.suid != cur_suid || cur_sample.stop==1 || cur_sample.gid==0 || cur_sample.passenager==1 
									|| (( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && ( cur_sample.stop != 2))
									|| trip_interval>default_interval || ( j == samplelist.size()-1 ))){
								
								cur_suid=cur_sample.suid;

								//end because of passenger on board or trip too long;
								if( (cur_sample.passenager == 1 || trip_interval > default_interval || ( j == samplelist.size()-1 )) 
										&& cur_sample.stop != 1 && cur_sample.gid != 0 
										&& !(( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && cur_sample.stop != 2)){
									pre_sample=cur_sample;
									end_index++; //means trip ends at cur_sample and probably starts with cur_sample;
								}
								
								//prepare the trip and record it in database;
								start_sample=null;
								if( trip_start>=0 && trip_start < samplelist.size()){
									start_sample=samplelist.get(trip_start);
									long interval=(pre_sample.utc.getTime()-start_sample.utc.getTime())/1000;
									double tempstop_time=0;
									if(interval>=min_trip_interval){
										ArrayList<Integer> gidlist=new ArrayList<Integer>();
										int cur_index=-1;
										String[] route_gids=null;
										cur_index++;
										gidlist.add(start_sample.gid);
										
										for(int i=trip_start+1; i<end_index; i++){
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
													if(route_gids[k].equals("")) {
														continue;
													}
													
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
										
										double stop_rate=tempstop_time/interval;
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
										    	
										    	sql="insert into "+trips_table+"(suid, start_gid, start_pos, start_utc, end_gid, end_pos, end_utc, route, stop_rate, type)" 
										    			+" values("+start_sample.suid+", "+start_sample.gid+", "+df.format(start_sample.offset)+", "+start_sample.utc.getTime()/1000+", "
										    			+pre_sample.gid+", "+df.format(pre_sample.offset)+", "+pre_sample.utc.getTime()/1000+", '"+route+"', "+stop_rate+", 0);";
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
								if( cur_sample.stop != 1 && cur_sample.gid != 0 &&  !(( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && cur_sample.stop != 2 )){
									trip_start=end_index-1;
									start_sample=samplelist.get(trip_start);
									trip_interval=(cur_sample.utc.getTime()-start_sample.utc.getTime())/1000;
									trip_interval=cur_sample.interval;
								}
								else if(cur_sample.stop != 1 && cur_sample.gid != 0 && (( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && cur_sample.stop != 2 )){
									trip_start=end_index;
									start_sample=samplelist.get(trip_start);
									trip_interval=(cur_sample.utc.getTime()-start_sample.utc.getTime())/1000;
									trip_interval=cur_sample.interval;
								}
								else{
									trip_start=-1;
									trip_interval=-1;
								}
								
								
								if(cur_sample.passenager != 0){
									has_passenager=true;
								}
								else{
									has_passenager=false;
								}
							}
							else{//if not possibly end a trip
								if(cur_sample.stop != 1 && cur_sample.gid != 0 && trip_start<0){
									trip_start=j;
									trip_interval=0;
								}
								
								if(cur_sample.passenager != 0){
									has_passenager=true;
								}
								else{
									has_passenager=false;
								}
							}
						}
						
						else{//if has_passenager==true;
							
							// possibly the end of a trip
							if( trip_start>=0 && (cur_sample.suid != cur_suid || cur_sample.stop==1 || cur_sample.gid==0 || cur_sample.passenager==0 
									|| (( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && cur_sample.stop != 2 ) || ( j == samplelist.size()-1 ))){
								
								if( (j == samplelist.size()-1 || cur_sample.passenager==0 ) && cur_sample.stop != 1 && cur_sample.gid !=0 &&
										!(( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && cur_sample.stop != 2 )	){
									pre_sample=cur_sample;
									end_index++;
								}
								
								cur_suid=cur_sample.suid;
								start_sample=null;
								
								if( trip_start>=0 && trip_start < samplelist.size()){
									start_sample=samplelist.get(trip_start);
									long interval=(pre_sample.utc.getTime()-start_sample.utc.getTime())/1000;
									double tempstop_time=0;
									if(interval>0 && interval>=min_trip_interval){
										ArrayList<Integer> gidlist=new ArrayList<Integer>();
										int cur_index=-1;
										String[] route_gids=null;
										cur_index++;
										gidlist.add(start_sample.gid);
										
										for(int i=trip_start+1; i<end_index;i++){
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
										
										double stop_rate=tempstop_time/interval;
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
										    	
										    	sql="insert into "+trips_table+"(suid, start_gid, start_pos, start_utc, end_gid, end_pos, end_utc, route, stop_rate, type)"
										    			+" values("+start_sample.suid+", "+start_sample.gid+", "+df.format(start_sample.offset)+", "+start_sample.utc.getTime()/1000+", "
										    			+pre_sample.gid+", "+df.format(pre_sample.offset)+", "+pre_sample.utc.getTime()/1000+", '"+route+"', "+stop_rate+", 1);";
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
								if( cur_sample.stop != 1 && cur_sample.gid != 0 &&  !(( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && cur_sample.stop != 2 )){
									trip_start=end_index-1;
									start_sample=samplelist.get(trip_start);
									trip_interval=(cur_sample.utc.getTime()-start_sample.utc.getTime())/1000;
									trip_interval=cur_sample.interval;
								}
								else if(cur_sample.stop != 1 && cur_sample.gid != 0 && (( cur_sample.route==null || cur_sample.route.compareTo("")==0 ) && cur_sample.stop != 2 )){
									trip_start=end_index;
									start_sample=samplelist.get(trip_start);
									trip_interval=(cur_sample.utc.getTime()-start_sample.utc.getTime())/1000;
									trip_interval=cur_sample.interval;
								}
								else{
									trip_start=-1;
									trip_interval=-1;
								}
								
								if(cur_sample.passenager != 0){
									has_passenager=true;
								}
								else{
									has_passenager=false;
								}
							}
							else{//if not possibly end a trip
								if( cur_sample.stop != 1 && cur_sample.gid != 0 && trip_start<0 ){
									trip_start= j;
									trip_interval= 0;
								}
								
								if(cur_sample.passenager != 0){
									has_passenager=true;
								}
								else{
									has_passenager=false;
								}
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
		    try{
	    		spt = con.setSavepoint("svpt1");
	    		sql="CREATE INDEX "+trips_table+"_suidutc_idx ON "+ trips_table +"(suid, start_utc);";
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
		    if(has_nextgid){
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
		    sql="select * from "+time_table+" where next_gid is null order by gid desc";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    while(rs.next()){
			    int cur_gid=rs.getInt("gid");
			    AllocationRoadsegment cur_road=new AllocationRoadsegment(cur_gid, rs.getDouble("average_speed"), rs.getDouble("reference"), rs.getDouble("taxi_ratio"));
			    cur_road.reverse_cost=rs.getDouble("reverse_cost");
			    cur_road.to_cost=rs.getDouble("to_cost");
			    cur_road.length=rs.getDouble("length");
			    roadlist[cur_gid]=cur_road;
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
	
	public static AllocationRoadsegment[] get_roadlist(String database, String time_table, String time_col, String traffic_col, boolean has_nextgid){
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
		    if(has_nextgid){
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
		    sql="select * from "+time_table+" where next_gid is null order by gid";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    while(rs.next()){
			    int cur_gid=rs.getInt("gid");
			    double edge_time=rs.getDouble(time_col);
			    double speed=rs.getDouble("length")*1000/edge_time;
			    
			    AllocationRoadsegment cur_road=new AllocationRoadsegment(cur_gid, speed, rs.getDouble(traffic_col), rs.getDouble("taxi_ratio"));
			    cur_road.reverse_cost=rs.getDouble("reverse_cost");
			    cur_road.to_cost=rs.getDouble("to_cost");
			    cur_road.length=rs.getDouble("length");
			    roadlist[cur_gid]=cur_road;
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
	
	//Function to get parameters used traffic-delay model
	public static AllocationRoadsegment[] get_static_roadlist(String database, String static_roadmap){
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
		    sql="select gid from "+static_roadmap+" order by gid desc limit 1";
		    
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
		    sql="select * from "+static_roadmap+" order by gid desc";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    ArrayList<String> cols=new ArrayList<String>();
		    //String type="";
		    String type="time_";
		    //String type="count_";
		    cols.add(type+"alpha");
		    cols.add(type+"beta");
		    cols.add(type+"r2");
		    
		    while(rs.next()){
			    int cur_gid=rs.getInt("gid");
			    AllocationRoadsegment cur_road=new AllocationRoadsegment(cur_gid, rs.getDouble("revised_max_speed"), 
			    		rs.getDouble(cols.get(0)), rs.getDouble(cols.get(1)), rs.getDouble("capacity"));
			    cur_road.model_r2=rs.getDouble(cols.get(2));
			    roadlist[cur_gid]=cur_road;
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
	
	public static void nontrip_footprint(String database, ArrayList<String> roadmap_tables, String sample_table, String trip_table, 
			String secondary_footprint, String trip_condition, long start_utc, long interval){
		
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
			//AllocationRoadsegment[] roadlist=null;
			
			//For each interval calculate the traffic time for road segments
			for(int i=0; i<roadmap_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				AllocationRoadsegment[] time = TripFinder.get_roadlist(database, roadmap_tables.get(i), true);
				System.out.println("Reading roadlist from "+roadmap_tables.get(i));
				time_list.add(time);
			}
			
			//get the suids
			ArrayList<Integer> suids=new ArrayList<Integer>();
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="select suid from "+sample_table+" group by suid;";
	    		System.out.println(sql);
	    		rs=stmt.executeQuery(sql);
	    		while(rs.next()){
	    			suids.add(rs.getInt("suid"));
	    		}
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
			//create secondary trip table;
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ secondary_footprint +";" ;
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
	    		spt = con.setSavepoint("svpt1");
	    		String sql="create table "+ secondary_footprint +"(suid bigint, trip_utc bigint, gid integer, coverage double precision, start_utc bigint, end_utc bigint);";
	    		
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
			
			//for each taxi get the trips that are not in the trip table
			for(int i=0; i<suids.size(); i++){
				int cur_suid=suids.get(i);
				//get the sample_trips
				System.out.println("["+i+"/"+suids.size()+"] starting to get non_trip footprint");
				ArrayList<Sample> samplelist= new ArrayList<Sample>();
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="select * from "+sample_table+" where suid= "+cur_suid+" order by utc;";
		    		//System.out.println(sql);
				    rs = stmt.executeQuery(sql);
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
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
				
				//get included trips
				ArrayList<Trip> trips=new ArrayList<Trip>();
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql=" select suid, start_utc,end_utc from "+trip_table+" where suid= "+cur_suid+" "+trip_condition+" order by start_utc;";
		    		//System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
		    		while(rs.next()){
		    			Trip trip=new Trip(rs.getLong("suid"), rs.getLong("start_utc"), 0);
		    			trip.end_utc=rs.getLong("end_utc");
		    			trips.add(trip);
		    		}
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
			
				int trip_index=0;
				
				long trip_start_utc=0;
				long trip_end_utc=0;
				boolean all_trip_finish=false;
				
				if(trips.size()>0){
					trip_start_utc=trips.get(trip_index).start_utc;
					trip_end_utc=trips.get(trip_index).end_utc;
				}
				else{
					all_trip_finish=true;
				}
				
				for(int j=0;j<samplelist.size();j++){
					Sample cur_sample=samplelist.get(j);
					long end_time=cur_sample.utc.getTime()/1000;
					long start_time=end_time-cur_sample.interval;
					
					while(! all_trip_finish && start_time>=trip_end_utc){
						trip_index++;
						if(trip_index>=trips.size()){
							all_trip_finish=true;
						}
						else{
							trip_start_utc=trips.get(trip_index).start_utc;
							trip_end_utc=trips.get(trip_index).end_utc;
						}
					}
					
					String route=cur_sample.route;
					int start_gid=cur_sample.pre_gid;
					int end_gid=cur_sample.gid;
					double start_pos=cur_sample.pre_offset;
					double end_pos=cur_sample.offset;
					/*
					if(route == null || route.compareTo("")==0){
						continue;
					}*/
					
					if( all_trip_finish || (start_time<trip_start_utc && end_time<=trip_start_utc)){ 
						
						String[] roads=null;
				    	int gid=-1;
				    	int next_gid=-1;
				    	int pre_interval=-1;
				    	AllocationRoadsegment[] time=null;
				    	double percent;
				    	
				    	double step_simulate=0.0;
				    	double cur_utc=start_time;
				    	double next_utc=cur_utc+step_simulate;
				    	
				    	ArrayList<String> values=new ArrayList<String>();
				    	if( route!=null && route.compareTo("")!=0 || ((route == null || route.compareTo("")==0) && cur_sample.stop == Sample.TEM_STOP)){
				    		if( route!=null && route.compareTo("")!=0){
					    		roads=route.split(",");
					    		for(int idx=0;idx<roads.length;idx++){
					    			if(!roads[idx].equals("")){
					    				gid=Integer.parseInt(roads[idx]);
					    				if(gid<0){
					    					continue;
					    				}
					    			}
					    			else{
					    				continue;
					    			}
					    			
					    			if(idx+1<roads.length && !roads[idx+1].equals("")){
					    				next_gid=Integer.parseInt(roads[idx+1]);
					    			}
					    			else{
					    				next_gid=-1;
					    			}
					    			
					    			//time based simulation
					    			int cur_interval=(int)((cur_utc-start_utc)/interval);
					    			if(cur_interval>max_interval){
					    				cur_interval=max_interval;
					    			}
					    			
					    			if(cur_interval!=pre_interval){
					    				time=time_list.get(cur_interval);
					    				pre_interval=cur_interval;
					    			}
					    			
					    			/*
					    			if(gid==31002){
					    				//System.out.println("here");
					    			}*/
					    			if(idx==0 && gid==start_gid){
					    				if(idx==roads.length-1 && gid==end_gid){
					    					//for time-based simulation;
					    					step_simulate = time[gid].get_traveltime(next_gid)*Math.abs(start_pos-end_pos);
					    				}
					    				else{
					    					step_simulate = time[gid].get_traveltime(start_pos, false, next_gid);
					    				}
					    			}
					    			else if(idx==roads.length-1 && gid==end_gid){
					    				step_simulate = time[gid].get_traveltime(end_pos, true, next_gid);
					    			}
					    			else{
					    				step_simulate = time[gid].get_traveltime(next_gid);
					    			}
					    			percent=step_simulate/time[gid].get_traveltime(next_gid);
					    			
					    			if(step_simulate<0){
					    				step_simulate=1;
					    			}
					    			
					    			next_utc=cur_utc+step_simulate;
					    			values.add("("+cur_suid+", "+start_time+", "+ gid +", "+percent+", "+(long)cur_utc+", "+(long)(next_utc)+")");
					    			cur_utc=next_utc;
					    			//"(suid bigint, trip_utc bigint, gid integer, coverage double precision, start_utc bigint, end_utc bigint);";
					    			//System.out.println(gid+",	"+trip_simulate+",	interval:"+(cur_interval+1));
					    		}
				    		}
				    		else{
				    			if(start_gid==end_gid){
				    				values.add("("+cur_suid+", "+start_time+", "+ start_gid +", "+Math.abs(start_pos-end_pos)+", "+(long)start_time+", "+(long)(end_time)+")");
				    			}
				    			else{
				    				
				    				int cur_interval=(int)((cur_utc-start_utc)/interval);
					    			if(cur_interval>max_interval){
					    				cur_interval=max_interval;
					    			}
					    			
					    			if(cur_interval!=pre_interval){
					    				time=time_list.get(cur_interval);
					    				pre_interval=cur_interval;
					    			}
					    			
					    			double step_time=time[start_gid].get_traveltime(start_pos, false, end_gid);
					    			double precent=step_time/time[start_gid].get_traveltime(end_gid);
				    				values.add("("+cur_suid+", "+start_time+", "+ start_gid +", "+precent+", "+(long)start_time+", "+(long)(start_time+step_time)+")");
				    				
				    				step_time=time[end_gid].get_traveltime(end_pos, true, -1);
				    				precent=step_time/time[end_gid].get_traveltime(-1);
				    				values.add("("+cur_suid+", "+start_time+", "+ end_gid +", "+precent+", "+(long)(end_time-step_time)+", "+(long)(end_time)+")");
					    			
				    			}
				    			
				    		}
				    		try{
	    	    		    	spt = con.setSavepoint("svpt1");
	    	    		    	String sql="insert into "+ secondary_footprint +" values ";
	    	    		    	for(int vi=0; vi<values.size(); vi++){
	    	    		    		sql+=values.get(vi);
	    	    		    		if(vi!=values.size()-1){
	    	    		    			sql+=",\n";
	    	    		    		}
	    	    		    		else{
	    	    		    			sql+=";";
	    	    		    		}
	    	    		    	}
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
	 * The Function use the reference count in roadmap_tables to generate background traffic, 
	 * then add the traffic routed and traffic not routed to get the total traffic
	 * Then use the total traffic to calculate the travel time for each road in roadmap_tables;
	 * finally use the newly-generated time in roadmap_tables to recalculate the travel time of the trips;
	 * 
	 * String time_col gives the name of traffic_time, the simulated time based on traffic_count;
	 * String traffic_col gives the name of total traffic after rerouting;
	 * this column is added to roadmap_tables;
	*/
	public static void traffictime_calculation(String database, ArrayList<String> roadmap_tables, String static_roadmap,
			ArrayList<String> footprint_tables, String time_col, String traffic_col, double scale, long start_utc, int start_index, long interval){
		
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
			long[] start_utcs=new long[interval_count];
			long[] end_utcs=new long[interval_count];
			
			AllocationRoadsegment[] static_roadlist=TripFinder.get_static_roadlist(database, static_roadmap);
			ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
			
			//get the scale factor
			ArrayList<Double> txrt_correction=new ArrayList<Double>();
			txrt_correction.add(1.0);
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="select pobrt_time from txrt_correction order by seq;";
	    		System.out.println(sql);
	    		rs=stmt.executeQuery(sql);
	    		rs=stmt.executeQuery(sql);
	    		while(rs.next()){
	    			txrt_correction.add(rs.getDouble("pobrt_time"));
	    		}
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
			//For each interval calculate the traffic time for road segments
			for(int i=0; i<roadmap_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				
				double correction_factor=txrt_correction.get(i+start_index-1);
				System.out.println("Traffic_speed Calculating for "+ roadmap_tables.get(i));
				//change the schema of each of roadmap_tables 
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="alter table "+ roadmap_tables.get(i) +" drop column IF EXISTS "+time_col+";";
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
		    		spt = con.setSavepoint("svpt1");
		    		String sql="alter table "+ roadmap_tables.get(i) +" add column "+time_col+" double precision;";
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
		    		spt = con.setSavepoint("svpt1");
		    		String sql="alter table "+ roadmap_tables.get(i) +" drop column IF EXISTS "+traffic_col+";";
		    		System.out.println(sql);
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    //e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					con.commit();
				}
				
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="alter table "+ roadmap_tables.get(i) +" add column "+traffic_col+" double precision;";
		    		System.out.println(sql);
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    //e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					con.commit();
				}
				
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="alter table "+ roadmap_tables.get(i) +" drop column IF EXISTS "+traffic_col+"_taxi;";
		    		System.out.println(sql);
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    //e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					con.commit();
				}
				
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="alter table "+ roadmap_tables.get(i) +" add column "+traffic_col+"_taxi double precision;";
		    		System.out.println(sql);
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    //e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					con.commit();
				}
				
				try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="create index "+ roadmap_tables.get(i) +"_gid_idx on "+roadmap_tables.get(i)+"(gid);";
		    		System.out.println(sql);
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    //e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					con.commit();
				}
				
				//get the background traffic;
				AllocationRoadsegment[] roadlist = TripFinder.get_roadlist(database, roadmap_tables.get(i), false);
				int road_count=roadlist.length;
				for(int gid=0; gid<road_count; gid++){
					if(roadlist[gid].taxi_ratio<=0){//if this road is not initialized
						roadlist[gid].traffic_count=0;
						roadlist[gid].taxi_count=0;
						continue;
					}
					
					//change the taxi_rate according to the scale factor;
					roadlist[gid].taxi_ratio=roadlist[gid].taxi_ratio*scale/correction_factor;
					//background traffic_count is the total traffic - traffic in system;
					roadlist[gid].traffic_count=(roadlist[gid].taxi_count/(roadlist[gid].taxi_ratio*0.4/100))*(1-roadlist[gid].taxi_ratio*0.4/100);
					if(roadlist[gid].traffic_count<0){
						roadlist[gid].traffic_count=0;
					}
					//System.out.println("roadlist["+gid+"].traffic_count="+roadlist[gid].traffic_count);
					//System.out.println("roadlist["+gid+"].taxi_count="+roadlist[gid].taxi_count);
					roadlist[gid].taxi_count=0;
				}
				
				//add the rerouted traffic
				for(int ftprt_idx=0; ftprt_idx<footprint_tables.size(); ftprt_idx++){
					
					String footprint_table=footprint_tables.get(ftprt_idx);
					if(i==0){
						try{
				    		spt = con.setSavepoint("svpt1");
				    		String sql="create index "+ footprint_table +"_gid_idx on "+ footprint_table +"(gid);";
				    		System.out.println(sql);
				    		stmt.executeUpdate(sql);
				    	}
				    	catch (SQLException e) {
						    //e.printStackTrace();
						    con.rollback(spt);
						}
						finally{
							con.commit();
						}
						
						try{
				    		spt = con.setSavepoint("svpt1");
				    		String sql="create index "+ footprint_table+"_startutc_idx on "+ footprint_table +"(start_utc);";
				    		System.out.println(sql);
				    		stmt.executeUpdate(sql);
				    	}
				    	catch (SQLException e) {
						    //e.printStackTrace();
						    con.rollback(spt);
						}
						finally{
							con.commit();
						}
						
						try{
				    		spt = con.setSavepoint("svpt1");
				    		String sql="create index "+ footprint_table+"_endutc_idx on "+ footprint_table +"(end_utc);";
				    		System.out.println(sql);
				    		stmt.executeUpdate(sql);
				    	}
				    	catch (SQLException e) {
						    //e.printStackTrace();
						    con.rollback(spt);
						}
						finally{
							con.commit();
						}
					}
					
					try{
			    		spt = con.setSavepoint("svpt1");
			    		String sql="select b.gid, sum(b.coverage*b.case) as reference from " +
			    						"( select gid, coverage, case " +
			    								"when start_utc< "+start_utcs[i]+" and end_utc<="+end_utcs[i]+" then (end_utc-"+start_utcs[i]+")/(end_utc-start_utc) \n" +
			    								"when start_utc< "+start_utcs[i]+" and end_utc>"+end_utcs[i]+" then "+(end_utcs[i]-start_utcs[i])+"/(end_utc-start_utc) \n" +
			    								"when start_utc>="+start_utcs[i]+" and end_utc>"+end_utcs[i]+" then ("+end_utcs[i]+"-start_utc)/(end_utc-start_utc) \n" +
			    								"when start_utc>="+start_utcs[i]+" and end_utc<="+end_utcs[i]+" then 1.0 \n" +
			    								"else 1.0 \n" + "end \n" +
			    						"from "+ footprint_table +
			    						" where (end_utc<="+end_utcs[i]+" and end_utc>"+start_utcs[i]+") or (start_utc >= "+start_utcs[i]+" and start_utc<"+end_utcs[i]+
			    								") or (start_utc<"+start_utcs[i]+" and end_utc>"+end_utcs[i]+")) as b " +
			    					"group by b.gid;";
			    		//System.out.println(sql);
			    		rs=stmt.executeQuery(sql);
			    		while(rs.next()){
			    			int gid=rs.getInt("gid");
			    			roadlist[gid].taxi_count+=rs.getDouble("reference")/0.4;
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
				
				//put the result into list for evaluation;
				time_list.add(roadlist);
				
				String sql="";
				for(int gid=0; gid<road_count; gid++){
					//AllocationRoadsegment cur_rs=roadlist[gid];
					if(roadlist[gid].taxi_ratio<=0 ){//if this road is not initialized
						roadlist[gid].traffic_count=0;
						roadlist[gid].taxi_count=0;
						continue;
					}
					
					//System.out.println(gid+"/"+road_count);
					//calculate travel time based on new traffic
					double total_traffic=roadlist[gid].traffic_count+roadlist[gid].taxi_count;
					double traffic_edge_time=0;
					boolean simulation_fail=false;
					
					if(static_roadlist[gid].model_maxspeed>0 && static_roadlist[gid].model_r2>0 && total_traffic>0){
	    				double saturation=(total_traffic*4)/static_roadlist[gid].model_capacity;
	    				traffic_edge_time = roadlist[gid].length*1000/static_roadlist[gid].model_maxspeed*
	    						(1+static_roadlist[gid].model_alpha*Math.pow(saturation, static_roadlist[gid].model_beta));
	    				//trip_traffic_simulate+= step_simulate;
	    				
	    				roadlist[gid].avg_inst_speed = roadlist[gid].length*1000/traffic_edge_time;
	    			}
	    			else{
	    				simulation_fail=true;
	    			}
					
					if(!simulation_fail){
						/*try{
				    		spt = con.setSavepoint("svpt1");
				    		String sql="update "+roadmap_tables.get(i)+" set "+time_col+"="+traffic_edge_time+", "+traffic_col+"="+total_traffic+
				    				" where gid="+gid+" ;\n";
				    		//System.out.println(sql);
				    		stmt.executeUpdate(sql);
				    	}
				    	catch (SQLException e) {
						    e.printStackTrace();
						    con.rollback(spt);
						}
						finally{
							con.commit();
						}*/
						sql+="update "+roadmap_tables.get(i)+" set "+time_col+"="+traffic_edge_time+", "+traffic_col+"="+total_traffic+
								", "+traffic_col+"_taxi="+roadlist[gid].taxi_count+" where gid="+gid+" ;\n";
					}
				}
				
				try{
		    		spt = con.setSavepoint("svpt1");
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
		    		System.err.println(sql);
				    e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					con.commit();
				}
				/*
				try{
		    		spt = con.setSavepoint("svpt1");
		    		sql="update "+roadmap_tables.get(i)+" set "+time_col+"=b.length*1000/b.revised_max_speed, "+traffic_col+
		    				"=0 from "+static_roadmap+" as b  where "+roadmap_tables.get(i)+"."+traffic_col+" is null and b.revised_max_speed>0 and "+roadmap_tables.get(i)+".gid=b.gid;";
		    		System.out.println(sql);
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					con.commit();
				}*/
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
	
	//time evaluation using traffic-delay model
	public static void traffictime_evaluation(String database, ArrayList<String> roadmap_tables, String static_roadmap,String trip_table, String route_col, 
			String evaluate_col, String time_col, String traffic_col, double r2_threshold, long start_utc, long interval){
		
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
			
			//change the schema of trip_table
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ trip_table +" drop column "+evaluate_col+";";
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
	    		String sql="alter table "+ trip_table +" add column "+evaluate_col+" double precision;";
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
			
			int interval_count=roadmap_tables.size();
			int max_interval=interval_count-1;
			long[] start_utcs=new long[interval_count];
			long[] end_utcs=new long[interval_count];
			ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
			AllocationRoadsegment[] static_roadlist=TripFinder.get_static_roadlist(database, static_roadmap);
			
			//For each interval calculate the traffic time for road segments
			for(int i=0; i<roadmap_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				AllocationRoadsegment[] roadlist = TripFinder.get_roadlist(database, roadmap_tables.get(i), time_col, traffic_col, false);
				System.out.println("Reading traffic data from "+roadmap_tables.get(i));
				time_list.add(roadlist);
			}
			
			//get the trips list in the given interval and do the evaluation
			for(int seq=0; seq<roadmap_tables.size(); seq++){
				try{
					System.out.println("Evaluating trips from interval["+seq+"/ "+roadmap_tables.size()+"]...");
					
		    		spt = con.setSavepoint("svpt1");
		    		String sql="select * from "+ trip_table +" where ( valid is null or valid >=0 ) and start_utc>="+start_utcs[seq]+
							" and start_utc<"+end_utcs[seq]+" order by start_utc; ";	
		    		//System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
		    		
		    		long cur_suid=-1;
		    		//double trip_actual=0;
		    		double trip_time_simulate=0;	
		    		
		    		String route;
		    		int start_gid=-1;
		    		int end_gid=-1;
		    		double start_pos=-1.0;
		    		double end_pos=-1.0;
		    		
		    		/*ArrayList<Long> invalid_suids=new ArrayList<Long>();
		    		ArrayList<Long> invalid_utcs=new ArrayList<Long>();
		    		ArrayList<Integer> invalid_types=new ArrayList<Integer>();*/

		    		ArrayList<Trip> time_trips=new ArrayList<Trip>();
	
			    	while(rs.next()){
			    		long suid=rs.getLong("suid");
			    		if(cur_suid!=-1 && suid!=cur_suid){
			    			//System.out.println(cur_suid+","+total_actual+","+ total_simulate+","+ (total_simulate-total_actual)*1.0/total_actual);
			    			cur_suid=suid;
			    			//total_actual=0;
			    			//total_simulate=0;
			    		}
			    		cur_suid=suid;
			    		//trip_actual=0;
		    			trip_time_simulate=0;
		    			
		    			//int interupt_type=0;
				    	long cur_utc=rs.getLong("start_utc");
				    	//long end_utc=rs.getLong("end_utc");
				    	//trip_actual += end_utc-cur_utc;
				    	
				    	start_gid=rs.getInt("start_gid");
				    	end_gid=rs.getInt("end_gid");
				    	start_pos=rs.getDouble("start_pos");
				    	end_pos=rs.getDouble("end_pos");
				    	
				    	route=rs.getString(route_col);
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
				    				if(gid<=0){
				    					continue;
				    				}
				    				else if(static_roadlist[gid].model_r2<r2_threshold ){
				    					simulation_interupted=true;
				    					break;
				    				}
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
				    			
				    			//traffic-estimation based simulation
				    			int cur_interval=(int)((cur_utc+trip_time_simulate-start_utc)/interval);
				    			if(cur_interval>max_interval){
				    				cur_interval=max_interval;
				    			}
				    			
				    			if(cur_interval!=pre_interval){
				    				time=time_list.get(cur_interval);
				    				pre_interval=cur_interval;
				    			}
				    			
				    			/*
				    			if(gid==31002){
				    				//System.out.println("here");
				    			}*/
				    			if(i==0 && gid==start_gid){
				    				if(i==roads.length-1 && gid==end_gid){
				    					//for time-based simulation;
				    					step_simulate = time[gid].get_traveltime(next_gid)*Math.abs(start_pos-end_pos);
				    				}
				    				else{
				    					step_simulate = time[gid].get_traveltime(start_pos, false, next_gid);
				    				}
				    			}
				    			else if(i==roads.length-1 && gid==end_gid){
				    				step_simulate = time[gid].get_traveltime(end_pos, true, next_gid);
				    			}
				    			else{
				    				step_simulate = time[gid].get_traveltime(next_gid);
				    			}
				    			//System.out.println(gid+",	"+trip_simulate+",	interval:"+(cur_interval+1));
				    			if(step_simulate>0){
				    				if(step_simulate>1000){
				    					System.out.println("here");
				    				}
		    						trip_time_simulate += step_simulate;
			    				}
		    					else{
			    					simulation_interupted=true;
			    					//interupt_type=2;
			    					break;
			    				}
				    		}
				    		if(!simulation_interupted){
				    			//System.out.println(cur_suid+","+rs.getLong("start_utc")+","+rs.getLong("end_utc")+","+trip_actual+","+ trip_time_simulate);
				    			
				    			Trip cur_time_trip=new Trip(cur_suid, rs.getLong("start_utc"), trip_time_simulate);
				    			time_trips.add(cur_time_trip);
				    		}
				    		else{
				    			//store to invalidate it in the database
				    			//invalid_suids.add(cur_suid);
				    			//invalid_utcs.add(rs.getLong("start_utc"));
				    			//invalid_types.add(interupt_type);
				    		}
				    	}
				    }
			    	
			    	//write the evaluation back to database;
			    	ArrayList<String> updates=new ArrayList<String>();
			    	for(int i=0; i<time_trips.size();i++){
			    		Trip cur_time_trip=time_trips.get(i);
			    		String newsql="update "+ trip_table +" set "+ evaluate_col +"="+cur_time_trip.cost+" where suid=" + cur_time_trip.suid +" and start_utc="+ cur_time_trip.start_utc+";";
			    		//System.out.println("["+i+"/"+time_trips.size()+"]:"+sql);
			    		updates.add(newsql);
			    		if(updates.size()>500){
			    			sql="";
	    					try{
					    		spt = con.setSavepoint("svpt1");
					    		for(int vi=0; vi<updates.size(); vi++){
	    	    		    		sql+=updates.get(vi);
	    	    		    	}
					    		//System.out.println("["+i+"/"+trips.size()+"]");
	    	    		    	stmt.executeUpdate(sql);
					    	}
					    	catch (SQLException e) {
					    		System.err.println(sql);
							    e.printStackTrace();
							    con.rollback(spt);
							}
							finally{
								con.commit();
								updates.clear();
							}	
			    		}
			    	}
			    	if(updates.size()>0){
						sql="";
						try{
				    		spt = con.setSavepoint("svpt1");
				    		for(int vi=0; vi<updates.size(); vi++){
		    		    		sql+=updates.get(vi);
		    		    	}
		    		    	stmt.executeUpdate(sql);
				    	}
				    	catch (SQLException e) {
				    		System.err.println(sql);
						    e.printStackTrace();
						    con.rollback(spt);
						}
						finally{
							con.commit();
							updates.clear();
						}	
		    		}
			    	
			    	/*
			    	for(int i=0;i<invalid_suids.size();i++){
			    		try{
				    		spt = con.setSavepoint("svpt1");
				    		sql="update "+ trip_table +" set valid="+invalid_types.get(i)*(-1)+" where suid=" +invalid_suids.get(i) +" and start_utc="+ invalid_utcs.get(i)+";";
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
			    	}*/
			    	
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
	
	public static void time_evaluation(String database, ArrayList<String> roadmap_tables, String aggregation_table, String static_roadmap, String trip_table, long start_utc, long interval, int min_trip_count){
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
		
		String simulate_col="timexiaoche_traffic_simulate";
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ trip_table +" add column actual_time_simulate double precision;";
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
	    		String sql="alter table "+ trip_table +" add column "+simulate_col+" double precision;";
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
			
			int interval_count=roadmap_tables.size();
			int max_interval=interval_count-1;
			long[] start_utcs=new long[interval_count];
			long[] end_utcs=new long[interval_count];
			ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
			AllocationRoadsegment[] static_roadlist=TripFinder.get_static_roadlist(database, static_roadmap);
			ArrayList<Double> rate_changes=new	ArrayList<Double>();
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="select *  from txrt_correction;";
	    		System.out.println(sql);
	    		rs=stmt.executeQuery(sql);
	    		while(rs.next()){
	    			rate_changes.add(rs.getDouble("pobrt_time"));
	    		}
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
			for(int i=0; i<roadmap_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				time_list.add(TripFinder.get_roadlist(database, roadmap_tables.get(i), true));
				System.out.println("Reading "+roadmap_tables.get(i)+"...");
				/*
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
				}*/
			}
			
			//change the schema of trip table 
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ trip_table +" add column valid integer;";
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
			
			//get the trips list and do the evaluation
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="select * from "+trip_table+" where suid in " +
	    				"(select suid from (select suid, count(*) from "+ trip_table +" group by suid ) as temp where count>="+ min_trip_count +") order by suid";
	    		//System.out.println(sql);
	    		rs=stmt.executeQuery(sql);
	    		
	    		long cur_suid=-1;
	    		//double total_actual=0;
	    		//double total_simulate=0;
	    		
	    		double trip_actual=0;
	    		double trip_time_simulate=0;
	    		double trip_traffic_simulate=0;	    		
	    		
	    		String route;
	    		int start_gid=-1;
	    		int end_gid=-1;
	    		double start_pos=-1.0;
	    		double end_pos=-1.0;
	    		
	    		ArrayList<Long> invalid_suids=new ArrayList<Long>();
	    		ArrayList<Long> invalid_utcs=new ArrayList<Long>();
	    		ArrayList<Integer> invalid_types=new ArrayList<Integer>();
	    		
	    		ArrayList<Trip> time_trips=new ArrayList<Trip>();
	    		ArrayList<Trip> traffic_trips=new ArrayList<Trip>();
	    		
		    	while(rs.next()){
		    		long suid=rs.getLong("suid");
		    		if(cur_suid!=-1 && suid!=cur_suid){
		    			//System.out.println(cur_suid+","+total_actual+","+ total_simulate+","+ (total_simulate-total_actual)*1.0/total_actual);
		    			cur_suid=suid;
		    			//total_actual=0;
		    			//total_simulate=0;
		    		}
		    		cur_suid=suid;
		    		trip_actual=0;
	    			trip_time_simulate=0;
	    			trip_traffic_simulate=0;
	    			
	    			int interupt_type=0;
	    			
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
			    	double traffic_edge_time=0.0;
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
			    			
			    			//traffic-estimation based simulation
			    			int cur_interval=(int)((cur_utc+trip_traffic_simulate-start_utc)/interval);
			    			if(cur_interval>max_interval){
			    				cur_interval=max_interval;
			    			}
			    			
			    			if(cur_interval!=pre_interval){
			    				time=time_list.get(cur_interval);
			    				pre_interval=cur_interval;
			    			}
			    			
			    			if(static_roadlist[gid].model_maxspeed>0 && time[gid].taxi_count>0){
			    				double saturation=(time[gid].taxi_count*4*rate_changes.get(cur_interval)/(time[gid].taxi_ratio*0.4/100))/static_roadlist[gid].model_capacity;
			    				traffic_edge_time = time[gid].length*1000/static_roadlist[gid].model_maxspeed*
			    						(1+static_roadlist[gid].model_alpha*Math.pow(saturation, static_roadlist[gid].model_beta));
			    				//trip_traffic_simulate+= step_simulate;
			    			}
			    			else{
			    				simulation_interupted=true;
			    				interupt_type=1;
			    				break;
			    			}
			    			
			    			
			    			//time-estimation based simulation
			    			cur_interval=(int)((cur_utc+trip_time_simulate-start_utc)/interval);
			    			if(cur_interval>max_interval){
			    				cur_interval=max_interval;
			    			}
			    			
			    			if(cur_interval!=pre_interval){
			    				time=time_list.get(cur_interval);
			    				pre_interval=cur_interval;
			    			}
			    			
			    			/*
			    			if(gid==31002){
			    				//System.out.println("here");
			    			}*/
			    			if(i==0 && gid==start_gid){
			    				if(i==roads.length-1 && gid==end_gid){
			    					
			    					//for traffic simulation;
			    					trip_traffic_simulate+= traffic_edge_time*Math.abs(start_pos-end_pos);
			    					
			    					//for time-based simulation;
			    					step_simulate = time[gid].get_traveltime(next_gid)*Math.abs(start_pos-end_pos);
			    					if(step_simulate>0){
				    					trip_time_simulate+= step_simulate;
				    				}
			    					else{
				    					simulation_interupted=true;
				    					interupt_type=2;
				    					break;
				    				}
			    				}
			    				else{
			    					
			    					trip_traffic_simulate+= time[gid].get_traveltime(start_pos, false, traffic_edge_time);

			    					step_simulate = time[gid].get_traveltime(start_pos, false, next_gid);
			    					if(step_simulate>0){
			    						trip_time_simulate+= step_simulate;
				    				}
			    					else{
				    					simulation_interupted=true;
				    					interupt_type=2;
				    					break;
				    				}
			    				}
			    			}
			    			else if(i==roads.length-1 && gid==end_gid){
			    				
			    				trip_traffic_simulate+= time[gid].get_traveltime(end_pos, true, traffic_edge_time);

			    				step_simulate = time[gid].get_traveltime(end_pos, true, next_gid);
			    				if(step_simulate>0){
			    					trip_time_simulate+= step_simulate;
			    				}
			    				else{
			    					simulation_interupted=true;
			    					interupt_type=2;
			    					break;
			    				}
			    			}
			    			else{
			    				trip_traffic_simulate+=traffic_edge_time;
			    				
			    				step_simulate = time[gid].get_traveltime(next_gid);
			    				if(step_simulate>0){
			    					trip_time_simulate+= step_simulate;
			    				}
			    				else{
			    					simulation_interupted=true;
			    					interupt_type=2;
			    					break;
			    				}
			    			}
			    			
			    			//System.out.println(gid+",	"+trip_simulate+",	interval:"+(cur_interval+1));
			    			
			    		}
			    		if(!simulation_interupted){
			    			//total_actual+=trip_actual;
			    			//total_simulate=trip_simulate;
			    			System.out.println(cur_suid+","+rs.getLong("start_utc")+","+rs.getLong("end_utc")+","+trip_actual+","+ trip_time_simulate+","
			    					+ trip_traffic_simulate);
			    			
			    			Trip cur_time_trip=new Trip(cur_suid, rs.getLong("start_utc"), trip_time_simulate);
			    			time_trips.add(cur_time_trip);
			    			Trip cur_traffic_trip=new Trip(cur_suid, rs.getLong("start_utc"), trip_traffic_simulate);
			    			traffic_trips.add(cur_traffic_trip);
			    		}
			    		else{
			    			//store to invalidate it in the database
			    			invalid_suids.add(cur_suid);
			    			invalid_utcs.add(rs.getLong("start_utc"));
			    			invalid_types.add(interupt_type);
			    		}
			    	}
			    }
		    	
		    	for(int i=0; i<time_trips.size();i++){
		    		Trip cur_time_trip=time_trips.get(i);
		    		Trip cur_traffic_trip=traffic_trips.get(i);
		    		try{
			    		spt = con.setSavepoint("svpt1");
			    		/*sql="update "+ trip_table +" set actual_time_simulate="+cur_time_trip.cost+", actual_traffic_simulate="+cur_traffic_trip.cost
			    				+" where suid=" + cur_time_trip.suid +" and start_utc="+ cur_time_trip.start_utc+";";*/
			    		
			    		sql="update "+ trip_table +" set "+simulate_col+"="+cur_traffic_trip.cost
			    				+" where suid=" + cur_time_trip.suid +" and start_utc="+ cur_time_trip.start_utc+";";
			    		System.out.println("["+i+"/"+time_trips.size()+"]:"+sql);
			    		if(stmt.executeUpdate(sql)!=1){
			    			System.err.println("Error: update time for multiple trips or zero trips !!!!!!");
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
		    	
		    	/*
		    	for(int i=0;i<invalid_suids.size();i++){
		    		
			    	try{
			    		spt = con.setSavepoint("svpt1");
			    		sql="update "+ trip_table +" set valid="+invalid_types.get(i)*(-1)+" where suid=" +invalid_suids.get(i) +" and start_utc="+ invalid_utcs.get(i)+";";
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
		    	}*/
		    	
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
				
				String type=args[1];
				String amplifier=args[0];
				
				String database="mydb";
				//String sample_table="labeled_gps";
				String roadmap_table="ring5_traffictime";
				String trip_table="ring5_all_trip";
				//int min_sample_thres=2;
				String static_roadmap="ring5_roads_initspeed";
				String footprint_table="ring5_footprint_pre";
				String secondary_footprint="nontrip_footprint";
				
				//double max_tempstop_precent=1;
				//int min_trip_interval=1;
				
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
				
				//footprint_table="all_footprint";
				footprint_table=footprint_table+"_"+amplifier+"_"+type;
				ArrayList<String> footprint_tables=new ArrayList<String>();
				footprint_tables.add(footprint_table);
				footprint_tables.add(secondary_footprint);
				
				String time_col=type+"_time_"+amplifier;
				String traffic_col=type+"_traffic_"+amplifier;
				double scale=1;
				
				int start_index=start_interval;
				TripFinder.traffictime_calculation(database, roadmap_tables, static_roadmap, footprint_tables, 
						time_col, traffic_col, scale, cur_utc, start_index, period);
				
				double r2_threshold=0.0;
				String route_col=type; 
				String evaluate_col=type+"_simulate_pre";
				trip_table=trip_table+"_"+amplifier;
				
				TripFinder.traffictime_evaluation(database, roadmap_tables, static_roadmap, trip_table, 
						route_col, evaluate_col, time_col, traffic_col, r2_threshold, cur_utc, period);
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else{
			try {
				String database="mydb";
				String sample_table="labeled_gps";
				String roadmap_table="ring5_traffictime";
				String trip_table="ring5_trip";
				int min_sample_thres=2;
				String static_roadmap="ring5_roads_initspeed";
				String footprint_table="ring5_footprint_trsp";
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
				
				int start_interval=2;
				cur_utc+=(start_interval-base_interval)*period;
				//ArrayList<String> time_tables=new ArrayList<String>();
				
				ArrayList<String> roadmap_tables=new ArrayList<String>();
				for(int i=start_interval; i<=54; i++){
					roadmap_tables.add(roadmap_table+""+i);
				}
				
				//TripFinder.time_evaluation(database, roadmap_tables, roadmap_table, static_roadmap, all_trip_table, cur_utc, period, 1);
				/*secondary_footprint="all_footprint";
				String trip_condition=" and ( valid is null and valid >=0 ) ";
				TripFinder.nontrip_footprint("mydb", roadmap_tables, sample_table, all_trip_table, secondary_footprint, trip_condition, cur_utc, period);
				*/
				/*
				//footprint_table="all_footprint";
				ArrayList<String> footprint_tables=new ArrayList<String>();
				footprint_tables.add(footprint_table);
				footprint_tables.add(secondary_footprint);
				
				String time_col="footprint_time";
				String traffic_col="footprint_traffic";
				double scale=1;
				int start_index=start_interval;
				
				TripFinder.traffictime_calculation(database, roadmap_tables, static_roadmap, footprint_tables, 
						time_col, traffic_col, scale, cur_utc, start_index, period);
				
				double r2_threshold=0.0;
				String route_col="eebksp_considerall"; 
				String evaluate_col="eebksp_considerall_simulate_pre";
				//TripFinder.traffictime_evaluation(database, roadmap_tables, static_roadmap, all_trip_table, 
						//route_col, evaluate_col, time_col, traffic_col, r2_threshold, cur_utc, period);
				
				
				footprint_table="ring5_footprint_pre_trsp";
				footprint_tables=new ArrayList<String>();
				footprint_tables.add(footprint_table);
				footprint_tables.add(secondary_footprint);
				
				scale=1;
				time_col="trsp_time_pre";
				traffic_col="trsp_traffic_pre";
				//TripFinder.traffictime_calculation(database, roadmap_tables, static_roadmap, footprint_tables, 
						//time_col, traffic_col, scale, cur_utc, start_index, period);
				
				r2_threshold=0.0;
				route_col="trsp"; 
				evaluate_col="trsp_simulate_pre";
				//TripFinder.traffictime_evaluation(database, roadmap_tables, static_roadmap, all_trip_table, 
						//route_col, evaluate_col, time_col, traffic_col, r2_threshold, cur_utc, period);
				*/
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}