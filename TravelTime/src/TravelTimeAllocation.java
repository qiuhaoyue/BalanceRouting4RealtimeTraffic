/** 
 * 2016年3月12日 
 * TravelTimeAllocation.java 
 * author:ZhangYu
 */

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
 
public class TravelTimeAllocation {
	
	//从sample_table中获取suid属性存入taxi_list
	public static void get_suids(String database, String sample_table, ArrayList<String> taxi_list){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    String sql="select suid from " + sample_table +" group by suid";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    if(taxi_list==null){
		    	return;
		    }
		    taxi_list.clear();
		    while(rs.next()){
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
		    Common.dropConnection(con);
		}
		System.out.println("get_suids finished!");
	}
	
	//将time_table中的路段记录转换为AllocationRoadsegment类
	public static AllocationRoadsegment[] get_roadlist(String database, String time_table, boolean has_nextgid){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		int max_gid=0;
		AllocationRoadsegment[] roadlist=null;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return null;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    String sql="";
			//获取最大gid值
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
		    System.out.println(sql);
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
			    System.out.println(sql);
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
		    Common.dropConnection(con);
		}
		System.out.println("get_roadlist finished!");
		return roadlist;
	}
	
	public static void preprocess_intervals(String database, String sample_table,  ArrayList<String> taxi_list){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;
		DecimalFormat df=new DecimalFormat();
		df.setMaximumFractionDigits(6);
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+sample_table+" add column interval double precision;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+sample_table+" add column pre_gid integer;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+sample_table+" add column pre_offset double precision;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		
		for(int i=0; i<taxi_list.size(); i++){
			String cur_suid=taxi_list.get(i);
			//System.out.println("-------preprocess_intervals:	"+i+"	/	"+taxi_list.size());
			ArrayList<Sample> samplelist=new ArrayList<Sample>();
			try{
	    		//spt = con.setSavepoint("svpt1");
	    		String sql="select * from "+ sample_table +" where suid="+cur_suid+" and (ostdesc not like '%定位无效%') order by utc;";
	    		//System.out.println(sql);
	    		rs = stmt.executeQuery(sql);
	    		
			    while(rs.next()){
			    	double offset=rs.getDouble("edge_offset");
			    	Sample cur_sample=new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getInt("gid"), offset, rs.getString("route"), rs.getInt("stop"));
			    	samplelist.add(cur_sample);
				}
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			}
			
			Sample cur_sample=null;
			Sample pre_sample=null;
			ArrayList<String> updates=new ArrayList<String>();
			for(int j=0;j<samplelist.size(); j++){
				try{
					pre_sample=cur_sample;
					cur_sample=samplelist.get(j);
					
					if(pre_sample==null || pre_sample.suid!=cur_sample.suid || cur_sample.stop==1 || pre_sample.stop==1){
						continue;
					}
					else{
						long cur_utc=cur_sample.utc.getTime()/1000;
						cur_sample.interval=cur_utc-pre_sample.utc.getTime()/1000;
						if(cur_sample.interval<=0){
							continue;
						}
						try{
					    	spt = con.setSavepoint("svpt1");
					    	String offset=df.format(pre_sample.offset);
					    	String newsql="UPDATE "+ sample_table +" SET interval=" + cur_sample.interval +", pre_gid=" + pre_sample.gid +", pre_offset=" + offset
					    			+ " WHERE suid="+ cur_sample.suid + " and utc=" +cur_utc +"; \n";
							//System.out.println("["+j+"/"+samplelist.size()+"]"+sql);
							//stmt.executeUpdate(sql);
					    	updates.add(newsql);
					    	if(updates.size()>500){
				    			String sql="";
				    			try{
				    				for(int vi=0; vi<updates.size(); vi++){
				    					//sql+=updates.get(vi);
										stmt.addBatch(updates.get(vi));
				    				}
				    				//System.out.println(sql);
				    				//Savepoint spt3 = con.setSavepoint("svpt3");
				    				//stmt.executeUpdate(sql);
									stmt.executeBatch();
				    			}
						    	catch (SQLException e) {
						    		System.err.println(sql);
								    e.printStackTrace();
								    con.rollback();
								}
								finally{
									con.commit();
									updates.clear();
								}	
				    		}
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
				catch (SQLException e) {
				    e.printStackTrace();
				}
				catch (Exception e) {
				    e.printStackTrace();
				}
			}
			try{
				if(updates.size()>0){
	    			String sql="";
					try{
			    		for(int vi=0; vi<updates.size(); vi++){
	    		    		//sql+=updates.get(vi);
							stmt.addBatch(updates.get(vi));
	    		    	}
			    		//System.out.println(sql);
	    		    	//stmt.executeUpdate(sql);
						stmt.executeBatch();
			    	}
			    	catch (SQLException e) {
			    		System.err.println(sql);
					    e.printStackTrace();
					    con.rollback();
					}
					finally{
						con.commit();
						updates.clear();
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
		Common.dropConnection(con);
		System.out.println("preprocess_interval finished!");
	}
	
	public static void filter_roads(String database, String whole_map, String filtered_map, long min_x_lon, long max_x_lon, long min_y_lon, long max_y_lon){
		
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;
		
		double min_x=min_x_lon/100000.0;
		double max_x=max_x_lon/100000.0;
		double min_y=min_y_lon/100000.0;
		double max_y=max_y_lon/100000.0;

		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ filtered_map +";";
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
		    	String sql="create table "+ filtered_map +" as select * from "+ whole_map +" where " +
		    			" x1>="+min_x+" and x1<="+max_x+" and y1>="+min_y+" and y1<="+max_y+
		    			" and x2>="+min_x+" and x2<="+max_x+" and y2>="+min_y+" and y2<="+max_y+";";
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
	    		String sql="CREATE INDEX "+filtered_map+"_gid_idx ON "+ filtered_map +"(gid);";
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
	
	public static void allocate_time(String database, AllocationRoadsegment[] roadlist, String sample_table, String result_table/*, ArrayList<String> taxi_list*/, long start_utc, long end_utc){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;

		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ result_table +";";
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
	    		String sql="create table "+ result_table +"(seq bigint, gid integer, next_gid integer, time double precision, percent double precision, " +
	    				"interval double precision, tmstp bigint, suid bigint, utc bigint, start_pos double precision);";
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
		
		//for(int i=0; i<taxi_list.size(); i++){
			//String cur_suid=taxi_list.get(i);
			ArrayList<Sample> samplelist=new ArrayList<Sample>();
			
			try{
	    		//spt = con.setSavepoint("svpt1");
	    		String sql="select * from "+ sample_table +/*" where suid="+ cur_suid+ " and " + */
	    				" where utc>="+start_utc+ " and utc<"+end_utc+ " and (gid is not null) and (stop is null or stop!=1) and (ostdesc not like '%定位无效%') "+
	    				" order by suid, utc ";
	    		System.out.println(sql);
	    		rs = stmt.executeQuery(sql);
	    		
			    while(rs.next()){
			    	Sample cur_sample=new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getInt("gid"), rs.getDouble("edge_offset"),
			    			rs.getString("route"), rs.getInt("stop"), Math.round(rs.getDouble("interval")));
			    	cur_sample.pre_gid=rs.getInt("pre_gid");
			    	cur_sample.pre_offset=rs.getDouble("pre_offset");
			    	samplelist.add(cur_sample);
				}
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			}
			
			Sample cur_sample=null; //samplelist.get(0);
			//Sample pre_sample=null;
			long seq=0;
			long pre_suid=-1;
			long pre_interval=-1;
			
			ArrayList<String> updates=new ArrayList<String>();
			for(int j=0;j<samplelist.size(); j++){
				//System.out.println("-----------Allocating: ["+j+"/"+samplelist.size()+"]--------------");
				cur_sample=samplelist.get(j);
				try{
					if(cur_sample.stop==1 || (cur_sample.stop==2 && cur_sample.suid!=pre_suid)){
						continue;
					}
					else{
						long cur_utc=cur_sample.utc.getTime()/1000;
						long interval = cur_sample.interval;
						int pre_gid=cur_sample.pre_gid;
						double pre_offset=cur_sample.pre_offset;
						int origin_gid=0;
						boolean changed_gid=false;
						//update no need to do time_allocation
						if( cur_sample.stop==2 || (cur_sample.gid== pre_gid && cur_sample.suid==pre_suid && (cur_sample.interval<=10 || Math.abs(cur_sample.pre_offset-cur_sample.offset)<=0.1))){
							if(cur_sample.stop==2 && cur_sample.gid!= pre_gid ){
								//special handling for duplicated record;
								if((j+1<samplelist.size()) && cur_sample.suid==samplelist.get(j+1).suid && cur_sample.utc.getTime()==samplelist.get(j+1).utc.getTime()){
									j++;
									System.out.println("Duplicated["+cur_sample.suid+","+cur_utc+"]: temperary stop happens among multiple road segments!");
									continue;
								}
								System.out.println("Exception["+cur_sample.suid+","+cur_utc+"]: temperary stop happens among multiple road segments!");
								
								origin_gid=cur_sample.gid;
								cur_sample.gid=pre_gid;
								cur_sample.offset=pre_offset;
								changed_gid=true;
								/*
								if(pre_gid<roadlist.length && roadlist[pre_gid].to_cost>=0 && roadlist[pre_gid].to_cost < RoadCostUpdater.inconnectivity-1){
									cur_sample.offset=1.0;
								}
								else if(pre_gid<roadlist.length && roadlist[pre_gid].reverse_cost>=0 && roadlist[pre_gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ // negative road
									cur_sample.offset=0.0;
								}
								else{
									//alien_road=true;
									continue;
								}*/
								
								if((j+1<samplelist.size()) && samplelist.get(j+1).suid==cur_sample.suid){
									samplelist.get(j+1).pre_gid=cur_sample.gid;
									samplelist.get(j+1).pre_offset=cur_sample.offset;
								}
							}
							
							double added_coverage=0;
							if(pre_gid<roadlist.length && roadlist[pre_gid].to_cost>=0 && roadlist[pre_gid].to_cost < RoadCostUpdater.inconnectivity-1){ //positive road
								if(cur_sample.stop==2 && cur_sample.offset<pre_offset){
									cur_sample.offset=pre_offset;
								}
								added_coverage=cur_sample.offset-pre_offset;
							}
							else if(pre_gid<roadlist.length && roadlist[pre_gid].reverse_cost>=0 && roadlist[pre_gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ // negative road
								if(cur_sample.stop==2 && cur_sample.offset>pre_offset){
									cur_sample.offset=pre_offset;
								}
								added_coverage=pre_offset-cur_sample.offset;
							}
							else{
								//alien_road=true;
								continue;
							}
							
							if(added_coverage>=0){
								
								//make sure the insert has been handled;
								if(updates.size()>0){
									String sql="Insert into "+ result_table +" (seq, gid, next_gid, time, percent, interval, tmstp, suid, utc, start_pos) values \n";
									try{
							    		for(int vi=0; vi<updates.size(); vi++){
											
							    			if(vi!=0){
					    		    			sql+=", \n";
					    		    		}
					    		    		sql+=updates.get(vi);
											//stmt.addBatch(updates.get(vi));
					    		    	}
							    		System.out.println(sql);
					    		    	stmt.executeUpdate(sql);
										//stmt.executeBatch();
							    	}
							    	catch (SQLException e) {
							    		System.err.println(sql);
									    e.printStackTrace();
										if (e instanceof BatchUpdateException)
                                		{
                                    		BatchUpdateException bex = (BatchUpdateException) e;
                                    		bex.getNextException().printStackTrace(System.err);
                                		}	
									    con.rollback();
									}
									finally{
										con.commit();
										updates.clear();
									}	
					    		}
								
								//then update
								try{
									if(interval<pre_interval){
										interval=pre_interval;
									}
						    		spt = con.setSavepoint("svpt1");
						    		String sql="";
						    		if(changed_gid){
							    		sql="UPDATE "+ result_table +" SET time=time+" + cur_sample.interval + ", percent=percent+"+ added_coverage + ", next_gid="+ origin_gid
							    				+ ", interval="+ interval +" WHERE gid="+ cur_sample.gid + " and seq=" +seq ;
							    	}
						    		else{
						    			sql="UPDATE "+ result_table +" SET time=time+" + cur_sample.interval + ", percent=percent+"+ added_coverage
							    				+ ", interval="+ interval +" WHERE gid="+ cur_sample.gid + " and seq=" +seq ;
						    		}
								    System.out.println(sql);
								    int re_value=stmt.executeUpdate(sql);
								    if(re_value!=1){
								    	System.err.println("Error: Abnormal Update Behavior! return_value=" +re_value +"	"+sql);
								    }
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
								continue;
							}
						}
						
						//insert new record to result_table
						//else{
							if(cur_sample.route==null || cur_sample.route.equals("")){
								continue;
							}
							/*if(cur_sample.utc.getTime()/1000==1231233148L && cur_sample.suid==4231){
								System.out.println();
							}
							if(cur_sample.suid==2476 && cur_sample.utc.getTime()/1000==1231233039){
								System.out.println("here");
							}*/
							String[] route_gids=cur_sample.route.split(",");
							ArrayList<Integer> gidlist=new ArrayList<Integer>();
							ArrayList<Double> timelist=new ArrayList<Double>();
							ArrayList<Double> coverlist=new ArrayList<Double>();
							ArrayList<Double> startposlist=new ArrayList<Double>();
							double total_time=0;
							boolean alien_road=false;
							for(int k=0;k<route_gids.length;k++){
								double coverage=-1.0;
								double startpos=-1.0;
								try{
									int gid=Integer.parseInt(route_gids[k]);
									if(gid<0){
										break;
									}
									//System.out.println("gid:"+gid);
									if( k==0 && gid == cur_sample.pre_gid){
										startpos = pre_offset;
										if( gid != cur_sample.gid){
											if(gid<roadlist.length && roadlist[gid].reverse_cost>=0 && roadlist[gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ //negative road
												//coverage = 1-pre_offset;
												coverage = pre_offset;
											}
											else if(gid<roadlist.length && roadlist[gid].to_cost>=0 && roadlist[gid].to_cost < RoadCostUpdater.inconnectivity-1){ // positive road
												//coverage = pre_offset;
												coverage = 1-pre_offset;
											}
											else{
												alien_road=true;
												break;
											}
										}
										else{
											if(gid<roadlist.length && roadlist[gid].reverse_cost>=0 && roadlist[gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ //negative road
												coverage= pre_offset-cur_sample.offset;
											}
											else if(gid<roadlist.length && roadlist[gid].to_cost>=0 && roadlist[gid].to_cost < RoadCostUpdater.inconnectivity-1){ // positive road
												coverage= cur_sample.offset-pre_offset;
											}
											else{
												alien_road=true;
												break;
											}
										}
									}
									else if(k==route_gids.length-1 && gid==cur_sample.gid){
										if(gid<roadlist.length && roadlist[gid].reverse_cost>=0 && roadlist[gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ //negative road
											startpos = 1.0;
											coverage= 1-cur_sample.offset;
										}
										else if(gid<roadlist.length && roadlist[gid].to_cost>=0 && roadlist[gid].to_cost < RoadCostUpdater.inconnectivity-1){ // positive road
											startpos = 0.0;
											coverage= cur_sample.offset;
										}
										else{
											alien_road=true;
											break;
										}
									}
									else{
										coverage=1.0;
										if( gid>=roadlist.length || (roadlist[gid].reverse_cost< 0 && roadlist[gid].to_cost< 0)){
											alien_road=true;
											break;
										}
										else{
											if(roadlist[gid].reverse_cost>=0 && roadlist[gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ //negative road
												startpos = 1.0;
											}
											else if(roadlist[gid].to_cost>=0 && roadlist[gid].to_cost < RoadCostUpdater.inconnectivity-1){ // positive road
												startpos = 0.0;
											}
										}
									}
									
									gidlist.add(gid);
									startposlist.add(startpos);
									coverlist.add(coverage);
									int next_gid=-1;
									if(k+1<route_gids.length){
										next_gid=Integer.parseInt(route_gids[k+1]);
									}
									total_time+=coverage*roadlist[gid].length/roadlist[gid].get_speed(next_gid);
									timelist.add(total_time);
								}
								catch(Exception e){
									e.printStackTrace();
									//break;
								}
							}
							if(alien_road){
								continue;
							}
							
							long start_time=cur_sample.utc.getTime()/1000-cur_sample.interval;
							//String sql="Insert into "+ result_table +" (seq, gid, next_gid, time, percent, interval, tmstp, suid, utc, start_pos) values \n";
							for(int k=0; k<gidlist.size();k++){
								int gid=gidlist.get(k);
								double coverage=coverlist.get(k);
								double start_pos=startposlist.get(k);
								/*
								if(cur_sample.suid==2234 && cur_sample.utc.getTime()/1000==1231233049){
									System.out.println("here");
								}
								*/
								int next_gid=-1;
								if(k+1< gidlist.size()){
									next_gid=gidlist.get(k+1);
								}
								double travel_time=cur_sample.interval*roadlist[gid].length/roadlist[gid].get_speed(next_gid)*coverage/total_time;
								long tmsp= start_time + Math.round(timelist.get(k)-travel_time);
								
								//if(cur_sample.suid==4232){
									//System.out.println("here");
								//}
						    	
						    	seq++;
						    	String newsql="";
						    	if(k<gidlist.size()-1){
							    	newsql=" ("+seq+", "+gid+", "+gidlist.get(k+1)+", "+travel_time+", "+coverage+", "+cur_sample.interval+", "+tmsp+", "+cur_sample.suid+", "+cur_sample.utc.getTime()/1000+", "+start_pos+") ";
						    	}
						    	else{
						    		newsql=" ("+seq+", "+gid+", NULL , "+travel_time+", "+coverage+", "+cur_sample.interval+", "+tmsp+", "+cur_sample.suid+", "+cur_sample.utc.getTime()/1000+", "+start_pos+") ";
						    	}
						    	updates.add(newsql);
						    	pre_suid=cur_sample.suid;
						    	
						    	if(updates.size()>200){
									String sql="Insert into "+ result_table +" (seq, gid, next_gid, time, percent, interval, tmstp, suid, utc, start_pos) values \n";
									try{
							    		for(int vi=0; vi<updates.size(); vi++){
											
					    		    		if(vi!=0){
					    		    			sql+=", \n";
					    		    		}
							    			sql+=updates.get(vi);
											//stmt.addBatch(updates.get(vi));
					    		    	}
							    		//System.out.println("["+i+"/"+trips.size()+"]");
							    		System.out.println(sql);
					    		    	stmt.executeUpdate(sql);
										//stmt.executeBatch();
							    	}
							    	catch (SQLException e) {
							    		System.err.println(sql);
									    e.printStackTrace();
										if (e instanceof BatchUpdateException)
                               		 	{
                                    		BatchUpdateException bex = (BatchUpdateException) e;
                                    		bex.getNextException().printStackTrace(System.err);
                                		}
									    con.rollback();
									}
									finally{
										con.commit();
										updates.clear();
									}	
					    		}
							}
							/*if(gidlist.size()>0){
								try{
									System.out.println(sql);
									//spt = con.setSavepoint("svpt1");
									stmt.executeUpdate(sql);
						    	}
						    	catch (SQLException e) {
								    e.printStackTrace();
								    con.rollback();
								}
								catch (Exception e) {
								    e.printStackTrace();
								}
								finally {
									con.commit();
								}
							}*/
					}
				}
				catch (SQLException e) {
				    e.printStackTrace();
				}
				catch (Exception e) {
				    e.printStackTrace();
				}
			}
			try{
				if(updates.size()>0){
					String sql="Insert into "+ result_table +" (seq, gid, next_gid, time, percent, interval, tmstp, suid, utc, start_pos) values \n";
					try{
			    		for(int vi=0; vi<updates.size(); vi++){
							/*
	    		    		if(vi!=0){
	    		    			sql+=", \n";
	    		    		}
			    			sql+=updates.get(vi);*/
							stmt.addBatch(updates.get(vi));
	    		    	}
			    		//System.out.println("["+i+"/"+trips.size()+"]");
			    		//System.out.println(sql);
	    		    	//stmt.executeUpdate(sql);
						stmt.executeBatch();
			    	}
			    	catch (SQLException e) {
			    		System.err.println(sql);
					    e.printStackTrace();
					    con.rollback();
					}
					finally{
						con.commit();
						updates.clear();
					}	
	    		}
			}
			catch (SQLException e) {
			    e.printStackTrace();
			}
			catch (Exception e) {
			    e.printStackTrace();
			}
		
		Common.dropConnection(con);
		System.out.println("update_speed finished!");
	}
	
	public static void aggregate_time(String database, String roadmap_table, String allocation_table, String aggregation_table, long start_utc, long end_utc){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;

		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ aggregation_table +";";
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
			
			//delete those having no specific turnings
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="delete from "+ aggregation_table + " where next_gid is null;";
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
		Common.dropConnection(con);
		System.out.println("aggregate_time finished!");
	}
}


