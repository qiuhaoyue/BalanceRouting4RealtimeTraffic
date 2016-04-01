/** 
 * 2016年3月12日 
 * RoadInitState.java 
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

public class RoadInitState {
	public static void get_gids(String database, String roadmap_table, ArrayList<String> road_list){
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
		    String sql="select gid from " + roadmap_table + " order by gid";

		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    if(road_list==null){
		    	return;
		    }
		    road_list.clear();
		    while(rs.next()){
		    	road_list.add(rs.getString("gid"));
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
		System.out.println("get_gids finished!");
	}
	
	public static void passStat(String database, /*String road, */String sample_table, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		int reference_count=0;
		Savepoint spt=null;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {

		    stmt = con.createStatement();
		    /*String sql="select count(*) from " + sample_table + " where route like '%,"+road+",%' or route like '"+road+",%' and (ostdesc not like '%定位无效%') ;";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);

		    reference_count=0;
		    while(rs.next()){
		    	reference_count=rs.getInt("count");
		    }*/
		    /*
		    String sql="select speed from " + sample_table + " where gid="+road+" and (stop is null or stop!=1) and (ostdesc not like '%定位无效%') order by speed desc";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);*/
		    
		    
		    try{
			    String sql="create table "+ "init_speed_"+sample_table +" as select gid, count(*) as count, avg(speed)/100 as avg_speed, max(speed)/100 as max_speed from " + sample_table + " where (gid is not null) and (stop is null or stop!=1) and (ostdesc not like '%定位无效%') and speed>0 group by gid";
			    System.out.println(sql);
			    rs = stmt.executeQuery(sql);
		    }
		    catch (Exception e) {
				e.printStackTrace();
			}
			finally{
			   con.commit();
			}
		    
		    try{
			    String sql="create index "+ "init_speed_"+sample_table +"_gid_idx on "+ "init_speed_"+sample_table +"(gid);";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
		    }
		    catch (Exception e) {
				e.printStackTrace();
			}
			finally{
			   con.commit();
			}
		    
		    /*
		    ArrayList<Double> speed_list=new ArrayList<Double>();
		    double everage_speed=0;
		    double max_speed=0;
		    double count=0;
		    while(rs.next()){
		    	speed_list.add((double)rs.getInt("speed")/100.0);
		    }
		    for(int i=0; i<speed_list.size(); i++){
		    	if(speed_list.get(i)>0){
		    		everage_speed+=speed_list.get(i);
		    		count++;
		    	}
		    }
		    if(count==0){
		    	everage_speed=0;
		    }
		    else{
		    	everage_speed/=count;
		    }
		    if(speed_list.size()>0){
		    	max_speed=speed_list.get(0);
		    }*/

		    try{
			    String sql="UPDATE "+roadmap_table +" SET reference=0, average_speed=0, max_speed=0;";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			finally{
			   con.commit();
			}
		    
			try{
			    String sql="UPDATE "+roadmap_table +" SET reference="+"init_speed_"+sample_table+".count, average_speed="+"init_speed_"+sample_table+".avg_speed, max_speed="+"init_speed_"+sample_table+".max_speed" +
			    		" from init_speed_"+sample_table+" WHERE "+roadmap_table +".gid="+"init_speed_"+sample_table+".gid;";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
			}
			catch (Exception e) {
				e.printStackTrace();
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
		System.out.println("passStat!");
	}
	

	public static void create_index_on_samples(String database, String sample_table){
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {

		    stmt = con.createStatement();
			try{
				spt = con.setSavepoint("svpt3");
				String sql="DROP INDEX "+sample_table+"_gid_idx;";
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
				String sql="CREATE INDEX "+sample_table+"_gid_idx ON "+ sample_table +"(gid);";
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
		finally {
		    Common.dropConnection(con);
		}
		System.out.println("passStat!");
	}
	
	public static void change_scheme(String database, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {

		    stmt = con.createStatement();
		    try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="ALTER TABLE "+ roadmap_table +" DROP COLUMN reference;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" ADD COLUMN reference integer;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" DROP COLUMN average_speed;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" ADD COLUMN average_speed double precision;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" DROP COLUMN max_speed;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" ADD COLUMN max_speed double precision;";

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
		finally {
		    Common.dropConnection(con);
		}
		System.out.println("passStat!");
	}

	//获取某一级别的道路的速度信息
	public static void get_class2speed(String database, String roadmap_table, HashMap<Integer, RoadClass> hmp_class2speed){
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
		    String sql="select class_id, count(*), avg(max_speed) as avg_max, stddev(max_speed) as dev_max, " +
		    		"avg(average_speed) as avg_avg, stddev(average_speed) as dev_avg from " + roadmap_table + 
		    		" where max_speed>0 and average_speed >0 group by class_id order by class_id";
		    
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    if(hmp_class2speed==null){
		    	return;
		    }
		    hmp_class2speed.clear();
		    while(rs.next()){
		    	hmp_class2speed.put(rs.getInt("class_id"), new RoadClass(rs.getDouble("avg_avg"),rs.getDouble("dev_avg"), rs.getDouble("avg_max"),rs.getDouble("dev_max"), rs.getLong("count")));
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
		System.out.println("get_gids finished!");
	}

	//根据同级别道路的速度值为没有速度信息的道路赋值,并为速度值设立上,下限
	public static void improve_speed(String database, String roadmap_table, String new_roadmap, HashMap<Integer, RoadClass> hmp_class2speed){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList<RoadSegment> roadmap=new ArrayList<RoadSegment>();
		Savepoint spt=null;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    if(new_roadmap!=null){
		    	try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="drop table "+new_roadmap;
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
		    	try{
		    		spt = con.setSavepoint("svpt1");
			    	String sql="create table "+new_roadmap+" as select * from " + roadmap_table;
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
		    	try{
		    		spt = con.setSavepoint("svpt1");
			    	String sql="create index "+new_roadmap+"_gid_index on " + new_roadmap +"(gid);";
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
		    String sql="select * from " + new_roadmap;
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    while(rs.next()){
		    	RoadSegment road=new RoadSegment(rs.getInt("gid"), rs.getDouble("to_cost"), rs.getDouble("reverse_cost"), rs.getDouble("length"), rs.getInt("class_id"));
		    	road.avg_inst_speed=rs.getDouble("average_speed");
		    	road.max_inst_speed=rs.getDouble("max_speed");
		    	road.reference=rs.getInt("reference");
		    	roadmap.add(road);
			}
		    
		    for(int i =0; i<roadmap.size(); i++){
		    	boolean default_update=false;
		    	boolean specific_update=false;
		    	
		    	RoadSegment cur_road=roadmap.get(i);
		    	if (!hmp_class2speed.containsKey(cur_road.class_id)){
		    		System.out.println("class id" + cur_road.class_id + " info not found.");
		    		continue;
		    	}
		    	double class_avg_speed=hmp_class2speed.get(cur_road.class_id).avg_speed;
		    	double class_dev_avg=hmp_class2speed.get(cur_road.class_id).dev_avg;
		    	if(cur_road.avg_inst_speed==0){
		    		cur_road.avg_inst_speed=class_avg_speed;
		    		default_update=true;
		    	}
		    	else{
		    		if(cur_road.avg_inst_speed>class_avg_speed+class_dev_avg){
		    			cur_road.avg_inst_speed=class_avg_speed+class_dev_avg;
		    			specific_update=true;
		    		}
		    		else if(cur_road.avg_inst_speed<class_avg_speed-class_dev_avg) {
		    			cur_road.avg_inst_speed=class_avg_speed-class_dev_avg;
		    			specific_update=true;
		    		}	
		    	}
		    	
		    	double class_max_speed=hmp_class2speed.get(cur_road.class_id).max_speed;
		    	double class_dev_max=hmp_class2speed.get(cur_road.class_id).dev_max;
		    	if(cur_road.max_inst_speed==0){
		    		cur_road.max_inst_speed=class_max_speed;
		    		default_update=true;
		    	}
		    	else{
		    		if(cur_road.max_inst_speed>class_max_speed+class_dev_max){
		    			cur_road.max_inst_speed=class_max_speed+class_dev_max;
		    			specific_update=true;
		    		}
		    		else if(cur_road.avg_inst_speed<class_max_speed-class_dev_max) {
		    			cur_road.max_inst_speed=class_max_speed-class_dev_max;
		    			specific_update=true;
		    		}	
		    	}
		    	
		    	if(!specific_update && !default_update){
		    		continue;
		    	}
		    	
		    	try{
		    		spt = con.setSavepoint("svpt1");
		    		sql="UPDATE "+ new_roadmap +" SET max_speed="+cur_road.max_inst_speed+ ", average_speed="+cur_road.avg_inst_speed +" WHERE gid="+ cur_road.gid;
		    		/*
		    		String reason="";
		    		if(specific_update){
		    			reason+="specific_update";
		    		}
		    		else{
		    			reason+="default_update";
		    		}*/
				    //System.out.println("improve_speed:["+i+"/"+roadmap.size()+"]"+ sql + " cur_road.class_id="+cur_road.class_id);
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
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    Common.dropConnection(con);
		}
		System.out.println("update_speed finished!");
	}
}
