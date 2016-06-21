package indi.zyu.realtraffic.common;

import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.process.TaxiInfo;
import indi.zyu.realtraffic.road.AllocationRoadsegment;
import indi.zyu.realtraffic.updater.GPSUpdater;
import indi.zyu.realtraffic.updater.RealTrafficUpdater;
import indi.zyu.realtraffic.updater.TravelTimeSliceUpdater;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.bmwcarit.barefoot.matcher.Matcher;
import com.bmwcarit.barefoot.road.PostGISReader;
import com.bmwcarit.barefoot.roadmap.Road;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.bmwcarit.barefoot.roadmap.RoadPoint;
import com.bmwcarit.barefoot.roadmap.TimePriority;
import com.bmwcarit.barefoot.spatial.Geography;
import com.bmwcarit.barefoot.topology.Dijkstra;
import com.bmwcarit.barefoot.util.Tuple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/** 
 * 2016年3月11日 
 * Common.java 
 * author:ZhangYu
 */

public class Common {
	
	//some config on database
	static String JdbcUrl  = "jdbc:postgresql://localhost:5432/";
	
	//static String Host     = "166.111.68.197";
	static String Host     = "localhost";
	//static int Port        = 11204;
	static int Port        = 5432;
	
	static String UserName = "secret";
	static String UserPwd  = "secret";
	
	static String DataBase = "taxi_data";
	
	public static String OriginWayTable = "ways";
	static String OneWayTable    = "oneway_test";
	static String OriginSampleTable = "gps";
	//static String ValidSampleTable  = "valid_gps_test2";
	public static String ValidSampleTable  = "valid_gps_utc";
	static String SingleSampleTable  = "gps_5434";
	//static String FilterSampleTable = "gps_ring2";
	static String FilterSampleTable = ValidSampleTable;
	//static String IntersectionTable = "oneway_intersection";
	public static String traffic_slice_table ="travel_time_slice_";
	public static String real_road_slice_table ="real_road_time_slice_";
	public static String real_turning_slice_table ="real_turning_time_slice_";
	public static String traffic_total_table ="travel_time_total_";
	
	static String UnKnownSampleTable = "match_fail_gps";
	
	//to create table with date
	public static String Date_Suffix = "";
	
	public static double max_speed = 33.33;
	public static double min_speed = 0.1;
	static double min_interval = 20;
	static double speed_alpha = 0.9;
	public static double init_turning_time = 5;
	
	public static int match_windows_size = 4;
	
	public static long period = 900L;
	public static long end_utc=1270569600L;//1231218000-1231221600
	public static long start_utc=1270483200L;
	public static long max_seg;
	
	public static TaxiInfo taxi[] = null;//taxi sample
	public static ExecutorService ThreadPool = null;//thread pool
	
	public static AllocationRoadsegment[] roadlist=null;
	
	public static Logger logger = LogManager.getLogger(Common.class.getName());
	
	public static RoadMap map = null;
	public static Matcher matcher;
	
	static GPSUpdater gps_updater;
	public static GPSUpdater unkown_gps_updater;//points that match failed
	static TravelTimeSliceUpdater traffic_updater;
	public static RealTrafficUpdater real_traffic_updater;
	
	//config Mapping of road class identifiers to priority factor and default maximum speed
	public static Map<Short, Tuple<Double, Integer>> road_config;
	
	//for debug calculate time
	public static float match_time = 0;
	public static float estimate_road_time = 0;
	public static int estimate_road_counter = 0;
	public static float estimate_turning_time = 0;
	public static int estimate_turning_counter = 0;
	public static float preprocess_time = 0;
		
	//initialize params
	public static void init(int max_suid, int pool_size){	
		try{
			//init road config
			init_road_config();
			map = RoadMap.Load(new PostGISReader(Common.Host, Common.Port, 
					DataBase, OriginWayTable, UserName, UserPwd, road_config));
			map.construct();
			matcher = new Matcher(Common.map, new Dijkstra<Road, RoadPoint>(),
		            new TimePriority(), new Geography());
			
			max_seg=(Common.end_utc-Common.start_utc)/Common.period;//96
			
			gps_updater = new GPSUpdater(100, "gps_final" + Date_Suffix);
			unkown_gps_updater = new GPSUpdater(100, UnKnownSampleTable);
			real_traffic_updater = new RealTrafficUpdater();
			
			//traffic_updater = new TravelTimeSliceUpdater(100);
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		taxi = new TaxiInfo[max_suid + 1];
		int size = Runtime.getRuntime().availableProcessors();
		Common.logger.debug("pool size: " + size);
		//ThreadPool = Executors.newFixedThreadPool(size);
		ThreadPool = Executors.newCachedThreadPool();
	}
	
	public static void init_road_config(){
		road_config = new HashMap<Short, Tuple<Double, Integer>>();
		short class_id[] = {100,101,102,104,105,106,107,108,109,110,111,112,113,114,
				117,118,119,120,122,123,124,125,201,202,301,303,304,305};
		double priority[] = {1.30,1.0,1.10,1.04,1.12,1.08,1.15,1.10,1.20,1.12,1.25,1.30,
				1.50,1.75,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30};
		for(int i=0; i<28; i++){
			road_config.put(class_id[i], new Tuple(priority[i], Common.max_speed));
		}
	}
	
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
	
	public static Connection getConnection(){
		Connection con=null;
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			Common.logger.error("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			e.printStackTrace();
			return null;
		}
		//System.out.println("PostgreSQL JDBC Driver Registered!");
		try {
			con = DriverManager.getConnection(JdbcUrl + DataBase, UserName, UserPwd);
			con.setAutoCommit(false);
			
		} catch (SQLException e) {
			Common.logger.error("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}	
		return con;
	}
	
	//implement ST_Distance_Sphere function in postgres to avoid interact with database
	public static float calculate_dist(double lat1, double lng1, double lat2, double lng2) {
	    double earthRadius = 6370986; //meters,same as ST_Distance_Sphere
	    double dLat = Math.toRadians(lat2-lat1);
	    double dLng = Math.toRadians(lng2-lng1);
	    double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
	               Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
	               Math.sin(dLng/2) * Math.sin(dLng/2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	    float dist = (float) (earthRadius * c);

	    return dist;
	}
	
	//将数据点的gid,offset,route清空,stop值为空的或!=1的都判断为moving
	public static void clear(String database, String sample_table){
		Connection con = null;
		Statement stmt = null;
		
		con = Common.getConnection();
		if (con == null) {
			Common.logger.error("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
			SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    String datetime = tempDate.format(new java.util.Date());
		    System.out.println(datetime);
		    
		    stmt = con.createStatement();
			//String sql="ALTER TABLE " + sample_table + " add column gid integer;";
	    	//System.out.println(sql);
	    	//stmt.executeUpdate(sql);
		    String sql="update " + sample_table +" SET gid=null , edge_offset=null, route=null";
		    //System.out.println(sql);
		    Common.logger.debug(sql);
		    stmt.executeUpdate(sql);

		    datetime = tempDate.format(new java.util.Date());
		    System.out.println(datetime);
		    sql="update " + sample_table +" SET stop="+Sample.MOVING+" where stop is null or stop!="+Sample.LONG_STOP;
		    //System.out.println(sql);
		    Common.logger.debug(sql);
		    stmt.executeUpdate(sql);
		    datetime = tempDate.format(new java.util.Date());
		    Common.logger.debug(datetime);
		    
		    con.commit();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    dropConnection(con);
		}
		Common.logger.debug("clear finished!");
	}

	//从merged_table中筛选出符合位置范围的点,存入matching_table
	public static void filter_samples(String database, String merged_table, String matching_table, long start_utc, long end_utc, long min_x, long max_x, long min_y, long max_y){
		
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;

		con = Common.getConnection();
		if (con == null) {
			Common.logger.error("Failed to make connection!");
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
		    	String sql="";
		    	//all time
		    	if(start_utc == 0 && end_utc ==0){
		    		sql="create table "+ matching_table +" as select * from "+ merged_table +" where "
		    				+ "lon>="+min_x+" and lon<"+max_x+" and lat>="+min_y+" and lat<"+max_y+";";
		    	}
		    	else {
		    		sql="create table "+ matching_table +" as select * from "+ merged_table +" where utc>="+start_utc+" and utc<"+end_utc
			    			+" and lon>="+min_x+" and lon<"+max_x+" and lat>="+min_y+" and lat<"+max_y+";";
		    	}
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
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="alter table "+matching_table+" add column gid integer;";
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
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="alter table "+matching_table+" add column edge_offset double precision;";
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
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="alter table "+matching_table+" add column route text;";
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
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+matching_table+"_suidutc_idx ON "+ matching_table +"(suid, utc);";
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
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+matching_table+"_suid_idx ON "+ matching_table +"(suid);";
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
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+matching_table+"_utc_idx ON "+ matching_table +"(utc);";
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
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
	}
	public static void init_roadlist(String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		int max_gid=0;
			
		con = Common.getConnection();
		if (con == null) {
			Common.logger.error("Failed to make connection!");
			return ;
		}
		try {
			//import the data from database;
			stmt = con.createStatement();
			String sql="";
			//获取最大gid值
			sql="select gid from "+ roadmap_table +" order by gid desc limit 1";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
			    max_gid=rs.getInt("gid");
			}
			    
			if(max_gid<=0){
			    return ;
			}
			    
			roadlist=new AllocationRoadsegment[max_gid+1];
			Common.logger.debug("max gid: " + max_gid);
			for(int i=0;i<roadlist.length;i++){
			    roadlist[i]=new AllocationRoadsegment();
			    roadlist[i].avg_speed = 10;
			}
			//read road info
			sql="select * from "+ roadmap_table;
			rs = stmt.executeQuery(sql);
			while(rs.next()){
			    long cur_gid = rs.getLong("gid");
			    if(cur_gid >= roadlist.length || cur_gid < 0){
			    	continue;
			    }
			    double maxspeed_forward = rs.getDouble("maxspeed_forward");
			    double maxspeed_backward = rs.getDouble("maxspeed_backward");
			    double maxspeed = maxspeed_forward > maxspeed_backward ? maxspeed_forward:maxspeed_backward;
			    AllocationRoadsegment cur_road=new AllocationRoadsegment(cur_gid,maxspeed, rs.getDouble("average_speed"), 0);
			    cur_road.length = rs.getDouble("length");
			    //10m/s
			    cur_road.time = cur_road.length * 100;	    
			    cur_road.avg_speed = 10;
			    roadlist[(int)cur_gid] = cur_road;
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
		//update_roadlist(roadmap_table, false);
		Common.logger.debug("init roadlist finished!");
		return ;
	}
	public static void init_roadlist2(){
		
		//get max gid
		Iterator<Road> roadmap = map.edges();
		long max_gid = -1;//max_gid is not equal to size
		while(roadmap.hasNext()){
			Road road = roadmap.next();
			long gid = road.id();
			if(gid > max_gid){
				max_gid = gid;
			}
		}
		Common.logger.debug("max gid: " + max_gid);
		roadlist=new AllocationRoadsegment[(int)max_gid+1];
		for(int i=0;i<roadlist.length;i++){
		    roadlist[i]=new AllocationRoadsegment();
		    roadlist[i].avg_speed = 10;
		}
		
		roadmap = map.edges();
		while(roadmap.hasNext()){
			Road road = roadmap.next();
			long gid = road.id();
			double maxspeed = road.maxspeed();
			AllocationRoadsegment cur_road=new AllocationRoadsegment(gid,maxspeed, 10.0, 0);
			cur_road.length = road.length();//meters
			cur_road.time = cur_road.length/10;
			cur_road.base_gid = road.base().id();
			roadlist[(int)gid] = cur_road;
		}
	}
	
	//将time_table中的路段记录转换为AllocationRoadsegment类
	public static void update_roadlist(String time_table, boolean has_nextgid){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		int max_gid=0;
			
		con = Common.getConnection();
		if (con == null) {
			Common.logger.error("Failed to make connection!");
			return ;
		}
			
		try {
			//import the data from database;
			stmt = con.createStatement();
			String sql="";
			    
			//read the default value for road segment
			if(!has_nextgid){
			    sql="select * from "+time_table+" order by gid desc";
			}
			else{
			    sql="select * from "+time_table+" where next_gid is null order by gid desc";
			}
			Common.logger.debug(sql);
			rs = stmt.executeQuery(sql);
			    
			if(!has_nextgid){
				while(rs.next()){
				    long cur_gid = rs.getLong("gid");
				    if(cur_gid >= roadlist.length){
				    	continue;
				    }
				    AllocationRoadsegment cur_road=new AllocationRoadsegment(cur_gid,rs.getDouble("max_speed"), rs.getDouble("average_speed"), rs.getInt("reference"));
				    cur_road.reverse_cost=rs.getDouble("reverse_cost") == 1000000 ? 1:rs.getDouble("reverse_cost");
				    cur_road.to_cost=rs.getDouble("to_cost") == 1000000 ? 1:rs.getDouble("to_cost");
				    cur_road.length=rs.getDouble("length");
				    cur_road.time = rs.getDouble("time");
				    if(cur_gid >= roadlist.length){
				    	continue;
				    }
				    roadlist[(int)cur_gid]=cur_road;
				}
			}else{
			    while(rs.next()){
				    long cur_gid = rs.getLong("gid");
				    if(cur_gid >= roadlist.length){
				    	continue;
				    }
				    AllocationRoadsegment cur_road=new AllocationRoadsegment(cur_gid, 0, rs.getDouble("average_speed"), rs.getInt("reference"));
				    cur_road.reverse_cost=rs.getDouble("reverse_cost") == 1000000 ? 1:rs.getDouble("reverse_cost");
				    cur_road.to_cost=rs.getDouble("to_cost") == 1000000 ? 1:rs.getDouble("to_cost");
				    cur_road.length=rs.getDouble("length");
				    cur_road.time = rs.getDouble("time");
				    roadlist[(int)cur_gid]=cur_road;
				}
			}
			    
			//read turning-specific value for road segment
			if(has_nextgid){
				sql="select * from "+ time_table +" where next_gid is not null order by gid, next_gid desc";
				Common.logger.debug(sql);
				rs = stmt.executeQuery(sql);
			    while(rs.next()){
				    int cur_gid=rs.getInt("gid");
				    if(cur_gid >= roadlist.length){
				    	continue;
				    }
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
		Common.logger.debug("update_roadlist finished!");
		return ;
	}
	
	public static void change_scheme_roadmap(String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;
		
		con = Common.getConnection();
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
		Common.logger.debug("change scheme of roadmap finished!");
	}
	//clear slice  table in certain date
	public static void clear_travel_table(String date) throws SQLException{
		Connection con = getConnection();
		try{
			Statement stmt = con.createStatement();
			for(int i=1; i<= Common.max_seg; i++){
				//drop slice table
				String slice_table = Common.traffic_slice_table + i + date;
				String sql = "DROP TABLE IF EXISTS " + slice_table + ";";
				Common.logger.debug(sql);
				stmt.executeUpdate(sql);
				//drop road and turning time table
				String road_slice_table = Common.real_road_slice_table + i + date;
				sql = "DROP TABLE IF EXISTS " + road_slice_table + ";";
				Common.logger.debug(sql);
				stmt.executeUpdate(sql);
				
				String turning_slice_table = Common.real_turning_slice_table + i + date;
				sql = "DROP TABLE IF EXISTS " + turning_slice_table + ";";
				Common.logger.debug(sql);
				stmt.executeUpdate(sql);
			}
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		finally{
			con.commit();
			Common.logger.debug("clear traffic_slice_table " + date + "finished");
		}
	}
	
	//return slice number of sample by utc
	public static int get_seq(Sample sample){
		int seq_num=(int)(max_seg-(end_utc-sample.utc.getTime()/1000)/period);
		return seq_num;
	}
}
