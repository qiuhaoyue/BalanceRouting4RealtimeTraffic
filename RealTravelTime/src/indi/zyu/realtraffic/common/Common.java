package indi.zyu.realtraffic.common;

import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.process.ProcessThread;
import indi.zyu.realtraffic.process.TaxiInfo;
import indi.zyu.realtraffic.road.AllocationRoadsegment;
import indi.zyu.realtraffic.updater.GPSUpdater;
import indi.zyu.realtraffic.updater.RealTrafficUpdater;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

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
	//static String JdbcUrl  = "jdbc:postgresql://ip:port/";
	
	//static String Host     = "secret";
	static String Host     = "localhost";
	
	//static int Port        = secret;
	static int Port        = 5432;
	
	static String UserName = "secret";
	static String UserPwd  = "secret";
	static String DataBase = "secret";
	
	public static String OriginWayTable = "ways";
	//static String OneWayTable    = "oneway_test";
	//static String OriginSampleTable = "gps";
	//static String ValidSampleTable  = "valid_gps_test2";
	public static String ValidSampleTable  = "valid_gps_utc";
	public static String SingleSampleTable  = "gps_5434";
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
	public static double init_turning_time = 4;
	public static int delay_update_thresold = 1;//to get delay updated traffic
	public static double smooth_alpha = 0.9;//to smooth traffic
	
	public static int match_windows_size = 4;
	
	public static long period = 300L;
	public static long end_utc=1270569600L;//1231218000-1231221600
	public static long start_utc=1270483200L;
	public static long max_seg;
	
	//to control speed of data emission
	public static int emission_step = 1;//send points within next x seconds every time
	public static int emission_multiple = 1;//times of speed of real time.
	
	//taxi info
	public static TaxiInfo taxi[] = null;//taxi sample
	
	public static AllocationRoadsegment[] roadlist=null;
	
	public static Logger logger = LogManager.getLogger(Common.class.getName());
	
	public static RoadMap map = null;
	public static Matcher matcher;
	
	static GPSUpdater gps_updater;
	public static GPSUpdater unkown_gps_updater;//points that match failed
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
	public static AtomicInteger thread_counter;//current thread number, to control speed of data emission
	public static int max_thread_number = 30; //max number of thread including thread in queue	
	public static int thread_number = 4;
	public static ProcessThread[] thread_pool;
	
	
	//initialize params
	public static void init(int max_suid){	
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
			//thread_counter = new AtomicInteger(0);
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		taxi = new TaxiInfo[max_suid + 1];
		//create thread
		int size = Runtime.getRuntime().availableProcessors();
		Common.logger.debug("pool size: " + size);
		thread_number = size;
		thread_pool = new ProcessThread[thread_number];
		for(int i=0; i< thread_number; i++){
			thread_pool[i] = new ProcessThread();
			thread_pool[i].start();
		}
		
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
	
	public static void init_roadlist(){
		
		//get max gid
		Iterator<Road> roadmap = map.edges();
		long max_gid = -1;//max_gid is not equal to size
		Common.logger.debug(map.size());
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
			cur_road.class_id = road.type();
			roadlist[(int)gid] = cur_road;
		}
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
