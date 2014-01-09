import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.TimeZone;
import java.lang.Math;
import java.util.Queue;
 
public class RemoveStopPoint {
	
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
	
	public static void get_suids(String database, String sample_table, ArrayList<String> taxi_list){
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
		    DBconnector.dropConnection(con);
		}
		System.out.println("get_suids finished!");
	}
	
	public static void label_stop(String database, String sample_table, String suid){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		double interval_threshold = 300.0;
		double distance_threshold = 50.0;
		double temp_distance_threshold = DBconnector.sigma_with_stop*2;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    String sql="select * from " + sample_table +" where suid = " +suid + " order by utc";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    ArrayList<Sample> trajectory= new ArrayList<Sample>();
		    while(rs.next()){
		    	trajectory.add(new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getLong("lat"), 
		    					rs.getLong("lon"), (int)rs.getLong("head"), rs.getLong("speed"), rs.getLong("distance")));
		    }
		    
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
		    	
		    	//get the distance_threshold-based clustering
		    	for(int j=cur_pos-1; j>=pre_start_pos; j--){
		    		pre_sample=trajectory.get(j);
		    		sql="select ST_Distance_Sphere(ST_GeometryFromText('POINT(" +
		    				pre_sample.lon + " " + pre_sample.lat + ")', 4326),"+
		    				"ST_GeometryFromText('POINT(" +
		    				cur_sample.lon + " " + cur_sample.lat + ")', 4326));";
		    		//System.out.println(sql);
		    		rs = stmt.executeQuery(sql);
	    			distance=0.0;
			    	while(rs.next()){
			    		distance=rs.getDouble("st_distance_sphere");
				    }
			    	if(distance>distance_threshold){
			    		break;
			    	}
			    	start_pos=j;
		    	}
		    	
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
		    
		    //write the test result into database
		    for(cur_pos=0; cur_pos<trajectory.size(); cur_pos++){
		    	cur_sample=trajectory.get(cur_pos);
		    	if(cur_sample.stop==0){
		    		continue;
		    	}
		    	sql="UPDATE "+ sample_table +" SET stop="+ cur_sample.stop +" WHERE suid="+suid+" and utc="+(int)(cur_sample.utc.getTime()/1000);
		    	try{
			    	//System.out.println("Sample["+cur_pos+"]"+ sql);
			    	stmt.executeUpdate(sql);
				}
				catch (SQLException e) {
					e.printStackTrace();
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
		finally {
		    DBconnector.dropConnection(con);
		}
		System.out.println( suid+": label_stop finished!");
	}
	
	public static void main(String[] args){
		
		try {
			if(args.length>0){
				/*
				try {
					//sleep 1 mins
					Thread.sleep(5000L);
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				//System.out.println(args[0]);
				
				ArrayList<String> taxi_list=new ArrayList<String>();
				RemoveStopPoint.get_suids("mydb", args[0], taxi_list);
				for(int i=0; i<taxi_list.size();i++){
					System.out.print((i+1)+"/"+taxi_list.size()+"	");
					RemoveStopPoint.label_stop("mydb", args[0], taxi_list.get(i));
				}
			}
			else{
				ArrayList<String> taxi_list=new ArrayList<String>();
				RemoveStopPoint.get_suids("mydb", "valid_gps_test", taxi_list);
				for(int i=0; i<taxi_list.size();i++){
					System.out.print((i+1)+"/"+taxi_list.size()+"	");
					RemoveStopPoint.label_stop("mydb", "valid_gps_test", taxi_list.get(i));
				}
			}
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}