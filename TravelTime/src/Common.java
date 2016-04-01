import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/** 
 * 2016年3月11日 
 * Common.java 
 * author:ZhangYu
 */

public class Common {
	
	//some config on database
	static String JdbcUrl  = "jdbc:postgresql://localhost:5432/";
	static String UserName = "secret";
	static String UserPwd  = "secret";
	
	//common data
	public static ArrayList taxi[] = null;//taxi sample
	
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
	
	public static Connection getConnection(String database){
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
			con = DriverManager.getConnection(JdbcUrl + database, UserName, UserPwd);
			con.setAutoCommit(false);
			
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}	
		return con;
	}
	
	//store taxi data into memory,return max_suid
	public static int get_taxi_data(String database, String sample_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList<Integer> taxi_list=new ArrayList<Integer>();
		con = getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return -1;
		}
			
		try {
			//import the data from database;
			stmt = con.createStatement();
			String sql="select suid from " + sample_table +" group by suid";
			//String sql="select suid from " + sample_table +" group by suid order by suid";
			//System.out.println(sql);
			rs = stmt.executeQuery(sql);
			    
			if(taxi_list==null){
				return -1;
			}
			taxi_list.clear();
			while(rs.next()){
			    taxi_list.add(rs.getInt("suid"));
			}
			System.out.println("get_suids finished!\n");
			System.out.println("size: " + taxi_list.size() + "\n");
			int max_suid = taxi_list.get(0);
			for(int i:taxi_list){
				if(i > max_suid){
					max_suid = i;
				}
			}
			//int max_suid = Integer.getInteger(Collections.max(taxi_list));
			System.out.println("max suid: " + max_suid + "\n");
			taxi = new ArrayList[max_suid + 1];
			//ArrayList<Integer> taxi;
			//提取同一辆车的有效轨迹点,按时间排序
			//import the data from database;
			stmt = con.createStatement();
			//String sql="select * from " + sample_table +" where suid between " +suid + "order by utc";// + " and ostdesc not like '%定位无效%' order by utc";
			//System.out.println(sql);
			sql="select * from " + sample_table;// + " and ostdesc not like '%定位无效%' order by utc";
			rs = stmt.executeQuery(sql);
			//int count = 0;
			System.out.println("start classify data by suid!\n");
			while(rs.next()){
				int temp_id = (int)rs.getLong("suid");
				if(taxi[temp_id] == null){
					taxi[temp_id] = new ArrayList<Sample>();
				}
				taxi[temp_id].add(new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getLong("lat"), 
	    		rs.getLong("lon"), (int)rs.getLong("head"), rs.getLong("speed"), rs.getLong("distance")));
				//count++;
				//System.out.println(count + "\n");
			}
			System.out.println("classify finished. start process\n");
			return max_suid;
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
		return -1;
	}
	
	//将数据点的gid,offset,route清空,stop值为空的或!=1的都判断为moving
	public static void clear(String database, String sample_table){
		Connection con = null;
		Statement stmt = null;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
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
		    System.out.println(sql);
		    stmt.executeUpdate(sql);

		    datetime = tempDate.format(new java.util.Date());
		    System.out.println(datetime);
		    sql="update " + sample_table +" SET stop="+Sample.MOVING+" where stop is null or stop!="+Sample.LONG_STOP;
		    System.out.println(sql);
		    stmt.executeUpdate(sql);
		    datetime = tempDate.format(new java.util.Date());
		    System.out.println(datetime);
		    
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
		System.out.println("clear finished!");
	}
	//add some column for map matching
	public static void change_schema(String database, String table){
		
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;

		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		try{
			stmt = con.createStatement();
		
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="alter table "+table+" add column gid integer;";
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
	    		String sql="alter table "+table+" add column edge_offset double precision;";
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
	    		String sql="alter table "+table+" add column route text;";
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
	    		String sql="CREATE INDEX "+table+"_suidutc_idx ON "+ table +"(suid, utc);";
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
	    		String sql="CREATE INDEX "+table+"_suid_idx ON "+ table +"(suid);";
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
	    		String sql="CREATE INDEX "+table+"_utc_idx ON "+ table +"(utc);";
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

		con = Common.getConnection(database);
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
}
