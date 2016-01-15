import java.sql.*;
import java.util.Calendar;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.lang.Math;
import java.util.HashMap;
import java.text.DecimalFormat;
 
public class PathPlanner {
	
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
	
	
	public static void backup_and_routable(String database, String time_table, String routable_table, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
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
			
			try{
				spt = con.setSavepoint("svpt1");
				String sql="create table "+ routable_table +" as select * from "+time_table+" where next_gid is null;";
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
	    		String sql="alter table "+ routable_table +" ADD COLUMN target integer;";
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
	    		String sql="alter table "+ routable_table +" ADD COLUMN source integer;";
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
	    		String sql="update "+ routable_table +" set source="+roadmap_table+".source, target="+roadmap_table+".target from "+
	    				roadmap_table+" where "+routable_table+".gid="+roadmap_table+".gid;";
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
	    		String sql="update "+ routable_table +" set to_cost=(length*1000/average_speed) where reverse_cost<0 or reverse_cost>=(0.5*"+(RoadCostUpdater.inconnectivity)+");";
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
	    		String sql="update "+ routable_table +" set reverse_cost=(length*1000/average_speed) where to_cost<0 or to_cost>="+(RoadCostUpdater.inconnectivity)+";";
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
	    		String sql="delete from "+ routable_table +" where source is null or target is null;";
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
		DBconnector.dropConnection(con);
		//System.out.println("aggregate_time finished!");
	}
	
	public static void make_time_table_routable(String database, String aggregated_time_table, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
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
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ aggregated_time_table +" ADD COLUMN target integer;";
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
	    		String sql="alter table "+ aggregated_time_table +" ADD COLUMN source integer;";
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
	    		String sql="update "+ aggregated_time_table +" set source="+roadmap_table+".source, target="+roadmap_table+".target from "+
	    				roadmap_table+" where "+aggregated_time_table+".gid="+roadmap_table+".gid;";
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
	    		String sql="update "+ aggregated_time_table +" set to_cost=(length*1000/average_speed) where reverse_cost<0 or reverse_cost>="+(RoadCostUpdater.inconnectivity)+";";
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
	    		String sql="update "+ aggregated_time_table +" set reverse_cost=(length*1000/average_speed) where to_cost<0 or to_cost>="+(RoadCostUpdater.inconnectivity)+";";
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
	    		String sql="delete from "+ aggregated_time_table +" where source is null or target is null;";
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
		DBconnector.dropConnection(con);
		//System.out.println("aggregate_time finished!");
	}
	
	
	public static void plan_shortest_path(String database, ArrayList<String> time_tables, String aggregated_time_table, 
			String roadmap_table, String trip_table, String footprint_table, long start_utc, long interval){
		
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
		
		int interval_count=time_tables.size();
		int max_interval=interval_count-1;
		long[] start_utcs=new long[interval_count];
		long[] end_utcs=new long[interval_count];
		ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
				
		for(int i=0; i<time_tables.size(); i++){
			start_utcs[i]=start_utc+i*interval;
			end_utcs[i]=start_utcs[i]+interval;
			time_list.add(TripFinder.get_roadlist(database, time_tables.get(i), true));
			System.out.println("Read "+time_tables.get(i)+" finished!");
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ trip_table +" add column trsp text;";
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
	    		String sql="alter table "+ trip_table +" add column trsp_plan double precision;";
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
	    		String sql="drop table "+ footprint_table +" ;";
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
	    		//store the suid, planed time, gid, time entering the road segment, time getting out of the roadsegment;
	    		String sql="create table "+ footprint_table +"(suid bigint, trip_utc bigint, gid integer, coverage double precision, start_utc bigint, end_utc bigint);";
	    		
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
			
			ArrayList<String> values=new ArrayList<String>();
			ArrayList<String> updates=new ArrayList<String>();
			
			for(int seq=1; seq<time_tables.size(); seq++){
				
				System.out.println("TRshortestPath ["+seq+"/"+time_tables.size()+"] ");
				
				ArrayList<Trip> trips=new ArrayList<Trip>();
				try{
					spt = con.setSavepoint("svpt1");
					String sql="select * from "+ trip_table +" where ( valid is null or valid >=0 ) and start_utc>="+start_utcs[seq]+
							" and start_utc<"+end_utcs[seq]+" order by start_utc; ";
					//System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
		    		while(rs.next()){
		    			Trip cur_trip=new Trip(rs.getLong("suid"), rs.getInt("start_gid"), rs.getDouble("start_pos"), rs.getLong("start_utc"), rs.getInt("end_gid"), rs.getDouble("end_pos"));
		    			cur_trip.index=rs.getInt("seq");
		    			trips.add(cur_trip);
		    		}
		    	}
		    	catch (SQLException e) {
					e.printStackTrace();
					con.rollback(spt);
				}
	    		
	    		TRshortestPath trsp_instance=new TRshortestPath();
	    		String restriction_table="oneway_intersection";
	    		//ArrayList<Path_element> path=new ArrayList<Path_element>();
	    		String plan_roadmap=time_tables.get(seq-1);
	    		HashMap<Integer, ArrayList<Path_element>> paths_hashtable=new HashMap<Integer, ArrayList<Path_element>>();
	    		
	    		for(int i=0; i<trips.size(); i++){
					Trip cur_trip=trips.get(i);
					
					//calculate the route for given source and destination;
					String str_path="";
					try{
			    		spt = con.setSavepoint("svpt1");
			    		
			    		ArrayList<Path_element> path=new ArrayList<Path_element>();
			    		
			    		if(!paths_hashtable.containsKey(cur_trip.index)){
				    		if(trsp_instance.shortest_path(database, plan_roadmap, restriction_table, 
				    				cur_trip.start_gid, cur_trip.start_pos, cur_trip.end_gid, cur_trip.end_pos, path)<0){
				    			continue;
				    		}
				    		paths_hashtable.put(cur_trip.index, path);
			    		}
    			    	else{
    			    		path.addAll(paths_hashtable.get(cur_trip.index));
    			    	}
			    		
			    		cur_trip.path.clear();
			    		double cost=0.0;
			    		for(int j=0; j<path.size(); j++){
			    			Path_element node=path.get(j);
			    			int gid=(int)node.edge_id;
			    			if(gid>=0){
			    				cur_trip.path.add(gid);
			    				cost+=node.cost;
			    				str_path+=node.edge_id+",";
			    			}
			    		}
			    		cur_trip.cost=cost;
			    		trips.set(i, cur_trip);
			    	}
			    	catch (Exception e) {
					    e.printStackTrace();
					    cur_trip.cost=-1;
					    cur_trip.path.clear();
					    trips.set(i, cur_trip);
					}
					
					//According the estimated time on each road segment, store the occupancy record to database
					
					if(cur_trip.cost>0 && cur_trip.path.size()!=0){
					
						//store the route and corresponding cost in database;
				    	String newsql="update "+ trip_table +" set trsp='" + str_path + "', trsp_plan="+cur_trip.cost+" where suid=" +cur_trip.suid +" and start_utc="+ cur_trip.start_utc+";";
			    		
						updates.add(newsql);
			    		if(updates.size()>200){
			    			String sql="";
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
				    	
				    	//calculate the occupancy and store in database
				    	double cur_utc=cur_trip.start_utc;
			    		int cur_interval= (int)(seq+((int)(cur_utc-start_utcs[seq]))/interval);
			    		int pre_interval=cur_interval;
			    		AllocationRoadsegment[] time=time_list.get(cur_interval);
			    		
			    		int trip_size=cur_trip.path.size();
				    	for(int j=0; j<trip_size; j++){
				    		int cur_gid = cur_trip.path.get(j);
				    		int next_gid = -1;
				    		if(j<trip_size-1){
				    			next_gid = cur_trip.path.get(j+1);
				    		}
				    		
				    		//determine which interval is this travel located;
				    		cur_interval= (int)(seq+((int)(cur_utc-start_utcs[seq]))/interval);
				    		if(cur_interval>max_interval){
				    			cur_interval=max_interval;
				    		}
				    		if(cur_interval!=pre_interval){
				    			time=time_list.get(cur_interval);
				    			pre_interval=cur_interval;
				    		}
				    		
				    		double segment_time=0;
				    		double percent=0;
				    		if(j==0 && trip_size==1){
				    			segment_time=time[cur_gid].get_traveltime(next_gid)*Math.abs(cur_trip.start_pos-cur_trip.end_pos);
				    			percent=Math.abs(cur_trip.start_pos-cur_trip.end_pos);
				    		}
				    		else if(j==0){
				    			segment_time=time[cur_gid].get_traveltime(cur_trip.start_pos, false, next_gid);
				    			percent=segment_time/time[cur_gid].get_traveltime(next_gid);
				    		}
				    		else if(j==trip_size-1){
				    			segment_time=time[cur_gid].get_traveltime(cur_trip.end_pos, true, next_gid);
				    			percent=segment_time/time[cur_gid].get_traveltime(next_gid);
				    		}
				    		else{
				    			segment_time=time[cur_gid].get_traveltime(next_gid);
				    			percent=1.0;
				    		}
				    		if(segment_time<0){
				    			//do nothing, because this still helps to provide the traffic count on give road segment;
				    			segment_time=1;
				    		}
				    		
				    		//store into database
				    		double next_utc=cur_utc+segment_time;
				    		values.add("("+cur_trip.suid+", "+cur_trip.start_utc+", "+cur_gid+", "+percent+", "+(long)cur_utc+", "+(long)(next_utc)+")");
				    		cur_utc=next_utc;
				    	}
    					
				    	if(values.size()>400){
				    		String sql="insert into "+ footprint_table +" values ";
					    	try{
	    	    		    	spt = con.setSavepoint("svpt1");
	    	    		    	for(int vi=0; vi<values.size(); vi++){
	    	    		    		sql+=values.get(vi);
	    	    		    		if(vi!=values.size()-1){
	    	    		    			sql+=",\n";
	    	    		    		}
	    	    		    		else{
	    	    		    			sql+=";";
	    	    		    		}
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
	    	    				values.clear();
	    	    			}
				    	}
				    }
				}
			}//end for
			if(updates.size()>0){
				String sql="";
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
			
			if(values.size()>0){
				String sql="insert into "+ footprint_table +" values ";
		    	try{
    		    	spt = con.setSavepoint("svpt1");
    		    	
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
    		    	System.err.println(sql);
    				e.printStackTrace();
    				con.rollback(spt);
    			}
    			finally{
    				con.commit();
    				values.clear();
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
	
	public static void random_k_shortest_path(String database, ArrayList<String> time_tables, String aggregated_time_table, String roadmap_table, 
			String trip_table, String footprint_table, int k, double max_delay, long start_utc, long interval){
		
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
		
		int interval_count=time_tables.size();
		int max_interval=interval_count-1;
		long[] start_utcs=new long[interval_count];
		long[] end_utcs=new long[interval_count];
		ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
				
		for(int i=0; i<time_tables.size(); i++){
			start_utcs[i]=start_utc+i*interval;
			end_utcs[i]=start_utcs[i]+interval;
			time_list.add(TripFinder.get_roadlist(database, time_tables.get(i), true));
			System.out.println("Read "+time_tables.get(i)+" finished!");
		}
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ trip_table +" add column rksp text;";
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
	    		String sql="alter table "+ trip_table +" add column rksp_plan double precision;";
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
	    		String sql="drop table "+ footprint_table +" ;";
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
	    		//store the suid, planed time, gid, time entering the road segment, time getting out of the roadsegment;
	    		String sql="create table "+ footprint_table +"(suid bigint, trip_utc bigint, gid integer, coverage double precision, start_utc bigint, end_utc bigint);";
	    		
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
			
			for(int seq=1; seq<time_tables.size(); seq++){
				
				System.out.println("Random_KSP ["+seq+"/"+time_tables.size()+"] ");
				
				ArrayList<Trip> trips=new ArrayList<Trip>();
				try{
					spt = con.setSavepoint("svpt1");
					String sql="select * from "+ trip_table +" where ( valid is null or valid >=0 ) and start_utc>="+start_utcs[seq]+
							" and start_utc<"+end_utcs[seq]+" order by start_utc; ";
					System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
		    		while(rs.next()){
		    			Trip cur_trip=new Trip(rs.getLong("suid"), rs.getInt("start_gid"), rs.getDouble("start_pos"), rs.getLong("start_utc"), rs.getInt("end_gid"), rs.getDouble("end_pos"));
		    			trips.add(cur_trip);
		    		}
		    	}
		    	catch (SQLException e) {
					e.printStackTrace();
					con.rollback(spt);
				}
	    		
				String plan_roadmap=time_tables.get(seq-1);
	    		TRkShortestPath trksp_instance=new TRkShortestPath();
	    		String restriction_table="oneway_intersection";
	    		ArrayList<Path_element> path=new ArrayList<Path_element>();
	    		ArrayList<Path> paths=new ArrayList<Path>();
	    		int top_k=k*2;
	    		//double max_delay=1.5;
	    		boolean update_graph=false;
	    		
	    		for(int i=0; i<trips.size(); i++){
					Trip cur_trip=trips.get(i);
					
					String str_path="";
					try{
			    		spt = con.setSavepoint("svpt1");
			    		paths.clear();
			    		path.clear();
			    		trksp_instance.get_shortest_paths(database, plan_roadmap, restriction_table, update_graph, 
			    				cur_trip.start_gid, cur_trip.start_pos, cur_trip.end_gid, cur_trip.end_pos, top_k, max_delay, paths);
			    		
			    		//randomly choose a path;
			    		int mode=paths.size();
			    		if(mode==0){
			    			continue;
			    		}
			    		//System.out.println("Ksp="+mode);
			    		if(mode>k){
			    			mode=k;
			    		}
			    		double seed=Math.random()*mode;
			    		path=paths.get((int)seed).path;
			    		
			    		cur_trip.path.clear();
			    		double cost=0.0;
			    		for(int j=0; j<path.size(); j++){
			    			Path_element node=path.get(j);
			    			int gid=(int)node.edge_id;
			    			if(gid>=0){
			    				cur_trip.path.add(gid);
			    				cost+=node.cost;
			    				str_path+=node.edge_id+",";
			    			}
			    		}
			    		cur_trip.cost=cost;
			    		trips.set(i, cur_trip);
			    		
			    	}
			    	catch (Exception e) {
					    e.printStackTrace();
					    cur_trip.cost=-1;
					    cur_trip.path.clear();
					    trips.set(i, cur_trip);
					}
					
					if(cur_trip.cost>0 && cur_trip.path!=null){
						
				    	try{
				    		spt = con.setSavepoint("svpt1");
				    		String sql="update "+ trip_table +" set rksp='"+str_path+"', rksp_plan="+cur_trip.cost+" where suid=" +cur_trip.suid +" and start_utc="+ cur_trip.start_utc+";";
				    		//System.out.println("["+i+"/"+trips.size()+"]:"+sql);
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
				    	
				    	//calculate the occupancy and store in database
				    	double cur_utc=cur_trip.start_utc;
			    		int cur_interval= (int)(seq+((int)(cur_utc-start_utcs[seq]))/interval);
			    		int pre_interval=cur_interval;
			    		AllocationRoadsegment[] time=time_list.get(cur_interval);
			    		
			    		int trip_size=cur_trip.path.size();
			    		ArrayList<String> values=new ArrayList<String>();
				    	for(int j=0; j<trip_size; j++){
				    		int cur_gid = cur_trip.path.get(j);
				    		int next_gid = -1;
				    		if(j<trip_size-1){
				    			next_gid = cur_trip.path.get(j+1);
				    		}
				    		
				    		//determine which interval is this travel located;
				    		cur_interval= (int)(seq+((int)(cur_utc-start_utcs[seq]))/interval);
				    		if(cur_interval>max_interval){
				    			cur_interval=max_interval;
				    		}
				    		if(cur_interval!=pre_interval){
				    			time=time_list.get(cur_interval);
				    			pre_interval=cur_interval;
				    		}
				    		
				    		double segment_time=0;
				    		double percent=0;
				    		if(j==0 && trip_size==1){
				    			segment_time=time[cur_gid].get_traveltime(next_gid)*Math.abs(cur_trip.start_pos-cur_trip.end_pos);
				    			percent=Math.abs(cur_trip.start_pos-cur_trip.end_pos);
				    		}
				    		else if(j==0){
				    			segment_time=time[cur_gid].get_traveltime(cur_trip.start_pos, false, next_gid);
				    			percent=segment_time/time[cur_gid].get_traveltime(next_gid);
				    		}
				    		else if(j==trip_size-1){
				    			segment_time=time[cur_gid].get_traveltime(cur_trip.end_pos, true, next_gid);
				    			percent=segment_time/time[cur_gid].get_traveltime(next_gid);
				    		}
				    		else{
				    			segment_time=time[cur_gid].get_traveltime(next_gid);
				    			percent=1.0;
				    		}
				    		if(segment_time<0){
				    			//do nothing, because this still helps to provide the traffic count on give road segment;
				    			segment_time=1;
				    		}
				    		
				    		//store into database
				    		double next_utc=cur_utc+segment_time;
				    		values.add("("+cur_trip.suid+", "+cur_trip.start_utc+", "+cur_gid+", "+percent+", "+(long)cur_utc+", "+(long)(next_utc)+")");
				    		cur_utc=next_utc;
				    	}
    					
				    	try{
    	    		    	spt = con.setSavepoint("svpt1");
    	    		    	String sql="insert into "+ footprint_table +" values ";
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
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		DBconnector.dropConnection(con);
		//System.out.println("aggregate_time finished!");
	}
	
	public static void enhenced_ebksp(String database, ArrayList<String> time_tables, String aggregated_time_table, String roadmap_table, 
			String trip_table, String footprint_table, String route, String plan_time, int topk, double max_delay, long start_utc, long interval){
		
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;
		//double threshold_rate=max_delay;
		/*
		long pre_time=0;
		long cur_time=0;
		Calendar cal =null;  
		
		double routing_time=0;
		double footprint_time=0;
		double database_time=0;
		double schedule_time=0;*/
		
		DecimalFormat df=new DecimalFormat();
		df.setMaximumFractionDigits(6);

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		int interval_count=time_tables.size();
		int max_interval=interval_count-1;
		long[] start_utcs=new long[interval_count];
		long[] end_utcs=new long[interval_count];
		ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
		ArrayList<Double> average_speeds=new ArrayList<Double>();
		ArrayList<Double> average_lengths=new ArrayList<Double>();
		HashMap<Integer, Double> mp_gid2lane_count = new HashMap<Integer, Double>();
		double average_lane=0;

		try{

			stmt = con.createStatement();
			
			try{
				stmt = con.createStatement();
				spt = con.setSavepoint("svpt1");
	    		String sql="select gid, lane_count from "+ roadmap_table + " ;";
	    		System.out.println(sql);
	    		rs=stmt.executeQuery(sql);
	    		int road_count=0;
	    		while(rs.next()){
	    			int gid=rs.getInt("gid");
	    			double lane_count=rs.getDouble("lane_count");
	    			mp_gid2lane_count.put(gid,lane_count);
	    			road_count++;
	    			average_lane+=lane_count;
	    		}
	    		average_lane/=road_count;
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt); 
			}
			
			for(int i=0; i<time_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				time_list.add(TripFinder.get_roadlist(database, time_tables.get(i), true));
				
				try{
					stmt = con.createStatement();
					spt = con.setSavepoint("svpt1");
		    		String sql="select avg(average_speed) as avg_speed, avg(length) as avg_length from "+ time_tables.get(i) + " where next_gid is null;";
		    		System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
		    		while(rs.next()){
		    			average_speeds.add(rs.getDouble("avg_speed"));
		    			average_lengths.add(rs.getDouble("avg_length"));
		    		}
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt); 
				}
			}
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ trip_table +" add column "+route+" text;";
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
	    		String sql="alter table "+ trip_table +" add column "+plan_time+" double precision;";
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
	    		String sql="drop table "+ footprint_table +" ;";
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
	    		//store the suid, planed time, gid, time entering the road segment, time getting out of the roadsegment;
	    		String sql="create table "+ footprint_table +"(suid bigint, trip_utc bigint, gid integer, coverage double precision, start_utc bigint, end_utc bigint);";
	    		
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
			
			Comparator<Event> comparator=new EventComparator();
			PriorityQueue<Event> event_list= new PriorityQueue<Event>(10000000, comparator);
			//use the taxi_count to record the current controlled traffic in a given road segment;
			HashMap<Integer, RoadSegment> instant_footprint = new HashMap<Integer, RoadSegment>();
			long total_ebksp=0;
			
			for(int seq=1; seq<time_tables.size(); seq++){
				System.out.println("EBKSP ["+seq+"/"+time_tables.size()+"]: " +
						" total_ebksp="+total_ebksp /*+ " \n routing_time="+(long)routing_time/1000 + " \n database_time="+(long)database_time/1000 
    					+ "\n footprint_time="+(long)footprint_time/1000 + " \n schedule_time="+(long)schedule_time/1000 */);
				
				//get the trip inside the interval
				ArrayList<Trip> trips=new ArrayList<Trip>();
				try{
					spt = con.setSavepoint("svpt1");
					String sql="select * from "+ trip_table +" where ( valid is null or valid >=0 ) and start_utc>="+start_utcs[seq]+
							" and start_utc<"+end_utcs[seq]+" order by start_utc; ";
					System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
		    		while(rs.next()){
		    			Trip cur_trip=new Trip(rs.getLong("suid"), rs.getInt("start_gid"), rs.getDouble("start_pos"), rs.getLong("start_utc"), rs.getInt("end_gid"), rs.getDouble("end_pos"));
		    			trips.add(cur_trip);
		    			Event event=new Event(cur_trip.suid, cur_trip.start_utc, cur_trip.start_utc, Event.ROUTING_REQUEST, (trips.size()-1));
		    			event_list.add(event);
		    		}
		    	}
		    	catch (SQLException e) {
					e.printStackTrace();
					con.rollback(spt);
				}
	    		
				String plan_roadmap=time_tables.get(seq-1);
	    		TRkShortestPath trksp_instance=new TRkShortestPath();
	    		String restriction_table="oneway_intersection";
	    		ArrayList<Path> paths=new ArrayList<Path>();
	    		int top_k=topk*2;
	    		boolean update_graph=false;
	    		
	    		//AllocationRoadsegment[] time=null;
	    		AllocationRoadsegment[] time=time_list.get(seq);
	    		HashSet<Integer> included_road=new HashSet<Integer>();
	    		
	    		/*
	    		cal =Calendar.getInstance();
	    		cur_time=cal.getTime().getTime();
	    		pre_time=cur_time;*/
	    		
	    		while (event_list.size()!=0 && event_list.peek().utc < end_utcs[seq]){
	    			/*
	    			cal =Calendar.getInstance();
		    		cur_time=cal.getTime().getTime();
		    		schedule_time+=(cur_time-pre_time);
		    		pre_time=cur_time;*/
		    		
	    			Event cur_event=event_list.remove();
	    			//cur_event.print();
	    			/*System.out.println("	[cur_utc="+(long)(cur_event.utc)+" /	"+end_utcs[seq]+"]:" +
	    					" total_ebksp="+total_ebksp + " \n routing_time="+(long)routing_time/1000 + " \n database_time="+(long)database_time/1000 
	    					+ "\n footprint_time="+(long)footprint_time/1000 + " \n schedule_time="+(long)schedule_time/1000 );*/
	    			
	    			if(cur_event.type==Event.REMOVE_LINK){
	    				
	    				/*
	    				cal =Calendar.getInstance();
    		    		cur_time=cal.getTime().getTime();
    		    		pre_time=cur_time;*/
	    				
	    				if(!instant_footprint.containsKey(cur_event.gid) || 
	    						instant_footprint.get(cur_event.gid).taxi_count < 1){
    		    			System.err.println("REDUCE TRAFFIC FROM A ROAD THAT HAS NO TRAFFIC");
    		    			continue;
    		    		}
    		    		
    		    		instant_footprint.get(cur_event.gid).taxi_count--;
    		    		/*
    		    		cal =Calendar.getInstance();
    		    		cur_time=cal.getTime().getTime();
    		    		footprint_time+=(cur_time-pre_time);
    		    		pre_time=cur_time;*/
    		    		
    		    		/*int list_size=cur_event.gid_list.size();
    		    		
    		    		//if need to add a new road segment
    		    		if(list_size!=0){
    		    			int gid=cur_event.gid_list.get(list_size-1);
    		    			cur_event.gid_list.remove(list_size-1);
    		    				
    		    			double next_utc;
    		    			double percent=0;
    		    			if(list_size==1){
    		    				next_utc=time[gid].get_traveltime(cur_event.end_pos, true, -1);
    		    				percent= next_utc/time[gid].get_traveltime(-1);
    		    				next_utc=cur_event.utc+next_utc;
    		    			}
    		    			else{
    		    				percent=1.0;
    		    				next_utc=cur_event.utc+time[gid].get_traveltime(cur_event.gid_list.get(list_size-2));
    		    				
    		    			}
    		    				
    		    			Event event=new Event(cur_event.suid, cur_event.trip_utc, next_utc, Event.REMOVE_LINK, gid, cur_event.gid_list, cur_event.end_pos);
    		    			event_list.add(event);
    		    			
    		    			//record this pass @ this road segment;
    	    				try{
    	    		    		spt = con.setSavepoint("svpt1");
    	    		    		String sql="insert into "+ footprint_table +" values ("+cur_event.suid+", "+cur_event.trip_utc+", "+gid+", "+ percent +", "+(long)cur_event.utc+", "+(long)next_utc+");";
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
	    			else if(cur_event.type==Event.ADD_LINK){
	    				//no event is of this type;
	    			}
	    			else{// cur_event.type==ROUTING_REQUEST
	    				/*
	    				cal =Calendar.getInstance();
    		    		cur_time=cal.getTime().getTime();
    		    		pre_time=cur_time;*/
	    				
	    				int i=cur_event.trip_seq;
	    				Trip cur_trip=trips.get(i);
	    				
	    				ArrayList<Path> path_candidates=new ArrayList<Path>();
	    				String str_path="";
	    				try{
	    			    	spt = con.setSavepoint("svpt1");
	    			    	paths.clear();
	    			    	ArrayList<Path_element> path=new ArrayList<Path_element>();
	    			    	
	    			    	trksp_instance.get_shortest_paths(database, plan_roadmap, restriction_table, update_graph, 
	    			    			cur_trip.start_gid, cur_trip.start_pos, cur_trip.end_gid, cur_trip.end_pos, top_k, max_delay, paths);
	    			    	
	    			    	//get the path_candidates;
	    			    	int mode=paths.size();
	    			    	if(mode==0){
	    			    		continue;
	    			    	}
	    			    	if(mode>topk){
	    			    		mode=topk;
	    			    	}
	    			    	
	    			    	// to get N=sum(weighted_footprint);
	    			    	double sum_footprint=0;
	    			    	included_road.clear();
	    			    	for(int index=0; index<mode; index++){
	    			    		path_candidates.add(paths.get(index));
	    			    		Path cur_path =path_candidates.get(index);
	    			    		// to get N=sum(n_i);
	        					for(int k=0; k<cur_path.length() ;k++){
	        						int step_gid=(int)cur_path.path.get(k).edge_id;
	        						//System.out.println(step_gid);
	        						if(included_road.contains(step_gid)){
	        							continue;
	        						}
	        						else{
	        							included_road.add(step_gid);
	        							if(instant_footprint.containsKey(step_gid)){
	        								double ftprt=instant_footprint.get(step_gid).taxi_count;
	        								if(ftprt<0){
	        									System.out.println("instant_footprint(@ gid="+ step_gid + "	)= "+ftprt);
	        								}
	        								double len=time[step_gid].length;
				        					double speed=time[step_gid].get_speed(-1);
	        								sum_footprint+=ftprt*average_speeds.get(seq)*average_lengths.get(seq)*average_lane/(len*speed*mp_gid2lane_count.get(step_gid));
	        							}
	        						}
	        					}
	    			    	}
	    			    	
	    					//chose the shortest one to minimize the road occupancy
	    					if(sum_footprint==0){//just randomly choose one of the candidate 
	    			    		//double seed=Math.random()*path_candidates.size();
	    			    		//path=paths.get((int)seed).path;
	    						path=paths.get(0).path;
        					}
	    					
	    					else{//Use EBkSP to determine which path should be chosen and then add REMOVE_LINK event based simulated time;
	    						
	    						total_ebksp++;
	    						
	    						double min_entropy=1;
		    					int min_path=-1;
		    					
	    						for(int j=0;j<path_candidates.size();j++){
	    							Path cur_path=path_candidates.get(j);
		        					double weighted_fc=0;
		        					double entropy=-1;
	
		        					for(int k=0; k<cur_path.length();k++){
			        					int step_gid=(int)cur_path.path.get(k).edge_id;
			        					int next_gid=-1;
			        					
			        					if(step_gid<=0){
			        						continue;
			        					}
			        					
			        					double count=0;
			        					if(instant_footprint.containsKey(step_gid)){
	        								count=instant_footprint.get(step_gid).taxi_count;
	        								if(count<0){
	        									System.out.println("instant_footprint(@ gid="+ step_gid + "	)= "+count);
	        								}
	        							}
			        					
			        					double len=time[step_gid].length;
			        					double speed=time[step_gid].get_speed(next_gid);
			        						
			        					weighted_fc=count*average_speeds.get(seq)*average_lengths.get(seq)*average_lane/(len*speed*mp_gid2lane_count.get(step_gid));
			        					/*weighted_fc=hashtable.get(step_gid)*average_speeds.get(cur_interval)*average_lengths.get(cur_interval)
			        								/(time[step_gid].length*time[step_gid].get_speed(next_gid));*/
			        					
			        					if(weighted_fc<0){
			        						System.err.println("weighted_fc<0");
			        					}
			        					else{
			        						weighted_fc=weighted_fc/sum_footprint+1/Math.E;
			        						entropy += weighted_fc * Math.log(weighted_fc)+1/Math.E; //traffic increase will incur entropy to increase and entropy>0
			        						//$entropy += $wt_ftprt*log($wt_ftprt)+exp(-1);
			        					}
			        				}
		        					
		        					//determine which is the least popular;
			        				if( min_path == -1 || entropy < min_entropy){
			        					min_entropy = entropy; //get the road that has lease entropy;
			        					min_path = j;
			        				}
		        				}
		        				
		        				//based on EBkSP's popularity determine which one should be chosen:
		        				path=path_candidates.get(min_path).path;
	    					}
	    			    	/*
	    					cal =Calendar.getInstance();
	    		    		cur_time=cal.getTime().getTime();
	    		    		routing_time+=(cur_time-pre_time);
	    		    		pre_time=cur_time;*/
	    					
	    			    	//use the final path to update the instant_traffic;
	    			    	cur_trip.path.clear();
	    			    	double cost=0.0;
	    			    	for(int j=path.size()-1; j>=0; j--){
	    			    		Path_element node=path.get(j);
	    			    		int gid=(int)node.edge_id;
	    			    		if(gid>0){
	    			    			cur_trip.path.add(gid);
	    			    			cost+=node.cost;
	    			    			str_path=node.edge_id+","+str_path;
	    			    			
	    			    			if(instant_footprint.containsKey(gid)){
	    	    						instant_footprint.get(gid).taxi_count++;
	    	    					}
	    	    					else{
	    	    						RoadSegment road=new RoadSegment();
	    	    						road.gid=gid;
	    	    						road.taxi_count=1;
	    	    						instant_footprint.put(gid, road);
	    	    					}
	    			    		}
	    			    	}
	    			    	cur_trip.cost=cost;
	    			    	trips.set(i, cur_trip);
	    			    	
	    			    	/*cal =Calendar.getInstance();
	    		    		cur_time=cal.getTime().getTime();
	    		    		footprint_time+=(cur_time-pre_time);
	    		    		pre_time=cur_time;*/
	    		    		
	    			    }
	    			    catch (Exception e) {
	    					e.printStackTrace();
	    					cur_trip.cost=-1;
	    					cur_trip.path.clear();
	    					trips.set(i, cur_trip);
	    				}
	    					
	    				if(cur_trip!=null && cur_trip.cost>0 && cur_trip.path!=null && cur_trip.path.size()!=0){
	    					
	    					//add the routing decision to database;
	    					try{
					    		spt = con.setSavepoint("svpt1");
					    		String sql="update "+ trip_table +" set "+route+"='"+str_path+"', "+plan_time+"="+cur_trip.cost+" where suid=" +cur_trip.suid +" and start_utc="+ cur_trip.start_utc+";";
					    		//System.out.println("["+i+"/"+trips.size()+"]:"+sql);
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
					    	
					    	//calculate the occupancy and store in database
					    	double cur_utc=cur_trip.start_utc;
				    		int cur_interval= (int)(seq+((int)(cur_utc-start_utcs[seq]))/interval);
				    		int pre_interval=cur_interval;
				    		AllocationRoadsegment[] roadtime=time_list.get(cur_interval);
				    		
				    		int trip_size=cur_trip.path.size();
				    		ArrayList<String> values=new ArrayList<String>();
					    	for(int j=0; j<trip_size; j++){
					    		int cur_gid = cur_trip.path.get(j);
					    		int next_gid = -1;
					    		if(j<trip_size-1){
					    			next_gid = cur_trip.path.get(j+1);
					    		}
					    		
					    		//determine which interval is this travel located;
					    		cur_interval= (int)(seq+((int)(cur_utc-start_utcs[seq]))/interval);
					    		if(cur_interval>max_interval){
					    			cur_interval=max_interval;
					    		}
					    		if(cur_interval!=pre_interval){
					    			roadtime=time_list.get(cur_interval);
					    			pre_interval=cur_interval;
					    		}
					    		
					    		double segment_time=0;
					    		double percent=0;
					    		if(j==0 && trip_size==1){
					    			segment_time=roadtime[cur_gid].get_traveltime(next_gid)*Math.abs(cur_trip.start_pos-cur_trip.end_pos);
					    			percent=Math.abs(cur_trip.start_pos-cur_trip.end_pos);
					    		}
					    		else if(j==0){
					    			segment_time=roadtime[cur_gid].get_traveltime(cur_trip.start_pos, false, next_gid);
					    			percent=segment_time/roadtime[cur_gid].get_traveltime(next_gid);
					    		}
					    		else if(j==trip_size-1){
					    			segment_time=roadtime[cur_gid].get_traveltime(cur_trip.end_pos, true, next_gid);
					    			percent=segment_time/roadtime[cur_gid].get_traveltime(next_gid);
					    		}
					    		else{
					    			segment_time=roadtime[cur_gid].get_traveltime(next_gid);
					    			percent=1.0;
					    		}
					    		if(segment_time<0){
					    			//do nothing, because this still helps to provide the traffic count on give road segment;
					    			segment_time=1;
					    		}
					    		
					    		//add new event in to the simulator
					    		double delete_utc=cur_utc+segment_time;
					    		//dobule delete_utc=cur_utc+cur_trip.cost;
					    		Event event=new Event(cur_trip.suid, cur_trip.start_utc, delete_utc, Event.REMOVE_LINK, cur_gid);
					    		event_list.add(event);
					    		
					    		//install into database
					    		double next_utc=cur_utc+segment_time;
					    		values.add("("+cur_trip.suid+", "+cur_trip.start_utc+", "+cur_gid+", "+percent+", "+(long)cur_utc+", "+(long)(next_utc)+")");
					    		cur_utc=next_utc;
					    	}
	    					
					    	try{
	    	    		    	spt = con.setSavepoint("svpt1");
	    	    		    	String sql="insert into "+ footprint_table +" values ";
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
					    	/*
					    	cal =Calendar.getInstance();
	    		    		cur_time=cal.getTime().getTime();
	    		    		database_time+=(cur_time-pre_time);
	    		    		pre_time=cur_time;*/
	    		    		
	    					/* previus solution
	    					//if need to add a new event and install the occupancy in database;
	    					int list_size=cur_trip.path.size();
	    					int cur_gid=0;
	    		    		
	    		    		if(list_size>0){
	    		    			cur_gid = cur_trip.path.get(list_size-1);
	    		    			double next_utc=0.0;
	    		    			Event event=new Event(cur_event.suid, cur_event.trip_utc, next_utc, Event.REMOVE_LINK, cur_gid, cur_trip.path, cur_trip.end_pos);
	    		    			event.gid_list.remove(list_size-1);
	    		    			
	    		    			double percent=0;
	    		    			if(list_size==1){
	    		    				percent=Math.abs(trips.get(cur_event.trip_seq).start_pos-cur_event.end_pos);
	    		    				next_utc=cur_event.utc+time[cur_gid].get_traveltime(-1)*percent;
	    		    			}
	    		    			else{
	    		    				next_utc=time[cur_gid].get_traveltime(trips.get(cur_event.trip_seq).start_pos, false, event.gid_list.get(list_size-2));
	    		    				percent=next_utc/time[cur_gid].get_traveltime(event.gid_list.get(list_size-2));
	    		    				next_utc=cur_event.utc+next_utc;
	    		    			}
	    		    			event.utc=next_utc;	
	    		    			event_list.add(event);
	    		    			
	    		    			//record this pass @ this road segment;
	    	    				try{
	    	    		    		spt = con.setSavepoint("svpt1");
	    	    		    		String sql="insert into "+ footprint_table +" values ("+event.suid+", "+event.trip_utc+", "+cur_gid+", "+ percent +", "+(long)event.trip_utc+", "+(long)next_utc+");";
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
	    				
	    			}// cur_event.type==ROUTING_REQUEST	
	    			
    			}//while (event_list.size()!=0 && event_list.peek().utc < end_utcs[seq])
	    		
    		}//for(int seq=0; seq<time_tables.size(); seq++)
			
		}//try
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		DBconnector.dropConnection(con);
		//System.out.println("aggregate_time finished!");
	}
	
	public static void amplify_request(String database, String trip_table, String amplified_table, int scale){
		
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
			
			for(int i=0; i<scale; i++){
				if(i==0){
					
					try{
						spt = con.setSavepoint("svpt1");
						String sql="drop table "+amplified_table+"temp;";
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
					
					//int delay=(int)(Math.random()*5)*60;
					try{
						spt = con.setSavepoint("svpt1");
						String sql="create table "+amplified_table+"temp as select row_number() over() as seq, suid, start_gid, start_pos, start_utc, end_gid, end_pos, end_utc, route, stop_rate, valid from "+trip_table+";";
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
						String sql="drop table "+amplified_table+";";
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
						String sql="create table "+amplified_table+" as select seq, suid,start_gid,start_pos,start_utc,end_gid,end_pos,end_utc,route, stop_rate, valid from "+amplified_table+"temp;";
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
						String sql="alter table "+amplified_table+" add column round integer;";
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
				else{
					int delay=(int)(Math.random()*5)*60;
					try{
						spt = con.setSavepoint("svpt1");
						String sql="insert into "+amplified_table+"(seq, suid,start_gid,start_pos,start_utc,end_gid,end_pos,end_utc,route, stop_rate, valid) " +
								"select seq, suid, start_gid, start_pos, start_utc+"+delay+", end_gid, end_pos, end_utc+"+delay+", route, stop_rate, valid from "+amplified_table+"temp;";
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
				
				try{
					spt = con.setSavepoint("svpt1");
					String sql="update "+amplified_table+" set round="+i+" where round is null;";
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
				
			try{
				spt = con.setSavepoint("svpt1");
				String sql="create index "+amplified_table+"_suid_idx on "+amplified_table+"(suid);";
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
				String sql="create index "+amplified_table+"_startutc_idx on "+amplified_table+"(start_utc);";
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
				String sql="create index "+amplified_table+"_suid_startutc_idx on "+amplified_table+"(suid, start_utc);";
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
		DBconnector.dropConnection(con);
		//System.out.println("aggregate_time finished!");
	}

	public static void enhenced_ebksp_considerall(String database, ArrayList<String> time_tables, String aggregated_time_table, String roadmap_table, 
			String trip_table, String footprint_table, String route, String plan_time, int topk, double max_delay, long start_utc, long interval){
		
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;
		//double threshold_rate=max_delay;
		/*
		long pre_time=0;
		long cur_time=0;
		Calendar cal =null;  
		
		double routing_time=0;
		double footprint_time=0;
		double database_time=0;
		double schedule_time=0;*/
		
		DecimalFormat df=new DecimalFormat();
		df.setMaximumFractionDigits(6);

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		int interval_count=time_tables.size();
		int max_interval=interval_count-1;
		long[] start_utcs=new long[interval_count];
		long[] end_utcs=new long[interval_count];
		ArrayList<AllocationRoadsegment[]> time_list= new ArrayList<AllocationRoadsegment[]>();
		ArrayList<Double> average_speeds=new ArrayList<Double>();
		ArrayList<Double> average_lengths=new ArrayList<Double>();
		HashMap<Integer, Double> mp_gid2lane_count = new HashMap<Integer, Double>();
		double average_lane=0;
		
		try{

			stmt = con.createStatement();
			
			try{
				stmt = con.createStatement();
				spt = con.setSavepoint("svpt1");
	    		String sql="select gid, lane_count from "+ roadmap_table + " ;";
	    		System.out.println(sql);
	    		rs=stmt.executeQuery(sql);
	    		int road_count=0;
	    		while(rs.next()){
	    			int gid=rs.getInt("gid");
	    			double lane_count=rs.getDouble("lane_count");
	    			mp_gid2lane_count.put(gid,lane_count);
	    			road_count++;
	    			average_lane+=lane_count;
	    		}
	    		average_lane/=road_count;
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt); 
			}
			
			for(int i=0; i<time_tables.size(); i++){
				start_utcs[i]=start_utc+i*interval;
				end_utcs[i]=start_utcs[i]+interval;
				time_list.add(TripFinder.get_roadlist(database, time_tables.get(i), true));
				
				try{
					stmt = con.createStatement();
					spt = con.setSavepoint("svpt1");
		    		String sql="select avg(average_speed) as avg_speed, avg(length) as avg_length from "+ time_tables.get(i) + " where next_gid is null;";
		    		System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
		    		while(rs.next()){
		    			average_speeds.add(rs.getDouble("avg_speed"));
		    			average_lengths.add(rs.getDouble("avg_length"));
		    		}
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt); 
				}
			}
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ trip_table +" add column "+route+" text;";
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
	    		String sql="alter table "+ trip_table +" add column "+plan_time+" double precision;";
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
	    		String sql="drop table "+ footprint_table +" ;";
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
	    		//store the suid, planed time, gid, time entering the road segment, time getting out of the roadsegment;
	    		String sql="create table "+ footprint_table +"(suid bigint, trip_utc bigint, gid integer, coverage double precision, start_utc bigint, end_utc bigint);";
	    		
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
			
			Comparator<Event> comparator=new EventComparator();
			PriorityQueue<Event> event_list= new PriorityQueue<Event>(10000000, comparator);
			//use the taxi_count to record the current controlled traffic in a given road segment;
			HashMap<Integer, RoadSegment> instant_footprint = new HashMap<Integer, RoadSegment>();
			long total_ebksp=0;
			ArrayList<String> values=new ArrayList<String>();
			ArrayList<String> updates=new ArrayList<String>();
			
			for(int seq=1; seq<time_tables.size(); seq++){
				System.out.println("EBKSP ["+seq+"/"+time_tables.size()+"]: " +
						" total_ebksp="+total_ebksp /*+ " \n routing_time="+(long)routing_time/1000 + " \n database_time="+(long)database_time/1000 
    					+ "\n footprint_time="+(long)footprint_time/1000 + " \n schedule_time="+(long)schedule_time/1000 */);
				total_ebksp=0;
				//get the trip inside the interval
				ArrayList<Trip> trips=new ArrayList<Trip>();
				try{
					spt = con.setSavepoint("svpt1");
					String sql="select seq, suid, start_gid, start_gid, start_pos, start_utc, end_gid, end_pos from "+ trip_table +" where ( valid is null or valid >=0 ) and start_utc>="+start_utcs[seq]+
							" and start_utc<"+end_utcs[seq]+" order by start_utc; ";
					System.out.println(sql);
		    		rs=stmt.executeQuery(sql);
		    		while(rs.next()){
		    			Trip cur_trip=new Trip(rs.getLong("suid"), rs.getInt("start_gid"), rs.getDouble("start_pos"), rs.getLong("start_utc"), rs.getInt("end_gid"), rs.getDouble("end_pos"));
		    			cur_trip.index=rs.getInt("seq");
		    			trips.add(cur_trip);
		    			Event event=new Event(cur_trip.suid, cur_trip.start_utc, cur_trip.start_utc, Event.ROUTING_REQUEST, (trips.size()-1));
		    			event_list.add(event);
		    		}
		    	}
		    	catch (SQLException e) {
					e.printStackTrace();
					con.rollback(spt);
				}
	    		
	    		String plan_roadmap=time_tables.get(seq-1);
	    		TRkShortestPath trksp_instance=new TRkShortestPath();
	    		String restriction_table="oneway_intersection";
	    		//ArrayList<Path> paths=new ArrayList<Path>();
	    		int top_k=topk*2-2;
	    		boolean update_graph=false;
	    		
	    		//AllocationRoadsegment[] time=null;
	    		AllocationRoadsegment[] time=time_list.get(seq);
	    		HashSet<Integer> included_road=new HashSet<Integer>();
	    		TreeMap<Integer, RoadSegment> reduced_footprint = new TreeMap<Integer, RoadSegment>();
	    		/*
	    		cal =Calendar.getInstance();
	    		cur_time=cal.getTime().getTime();
	    		pre_time=cur_time;*/
	    		HashMap<Integer, ArrayList<Path>> paths_hashtable=new HashMap<Integer, ArrayList<Path>>();
	    		
	    		while (event_list.size()!=0 && event_list.peek().utc < end_utcs[seq]){
	    			/*
	    			cal =Calendar.getInstance();
		    		cur_time=cal.getTime().getTime();
		    		schedule_time+=(cur_time-pre_time);
		    		pre_time=cur_time;*/
		    		
	    			Event cur_event=event_list.remove();
	    			//cur_event.print();
	    			/*System.out.println("	[cur_utc="+(long)(cur_event.utc)+" /	"+end_utcs[seq]+"]:" +
	    					" total_ebksp="+total_ebksp + " \n routing_time="+(long)routing_time/1000 + " \n database_time="+(long)database_time/1000 
	    					+ "\n footprint_time="+(long)footprint_time/1000 + " \n schedule_time="+(long)schedule_time/1000 );*/
	    			
	    			if(cur_event.type==Event.REMOVE_LINK){
	    				
	    				/*
	    				cal =Calendar.getInstance();
    		    		cur_time=cal.getTime().getTime();
    		    		pre_time=cur_time;*/
	    				
	    				if(!reduced_footprint.containsKey(cur_event.gid)){
	    					RoadSegment road=new RoadSegment();
    						road.gid=cur_event.gid;
    						road.taxi_count=0;
    						reduced_footprint.put(cur_event.gid, road);
    		    		}
    		    		
	    				reduced_footprint.get(cur_event.gid).taxi_count++;
    		    		
    		    		
    		    		/*
    		    		cal =Calendar.getInstance();
    		    		cur_time=cal.getTime().getTime();
    		    		footprint_time+=(cur_time-pre_time);
    		    		pre_time=cur_time;*/
    		    		
    		    		/*int list_size=cur_event.gid_list.size();
    		    		
    		    		//if need to add a new road segment
    		    		if(list_size!=0){
    		    			int gid=cur_event.gid_list.get(list_size-1);
    		    			cur_event.gid_list.remove(list_size-1);
    		    				
    		    			double next_utc;
    		    			double percent=0;
    		    			if(list_size==1){
    		    				next_utc=time[gid].get_traveltime(cur_event.end_pos, true, -1);
    		    				percent= next_utc/time[gid].get_traveltime(-1);
    		    				next_utc=cur_event.utc+next_utc;
    		    			}
    		    			else{
    		    				percent=1.0;
    		    				next_utc=cur_event.utc+time[gid].get_traveltime(cur_event.gid_list.get(list_size-2));
    		    				
    		    			}
    		    				
    		    			Event event=new Event(cur_event.suid, cur_event.trip_utc, next_utc, Event.REMOVE_LINK, gid, cur_event.gid_list, cur_event.end_pos);
    		    			event_list.add(event);
    		    			
    		    			//record this pass @ this road segment;
    	    				try{
    	    		    		spt = con.setSavepoint("svpt1");
    	    		    		String sql="insert into "+ footprint_table +" values ("+cur_event.suid+", "+cur_event.trip_utc+", "+gid+", "+ percent +", "+(long)cur_event.utc+", "+(long)next_utc+");";
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
	    			else if(cur_event.type==Event.ADD_LINK){
	    				//no event is of this type;
	    			}
	    			else{// cur_event.type==ROUTING_REQUEST
	    				/*
	    				cal =Calendar.getInstance();
    		    		cur_time=cal.getTime().getTime();
    		    		pre_time=cur_time;*/
	    				
	    				int i=cur_event.trip_seq;
	    				Trip cur_trip=trips.get(i);
	    				
	    				ArrayList<Path> path_candidates=new ArrayList<Path>();
	    				String str_path="";
	    				try{
	    			    	spt = con.setSavepoint("svpt1");
	    			    	ArrayList<Path> paths=new ArrayList<Path>();
	    			    	ArrayList<Path_element> path=new ArrayList<Path_element>();
	    			    	
	    			    	if(!paths_hashtable.containsKey(cur_trip.index)){
		    			    	trksp_instance.get_shortest_paths(database, plan_roadmap, restriction_table, update_graph, 
		    			    			cur_trip.start_gid, cur_trip.start_pos, cur_trip.end_gid, cur_trip.end_pos, top_k, max_delay, paths);
		    			    	paths_hashtable.put(cur_trip.index, paths);
	    			    	}
	    			    	else{
	    			    		paths.addAll(paths_hashtable.get(cur_trip.index));
	    			    	}
	    			    	
	    			    	//get the path_candidates;
	    			    	int mode=paths.size();
	    			    	if(mode==0){
	    			    		continue;
	    			    	}
	    			    	if(mode>topk){
	    			    		mode=topk;
	    			    	}
	    			    	
	    			    	// to get N=sum(weighted_footprint);
	    			    	double sum_footprint=0;
	    			    	included_road.clear();
	    			    	for(int index=0; index<mode; index++){
	    			    		path_candidates.add(paths.get(index));
	    			    		Path cur_path =path_candidates.get(index);
	    			    		// to get N=sum(n_i);
	        					for(int k=0; k<cur_path.length() ;k++){
	        						int step_gid=(int)cur_path.path.get(k).edge_id;
	        						//System.out.println(step_gid);
	        						if(included_road.contains(step_gid)){
	        							continue;
	        						}
	        						else{
	        							included_road.add(step_gid);
	        							if(instant_footprint.containsKey(step_gid)){
	        								double ftprt=instant_footprint.get(step_gid).taxi_count;
	        								if(ftprt<0){
	        									System.out.println("instant_footprint(@ gid="+ step_gid + "	)= "+ftprt);
	        								}
	        								double len=time[step_gid].length;
				        					double speed=time[step_gid].get_speed(-1);
	        								sum_footprint+=ftprt*average_speeds.get(seq)*average_lengths.get(seq)*average_lane/(len*speed*mp_gid2lane_count.get(step_gid));
	        							}
	        						}
	        					}
	    			    	}
	    			    	
	    					//chose the shortest one to minimize the road occupancy
	    					if(sum_footprint==0){//just randomly choose one of the candidate 
	    			    		//double seed=Math.random()*path_candidates.size();
	    			    		//path=paths.get((int)seed).path;
	    						path=paths.get(0).path;
        					}
	    					
	    					else{//Use EBkSP to determine which path should be chosen and then add REMOVE_LINK event based simulated time;
	    						
	    						total_ebksp++;
	    						
	    						double min_entropy=1;
		    					int min_path=-1;
		    					
	    						for(int j=0;j<path_candidates.size();j++){
	    							Path cur_path=path_candidates.get(j);
		        					double weighted_fc=0;
		        					double entropy=-1;
	
		        					for(int k=0; k<cur_path.length();k++){
			        					int step_gid=(int)cur_path.path.get(k).edge_id;
			        					int next_gid=-1;
			        					
			        					if(step_gid<=0){
			        						continue;
			        					}
			        					
			        					double count=0;
			        					if(instant_footprint.containsKey(step_gid)){
	        								count=instant_footprint.get(step_gid).taxi_count;
	        								if(count<0){
	        									System.out.println("instant_footprint(@ gid="+ step_gid + "	)= "+count);
	        								}
	        							}
			        					
			        					double len=time[step_gid].length;
			        					double speed=time[step_gid].get_speed(next_gid);
			        						
			        					weighted_fc=count*average_speeds.get(seq)*average_lengths.get(seq)*average_lane/(len*speed*mp_gid2lane_count.get(step_gid));
			        					/*weighted_fc=hashtable.get(step_gid)*average_speeds.get(cur_interval)*average_lengths.get(cur_interval)
			        								/(time[step_gid].length*time[step_gid].get_speed(next_gid));*/
			        					
			        					if(weighted_fc<0){
			        						System.err.println("weighted_fc<0 gid="+step_gid);
			        						System.err.println(count+"*"+average_speeds.get(seq)+"*"+average_lengths.get(seq)+"*"+average_lane+
			        								"/("+len+"*"+speed+"*"+mp_gid2lane_count.get(step_gid)+");");
			        						weighted_fc=0;
			        					}
			        					
			        					weighted_fc=weighted_fc/sum_footprint+1/Math.E;
			        					entropy += weighted_fc * Math.log(weighted_fc)+1/Math.E; //traffic increase will incur entropy to increase and entropy>0
			        					//$entropy += $wt_ftprt*log($wt_ftprt)+exp(-1);
			        					
			        				}
		        					
		        					//determine which is the least popular;
			        				if( min_path == -1 || entropy < min_entropy){
			        					min_entropy = entropy; //get the road that has lease entropy;
			        					min_path = j;
			        				}
		        				}
		        				
		        				//based on EBkSP's popularity determine which one should be chosen:
	    						if(min_path<0){
	    							min_path=0;
	    						}
		        				path=path_candidates.get(min_path).path;
	    					}
	    			    	/*
	    					cal =Calendar.getInstance();
	    		    		cur_time=cal.getTime().getTime();
	    		    		routing_time+=(cur_time-pre_time);
	    		    		pre_time=cur_time;*/
	    					
	    			    	//use the final path to update the instant_traffic;
	    			    	cur_trip.path.clear();
	    			    	double cost=0.0;
	    			    	for(int j=path.size()-1; j>=0; j--){
	    			    		Path_element node=path.get(j);
	    			    		int gid=(int)node.edge_id;
	    			    		if(gid>0){
	    			    			cur_trip.path.add(gid);
	    			    			cost+=node.cost;
	    			    			str_path=node.edge_id+","+str_path;
	    			    			
	    			    			if(instant_footprint.containsKey(gid)){
	    	    						instant_footprint.get(gid).taxi_count++;
	    	    					}
	    	    					else{
	    	    						RoadSegment road=new RoadSegment();
	    	    						road.gid=gid;
	    	    						road.taxi_count=1;
	    	    						instant_footprint.put(gid, road);
	    	    					}
	    			    		}
	    			    	}
	    			    	cur_trip.cost=cost;
	    			    	trips.set(i, cur_trip);
	    			    	
	    			    	/*cal =Calendar.getInstance();
	    		    		cur_time=cal.getTime().getTime();
	    		    		footprint_time+=(cur_time-pre_time);
	    		    		pre_time=cur_time;*/
	    		    		
	    			    }
	    			    catch (Exception e) {
	    					e.printStackTrace();
	    					cur_trip.cost=-1;
	    					cur_trip.path.clear();
	    					trips.set(i, cur_trip);
	    				}
	    					
	    				if(cur_trip!=null && cur_trip.cost>0 && cur_trip.path!=null && cur_trip.path.size()!=0){
	    					
	    					//add the routing decision to database;
	    					String newsql="update "+ trip_table +" set "+route+"='"+str_path+"', "+plan_time+"="+cur_trip.cost+" where suid=" +cur_trip.suid +" and start_utc="+ cur_trip.start_utc+";";
				    		//System.out.println("["+i+"/"+trips.size()+"]:"+sql);
				    		updates.add(newsql);
				    		if(updates.size()>200){
				    			String sql="";
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
					    	
					    	//calculate the occupancy and store in database
					    	double cur_utc=cur_trip.start_utc;
				    		int cur_interval= (int)(seq+((int)(cur_utc-start_utcs[seq]))/interval);
				    		int pre_interval=cur_interval;
				    		AllocationRoadsegment[] roadtime=time_list.get(cur_interval);
				    		
				    		int trip_size=cur_trip.path.size();
					    	for(int j=0; j<trip_size; j++){
					    		int cur_gid = cur_trip.path.get(j);
					    		int next_gid = -1;
					    		if(j<trip_size-1){
					    			next_gid = cur_trip.path.get(j+1);
					    		}
					    		
					    		//determine which interval is this travel located;
					    		cur_interval= (int)(seq+((int)(cur_utc-start_utcs[seq]))/interval);
					    		if(cur_interval>max_interval){
					    			cur_interval=max_interval;
					    		}
					    		if(cur_interval!=pre_interval){
					    			roadtime=time_list.get(cur_interval);
					    			pre_interval=cur_interval;
					    		}
					    		
					    		double segment_time=0;
					    		double percent=0;
					    		if(j==0 && trip_size==1){
					    			segment_time=roadtime[cur_gid].get_traveltime(next_gid)*Math.abs(cur_trip.start_pos-cur_trip.end_pos);
					    			percent=Math.abs(cur_trip.start_pos-cur_trip.end_pos);
					    		}
					    		else if(j==0){
					    			segment_time=roadtime[cur_gid].get_traveltime(cur_trip.start_pos, false, next_gid);
					    			percent=segment_time/roadtime[cur_gid].get_traveltime(next_gid);
					    		}
					    		else if(j==trip_size-1){
					    			segment_time=roadtime[cur_gid].get_traveltime(cur_trip.end_pos, true, next_gid);
					    			percent=segment_time/roadtime[cur_gid].get_traveltime(next_gid);
					    		}
					    		else{
					    			segment_time=roadtime[cur_gid].get_traveltime(next_gid);
					    			percent=1.0;
					    		}
					    		if(segment_time<0){
					    			//do nothing, because this still helps to provide the traffic count on give road segment;
					    			segment_time=1;
					    		}
					    		
					    		//add new event in to the simulator
					    		double delete_utc=cur_utc+segment_time;
					    		//dobule delete_utc=cur_utc+cur_trip.cost;
					    		Event event=new Event(cur_trip.suid, cur_trip.start_utc, delete_utc, Event.REMOVE_LINK, cur_gid);
					    		event_list.add(event);
					    		
					    		//install into database
					    		double next_utc=cur_utc+segment_time;
					    		values.add("("+cur_trip.suid+", "+cur_trip.start_utc+", "+cur_gid+", "+percent+", "+(long)cur_utc+", "+(long)(next_utc)+")");
					    		cur_utc=next_utc;
					    	}
	    					
					    	if(values.size()>400){
					    		String sql="insert into "+ footprint_table +" values ";
						    	try{
		    	    		    	spt = con.setSavepoint("svpt1");
		    	    		    	for(int vi=0; vi<values.size(); vi++){
		    	    		    		sql+=values.get(vi);
		    	    		    		if(vi!=values.size()-1){
		    	    		    			sql+=",\n";
		    	    		    		}
		    	    		    		else{
		    	    		    			sql+=";";
		    	    		    		}
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
		    	    				values.clear();
		    	    			}
					    	}
					    	
					    	/*
					    	cal =Calendar.getInstance();
	    		    		cur_time=cal.getTime().getTime();
	    		    		database_time+=(cur_time-pre_time);
	    		    		pre_time=cur_time;*/
	    		    		
	    					/* previus solution
	    					//if need to add a new event and install the occupancy in database;
	    					int list_size=cur_trip.path.size();
	    					int cur_gid=0;
	    		    		
	    		    		if(list_size>0){
	    		    			cur_gid = cur_trip.path.get(list_size-1);
	    		    			double next_utc=0.0;
	    		    			Event event=new Event(cur_event.suid, cur_event.trip_utc, next_utc, Event.REMOVE_LINK, cur_gid, cur_trip.path, cur_trip.end_pos);
	    		    			event.gid_list.remove(list_size-1);
	    		    			
	    		    			double percent=0;
	    		    			if(list_size==1){
	    		    				percent=Math.abs(trips.get(cur_event.trip_seq).start_pos-cur_event.end_pos);
	    		    				next_utc=cur_event.utc+time[cur_gid].get_traveltime(-1)*percent;
	    		    			}
	    		    			else{
	    		    				next_utc=time[cur_gid].get_traveltime(trips.get(cur_event.trip_seq).start_pos, false, event.gid_list.get(list_size-2));
	    		    				percent=next_utc/time[cur_gid].get_traveltime(event.gid_list.get(list_size-2));
	    		    				next_utc=cur_event.utc+next_utc;
	    		    			}
	    		    			event.utc=next_utc;	
	    		    			event_list.add(event);
	    		    			
	    		    			//record this pass @ this road segment;
	    	    				try{
	    	    		    		spt = con.setSavepoint("svpt1");
	    	    		    		String sql="insert into "+ footprint_table +" values ("+event.suid+", "+event.trip_utc+", "+cur_gid+", "+ percent +", "+(long)event.trip_utc+", "+(long)next_utc+");";
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
	    				
	    			}// cur_event.type==ROUTING_REQUEST	
	    			
    			}//while (event_list.size()!=0 && event_list.peek().utc < end_utcs[seq]);
	    		
	    		Iterator<Integer> it=reduced_footprint.keySet().iterator();
	    		while(it.hasNext()){
	    			int gid=(it.next()).intValue();
	    			double ref_count=reduced_footprint.get(gid).taxi_count;
	    			if(ref_count>0 && !instant_footprint.containsKey(gid)){
	    				System.err.println("REDUCE TRAFFIC FROM A ROAD THAT HAS NO TRAFFIC");
	    				continue;
	    			}
	    			else if(ref_count>0 && instant_footprint.get(gid).taxi_count<ref_count){
	    				System.err.println("REDUCE MORE TRAFFIC THAN IT HAS");
		    			ref_count=instant_footprint.get(gid).taxi_count;
	    			}
	    			instant_footprint.get(gid).taxi_count-=ref_count;
	    		}
    		}//for(int seq=0; seq<time_tables.size(); seq++)
			
			if(updates.size()>0){
				String sql="";
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
			
			if(values.size()>0){
				String sql="insert into "+ footprint_table +" values ";
		    	try{
    		    	spt = con.setSavepoint("svpt1");
    		    	
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
    		    	System.err.println(sql);
    				e.printStackTrace();
    				con.rollback(spt);
    			}
    			finally{
    				con.commit();
    				values.clear();
    			}
	    	}
			
		}//try
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
				
				String database="mydb";
				String aggregated_time_table="ring5_traffictime";
				String trip_table="ring5_all_trip";
				String roadmap_table="ring5_roads_initspeed";
				String footprint_table="ring5_footprint_pre";
				
				double max_tempstop_precent=1;
				int min_trip_interval=60;
				
				/*String trip_extention="_";
				trip_extention+=(int)(max_tempstop_precent*10);
				trip_extention+="_"+min_trip_interval;
				//trip_table+=trip_extention;*/
				
				long cur_utc=1231172100L; //1231171200L is min(utc)
				long period=900L;
				int base_interval=2;
				
				int start_interval=2;
				cur_utc+=(start_interval-base_interval)*period;
				ArrayList<String> time_tables=new ArrayList<String>();
				for(int i=start_interval; i<=54; i++){
					String cur_time_table=aggregated_time_table+""+i;
					String routable_time_table="routable_"+cur_time_table;
					time_tables.add(routable_time_table);
					//PathPlanner.backup_and_routable(database, cur_time_table, routable_time_table, roadmap_table);
				}
				
				long pre_time=0;
				long cur_time=0;
				Calendar cal =null;  
				
				//PathPlanner.make_time_table_routable(database, aggregated_time_table, roadmap_table);
				cal =Calendar.getInstance();
				cur_time=cal.getTime().getTime();
				pre_time=cur_time;
				
				if(type.compareTo("trsp_1")==0){
					int scale=1;
					String amplified_table=trip_table+"_"+scale;
					String amplified_footprint=footprint_table+"_"+scale;
					//PathPlanner.amplify_request(database, trip_table, amplified_table, scale);
					
					System.out.println("Starting trsp ....");
					PathPlanner.plan_shortest_path(database, time_tables, aggregated_time_table, roadmap_table, amplified_table, 
							amplified_footprint+"_trsp", cur_utc, period);
					cal =Calendar.getInstance();
					cur_time=cal.getTime().getTime();
					System.out.print("TIME_ON_SP:"+(cur_time-pre_time));
					pre_time=cur_time;
				}
				
				if(type.compareTo("trsp_3")==0){
					
					int scale=3;
					String amplified_table=trip_table+"_"+scale;
					String amplified_footprint=footprint_table+"_"+scale;
					//PathPlanner.amplify_request(database, trip_table, amplified_table, scale);
					
					System.out.println("Starting trsp ....");
					PathPlanner.plan_shortest_path(database, time_tables, aggregated_time_table, roadmap_table, amplified_table, 
							amplified_footprint+"_trsp", cur_utc, period);
					cal =Calendar.getInstance();
					cur_time=cal.getTime().getTime();
					System.out.print("TIME_ON_SP:"+(cur_time-pre_time));
					pre_time=cur_time;
				}
				
				if(type.compareTo("trsp_6")==0){
					int scale=6;
					String amplified_table=trip_table+"_"+scale;
					String amplified_footprint=footprint_table+"_"+scale;
					//PathPlanner.amplify_request(database, trip_table, amplified_table, scale);
					
					System.out.println("Starting trsp ....");
					PathPlanner.plan_shortest_path(database, time_tables, aggregated_time_table, roadmap_table, amplified_table, 
							amplified_footprint+"_trsp", cur_utc, period);
					cal =Calendar.getInstance();
					cur_time=cal.getTime().getTime();
					System.out.print("TIME_ON_SP:"+(cur_time-pre_time));
					pre_time=cur_time;
				}
				
				int k=3;
				double max_delay=1.5;
				
				if(type.compareTo("rksp")==0){
					System.out.println("Starting rksp ....");
					PathPlanner.random_k_shortest_path(database, time_tables, aggregated_time_table, roadmap_table, trip_table, 
							footprint_table+"_rksp", k, max_delay, cur_utc, period);
					cal =Calendar.getInstance();
					cur_time=cal.getTime().getTime();
					System.out.print("TIME_ON_RKSP:"+(cur_time-pre_time));
					pre_time=cur_time;
				}
				
				/*
				PathPlanner.entropy_based_ksp("mydb", time_tables, aggregated_time_table, roadmap_table, trip_table, footprint_table, cur_utc, period);
				cal =Calendar.getInstance();
				cur_time=cal.getTime().getTime();
				System.out.print("TIME_ON_EBKSP:"+(cur_time-pre_time));
				pre_time=cur_time;*/
				
				if(type.compareTo("eebksp")==0){
					System.out.println("Starting eebksp ....");
					PathPlanner.enhenced_ebksp("mydb", time_tables, aggregated_time_table, roadmap_table, trip_table, 
							footprint_table+"_eebksp", "eebksp", "eebksp_plan", k, max_delay, cur_utc, period);
					cal =Calendar.getInstance();
					cur_time=cal.getTime().getTime();
					System.out.print("TIME_ON_EEBKSP:"+(cur_time-pre_time));
					pre_time=cur_time;
				}
				
				
				if(type.compareTo("eebksp_considerall_1")==0){
					int scale=1;
					//footprint_table="ring5_footprint_pre";
					String amplified_table=trip_table+"_"+scale;
					String amplified_footprint=footprint_table+"_"+scale;
					PathPlanner.amplify_request(database, trip_table, amplified_table, scale);
					
					k=4;
					max_delay=1.5;
					
					System.out.println("Starting EEBKSP_CONSIDERALL ....");
					PathPlanner.enhenced_ebksp_considerall(database, time_tables, aggregated_time_table, roadmap_table, amplified_table, 
							amplified_footprint+"_eebksp_considerall", "eebksp_considerall", "eebksp_considerallplan", k, max_delay, cur_utc, period);
					cal =Calendar.getInstance();
					cur_time=cal.getTime().getTime();
					System.out.print("TIME_ON_EEBKSP_CONSIDERALL:"+(cur_time-pre_time));
					pre_time=cur_time;
				}
				
				if(type.compareTo("eebksp_considerall_3")==0){
					int scale=3;
					//footprint_table="ring5_footprint_pre";
					String amplified_table=trip_table+"_"+scale;
					String amplified_footprint=footprint_table+"_"+scale;
					//PathPlanner.amplify_request(database, trip_table, amplified_table, scale);
					
					k=4;
					max_delay=1.5;
					
					System.out.println("Starting EEBKSP_CONSIDERALL ....");
					PathPlanner.enhenced_ebksp_considerall(database, time_tables, aggregated_time_table, roadmap_table, amplified_table, 
							amplified_footprint+"_eebksp_considerall", "eebksp_considerall", "eebksp_considerallplan", k, max_delay, cur_utc, period);
					cal =Calendar.getInstance();
					cur_time=cal.getTime().getTime();
					System.out.print("TIME_ON_EEBKSP_CONSIDERALL:"+(cur_time-pre_time));
					pre_time=cur_time;
				}
				
				if(type.compareTo("eebksp_considerall_6")==0){
					//footprint_table="ring5_footprint_pre";
					int scale=6;
					String amplified_table=trip_table+"_"+scale;
					String amplified_footprint=footprint_table+"_"+scale;
					PathPlanner.amplify_request(database, trip_table, amplified_table, scale);
					
					k=4;
					max_delay=1.5;
					
					System.out.println("Starting EEBKSP_CONSIDERALL ....");
					PathPlanner.enhenced_ebksp_considerall(database, time_tables, aggregated_time_table, roadmap_table, amplified_table, 
							amplified_footprint+"_eebksp_considerall", "eebksp_considerall", "eebksp_considerallplan", k, max_delay, cur_utc, period);
					cal =Calendar.getInstance();
					cur_time=cal.getTime().getTime();
					System.out.print("TIME_ON_EEBKSP_CONSIDERALL:"+(cur_time-pre_time));
					pre_time=cur_time;
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else{
			try {
				
				String database="mydb";
				String aggregated_time_table="ring5_traffictime";
				String trip_table="ring5_all_trip";
				String amplified_table="ring5_all_trip";
				String roadmap_table="ring5_roads_initspeed";
				String footprint_table="ring5_footprint_pre";
				
				double max_tempstop_precent=1;
				int min_trip_interval=60;
				
				/*String trip_extention="_";
				trip_extention+=(int)(max_tempstop_precent*10);
				trip_extention+="_"+min_trip_interval;
				//trip_table+=trip_extention;*/
				
				long cur_utc=1231172100L; //1231171200L is min(utc)
				long period=900L;
				int base_interval=2;
				
				int start_interval=2;
				cur_utc+=(start_interval-base_interval)*period;
				ArrayList<String> time_tables=new ArrayList<String>();
				for(int i=start_interval; i<=54; i++){
					String cur_time_table=aggregated_time_table+""+i;
					String routable_time_table="routable_"+cur_time_table;
					time_tables.add(routable_time_table);
					//PathPlanner.backup_and_routable(database, cur_time_table, routable_time_table, roadmap_table);
				}
				
				long pre_time=0;
				long cur_time=0;
				Calendar cal =null;  
				
				//PathPlanner.make_time_table_routable(database, aggregated_time_table, roadmap_table);
				cal =Calendar.getInstance();
				cur_time=cal.getTime().getTime();
				pre_time=cur_time;
				
				int scale=3;
				amplified_table=trip_table+"_"+scale;
				String amplified_footprint=footprint_table+"_"+scale;
				//PathPlanner.amplify_request(database, trip_table, amplified_table, scale);
				
				int k=4;
				double max_delay=1.5;
				/*
				System.out.println("Starting EEBKSP_CONSIDERALL ....");
				PathPlanner.enhenced_ebksp_considerall(database, time_tables, aggregated_time_table, roadmap_table, amplified_table, 
						amplified_footprint+"_eebksp_considerall", "eebksp_considerall", "eebksp_considerallplan", k, max_delay, cur_utc, period);
				cal =Calendar.getInstance();
				cur_time=cal.getTime().getTime();
				System.out.print("TIME_ON_EEBKSP_CONSIDERALL:"+(cur_time-pre_time));
				pre_time=cur_time;*/
				
				
				/*PathPlanner.plan_shortest_path(database, time_tables, aggregated_time_table, roadmap_table, trip_table, footprint_table+"_trsp", cur_utc, period);
				cal =Calendar.getInstance();
				cur_time=cal.getTime().getTime();
				System.out.print("TIME_ON_SP:"+(cur_time-pre_time));
				pre_time=cur_time;*/
				
				/*int k=3;
				double max_delay=1.5;
				PathPlanner.random_k_shortest_path(database, time_tables, aggregated_time_table, roadmap_table, trip_table, footprint_table+"_rksp", k, max_delay, cur_utc, period);
				cal =Calendar.getInstance();
				cur_time=cal.getTime().getTime();
				System.out.print("TIME_ON_RKSP:"+(cur_time-pre_time));
				pre_time=cur_time;*/
				
				
				
				/*
				PathPlanner.entropy_based_ksp("mydb", time_tables, aggregated_time_table, roadmap_table, trip_table, footprint_table, cur_utc, period);
				cal =Calendar.getInstance();
				cur_time=cal.getTime().getTime();
				System.out.print("TIME_ON_EBKSP:"+(cur_time-pre_time));
				pre_time=cur_time;*/
				//trip_table="ring5_trip";
				/*PathPlanner.enhenced_ebksp("mydb", time_tables, aggregated_time_table, roadmap_table, trip_table, footprint_table+"_eebksp", k, max_delay, cur_utc, period);
				cal =Calendar.getInstance();
				cur_time=cal.getTime().getTime();
				System.out.print("TIME_ON_EEBKSP:"+(cur_time-pre_time));
				pre_time=cur_time;*/
					
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}