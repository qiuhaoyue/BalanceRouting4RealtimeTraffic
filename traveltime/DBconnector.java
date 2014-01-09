import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.TimeZone;
import java.lang.Math;
 
public class DBconnector {
	
	public static final double sigma_with_stop=6.636350348;
	public static final double sigma_without_stop=4.07;
	public static final double closest_beta=26.7127971075959;
	public static final double shortest_beta=2.7804772984;
	
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
	
	public static double getMatchWeight(double distance){
		
		double sigma=DBconnector.sigma_without_stop;
		
		return (-Math.log(Math.sqrt(2*Math.PI)*sigma)-0.5*(distance/sigma)*(distance/sigma));
	}
	
	
	public static double getTransitionWeight(double distance, double interval){

		double beta=(DBconnector.closest_beta+DBconnector.shortest_beta)*0.75;	
		double unit_time=120.0;
		
		beta=beta*interval/unit_time;
		
		return -(distance)/beta-Math.log(beta);
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
	
	public static void clear(String database, String sample_table){
		Connection con = null;
		Statement stmt = null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    String sql="update " + sample_table +" SET gid=null , edge_offset=null, route=null";
		    //System.out.println(sql);
		    stmt.executeUpdate(sql);
		    sql="update " + sample_table +" SET stop="+Sample.MOVING+" where stop is null or stop!="+Sample.LONG_STOP;
		    //System.out.println(sql);
		    stmt.executeUpdate(sql);

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
		System.out.println("clear finished!");
	}
	
	
	public static void mapMatching(String database, String suid, String sample_table, String roadmap_table, String intersection_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt1;
		double distance_threshod=14.0; //paper sets this value as 200
		double temp_stop_threshold=13.0;
		int number_threshold=5; //paper sets this value as 5
		//int candidate_count_threshold=10;
		double unit_time=120.0;
		double time_threshold=330.0;
		double fade_factor=1.0;
		double travel_threshold=900;
		double default_priority=1.5;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
		    stmt = con.createStatement();
		    
		    
		    //prepare the intersection_table
		    String sql=//"DROP TABLE match_temp; \n"+
	    			"CREATE TABLE match_temp(to_cost float8, target_id int4, via_path text); \n"+
	    			"INSERT INTO match_temp SELECT to_cost, to_way AS target_id, from_way AS via_path FROM "+intersection_table+" where to_way IS NOT NULL and from_way IS NOT NULL; \n";
	    	//System.out.println(sql);
	    	//stmt.executeUpdate(sql);
		    
		    sql="select * from " + sample_table +" where suid = " +suid + " and (stop !=1 or stop is null)order by utc";
		    System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    ArrayList<Sample> trajectory= new ArrayList<Sample>();
		    while(rs.next()){
		    	trajectory.add(new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getLong("lat"), 
		    					rs.getLong("lon"), (int)rs.getLong("head"), rs.getLong("speed"), rs.getLong("distance")));
		    }
		    
		    ArrayList<ArrayList<MatchEntry>> match_matrix = new ArrayList<ArrayList<MatchEntry>>();
		    ArrayList<MatchEntry> pre_match_candidates=null, cur_match_candidates=null;
		    Sample cur_sample=null, pre_sample=null;
		    
		    for(int i=0; i<trajectory.size(); i++){
		    	pre_sample=cur_sample;
		    	cur_sample=trajectory.get(i);
		    	
		    	if(i==98){
		    		System.out.println(cur_sample.toString()); 
		    	}
			    spt1 = con.setSavepoint("svpt1");
			    pre_match_candidates = cur_match_candidates;
			    cur_match_candidates = new ArrayList<MatchEntry>();
			    
			    //Date time1=new Date();
			    
			    try{
			    	// find the segment whose distance to cur_sample is smaller than the threshold;
			    	sql="SELECT tmp.gid, tmp.x1, tmp.y1, tmp.x2, tmp.y2, tmp.offset, tmp.dist, " +
			    			"ST_AsText(ST_Transform(ST_Line_Interpolate_Point(ST_Transform(tmp.the_geom,26986), tmp.offset), 4326)) AS point \n" +
			    			"FROM (SELECT gid, x1, y1, x2, y2, the_geom, " +
			    						"ST_Distance_Sphere(the_geom, ST_GeometryFromText('POINT(" + cur_sample.lon + " "+cur_sample.lat + ")', 4326)) AS dist, "+  
			    						"ST_Line_Locate_Point(the_geom, ST_GeometryFromText('POINT(" + cur_sample.lon + " "+cur_sample.lat + ")', 4326)) AS offset "+
			    				  " FROM "+ roadmap_table +" " +
			    				  "WHERE the_geom && ST_setsrid(" +
		            					"'BOX3D("+ (cur_sample.lon-0.05)+ " " + (cur_sample.lat-0.05) + "," + (cur_sample.lon+0.05)+ " " + (cur_sample.lat + 0.05)
		            					+ ")'::box3d, 4326)) as tmp \n" +
		            		" WHERE tmp.dist < "+distance_threshod+" ORDER BY tmp.dist limit "+ number_threshold;
			    	/*
			    	select tmp.gid, tmp.x1, tmp.y1, tmp.x2, tmp.y2, tmp.offset, tmp.dist,
						ST_AsText(ST_Transform(ST_Line_Interpolate_Point(ST_Transform(tmp.the_geom,26986), tmp.offset), 4326)) AS point

					from (SELECT gid, x1, y1, x2, y2, the_geom, 
						ST_Distance_Sphere(the_geom, ST_GeometryFromText('POINT(116.35294938575355 39.9391357353164)', 4326)) AS dist,
						ST_Line_Locate_Point(the_geom,ST_GeometryFromText('POINT(116.35294938575355 39.9391357353164)', 4326)) AS offset
						FROM ways  
						WHERE the_geom && ST_setsrid(
						                   'BOX3D(116.25294938575 
						                          39.839135735316, 
						                          116.45294938575 
						                          40.039135735316)'::box3d, 4326)) as tmp
					where tmp.dist< 50
					ORDER BY tmp.dist LIMIT 10
			    	*/
			    	
			    	//System.out.println(sql);
			    	rs = stmt.executeQuery(sql);
			    	
			    	double dynamic_threshold=-1.0;
			    	//double min_matching_dis=-1.0;
			    	while(rs.next()){
			    		//"POINT(116.352949508079 39.9391208659371)"
			    		String point= rs.getString("point");
			    		int start=point.indexOf('(');
			    		int end=point.indexOf(')');
			    		int mid=point.indexOf(' ');
			    		double p_x=Double.parseDouble(point.substring(start+1, mid));
			    		double p_y=Double.parseDouble(point.substring(mid+1, end));	
			    		
			    		double distance = rs.getDouble("dist");
			    		
			    		if(dynamic_threshold < 0 || distance < dynamic_threshold){
				    		MatchEntry newEntry=new MatchEntry(
				    				new RoadSegment(rs.getInt("gid"), rs.getDouble("x1"), rs.getDouble("y1"), rs.getDouble("x2"), rs.getDouble("y2")),
				    				rs.getDouble("offset"),
				    				p_x,
				    				p_y,
				    				distance);
				    		
				    		newEntry.matching_weight=DBconnector.getMatchWeight(cur_sample.distance);
				    		//newEntry.final_weight=DBconnector.getMatchWeight(cur_sample.distance);
			    		
				    		if( cur_sample.min_matching_distance<0 ){
				    			cur_sample.min_matching_distance=distance;
				    			if(distance < 2.5){
				    				//dynamic_threshold=25.0;
				    			}
				    			else{
				    				//dynamic_threshold=distance*10;
				    			}
				    		}
				    		//System.out.println(newEntry.toString());
				    		cur_match_candidates.add(newEntry);
			    		}
				    }
			    	
			    	//Date time2=new Date();
			    	//System.out.println("Project Time:"+(time2.getTime()-time1.getTime()));
			    	
			    	if(cur_sample.min_matching_distance >=0 ){
			    		System.out.println("SAMPLE["+i+"],"+cur_sample.toString());
			    	}
			    	
			    	MatchEntry cur_candidate=null, pre_candidate=null;
			    	if(i!=0 && pre_match_candidates != null && !pre_match_candidates.isEmpty()){
			    		//boolean route_success=true;
			    		
			    		sql="select ST_Distance_Sphere(ST_GeometryFromText('POINT(" +
			    				pre_sample.lon + " " + pre_sample.lat + ")', 4326),"+
			    				"ST_GeometryFromText('POINT(" +
			    				cur_sample.lon + " " + cur_sample.lat + ")', 4326));";
			    		//System.out.println(sql);
			    		rs = stmt.executeQuery(sql);
		    			double distance=0.0;
				    	while(rs.next()){
				    		distance=rs.getDouble("st_distance_sphere");
					    }
				    	double circle_distance=distance;
				    	
				    	//store the moving distance to the previous sample, if the distance is too small then don't need to calculate the path;
				    	cur_sample.moving_distance=distance;
				    	trajectory.set(i, cur_sample);
			    		
				    	if( cur_sample.moving_distance >=0 && cur_sample.moving_distance < temp_stop_threshold ){
				    		for(int j=0; j<cur_match_candidates.size();j++ ){
				    			cur_candidate=cur_match_candidates.get(j);
				    			cur_candidate.final_weight=cur_candidate.matching_weight;
				    			cur_match_candidates.set(j, cur_candidate);
				    		}
				    		match_matrix.add(cur_match_candidates);
				    		continue;
				    	}
				    	
				    	//else if the moving_distance in considerable to calculate the path
			    		for(int j=0; j<cur_match_candidates.size();j++ ){
				    		cur_candidate=cur_match_candidates.get(j);
				    		//System.out.println("Current Candidate:	"+cur_candidate.toString());
				    		//Date time3=new Date();
				    		boolean no_predecessor=true;
				    		
				    		for(int k=0; /*route_success &&*/ k<pre_match_candidates.size();k++){
				    			pre_candidate=pre_match_candidates.get(k);
				    			if(pre_candidate.final_weight>0){//cannot get to the pre_candidate
				    				continue;
						    	}
				    			else{
				    				no_predecessor=false;
				    			}
				    			Savepoint spt2 = con.setSavepoint("svpt1");
				    			
				    			/*
				    			sql="DROP TABLE match_temp; \n"+
				    			"CREATE TABLE match_temp(to_cost float8, target_id int4, via_path text); \n"+
				    			"INSERT INTO match_temp SELECT to_cost, to_way AS target_id, from_way AS via_path FROM "+intersection_table+" where to_way IS NOT NULL and from_way IS NOT NULL; \n";
				    			//System.out.println(sql);
				    			stmt.executeUpdate(sql);*/
				    			
				    			sql="SELECT * FROM pgr_trsp('select gid as id, source, target, to_cost as cost, reverse_cost from "+roadmap_table+"', "+
				    			pre_candidate.road.gid + ", " + pre_candidate.offset + ", " + cur_candidate.road.gid + ", " + cur_candidate.offset + ", " +
				    			"true, true, 'SELECT to_cost, target_id, via_path FROM match_temp') order by seq;";
				    			/*
				    			DROP TABLE match_temp;
								CREATE TABLE match_temp(to_cost float8, target_id int4, via_path text);
								INSERT INTO match_temp SELECT to_cost, to_edge AS target_id, from_edge AS via_path FROM intersections where to_edge IS NOT NULL;
								SELECT * FROM pgr_trsp('select gid as id, source, target, to_cost as cost, reverse_cost from ways',
															29408,
															0.0,
															33518,
															0.1,
															true,
															true,
															'SELECT to_cost, target_id, via_path FROM match_temp'
														)
				    			*/
				    			//System.out.println("try to match with:	" +pre_candidate.toString());
				    			//System.out.println(sql);
				    			try{
				    				rs = stmt.executeQuery(sql);
					    			double cost=0.0;
					    			String path="";
					    			
							    	while(rs.next()){
							    		//"POINT(116.352949508079 39.9391208659371)"
							    		cost+=rs.getDouble("cost");
							    		path+=""+rs.getLong("id2")+",";
								    }
							    	
							    	cost = Math.abs(cost*1000-circle_distance);
							    	double timediff=Math.abs((cur_sample.utc.getTime()-pre_sample.utc.getTime())/1000);
							    	
							    	if( timediff >= time_threshold || cost > travel_threshold*timediff/unit_time ){
							    		continue;
							    	}
							    	else{
								    	double weight = DBconnector.getTransitionWeight(cost, timediff) + (pre_candidate.final_weight)*fade_factor;
								    	if( weight<0 && (cur_candidate.transit_weight >0 || weight > cur_candidate.transit_weight)){
								    		cur_candidate.transit_weight = weight;
								    		cur_candidate.pre_entry_index = k;
								    		cur_candidate.path=path;
								    	}
							    	}
				    			}
				    			catch (SQLException e) {
							    	con.rollback(spt2);
								    //e.printStackTrace();
								    System.err.println(e.getMessage());
								    //route_success=false;
								    break;
							    }
						    	
				    		}//end for: pre_match_candidates
				    		
				    		//Date time4=new Date();
					    	//System.out.println("ROUTE Time:"+(time4.getTime()-time3.getTime()));
				    		
				    		if(no_predecessor){
				    			cur_candidate.final_weight=cur_candidate.matching_weight;
				    		}
				    		else{
				    			if(cur_candidate.transit_weight<0){
				    				cur_candidate.final_weight=cur_candidate.matching_weight+cur_candidate.transit_weight;
				    				
				    			}
				    		}
				    		cur_match_candidates.set(j, cur_candidate);
				    	}
			    	}//end for transit_weight cur_match_candidates
			    	else{//if don't have pre_sample
			    		for(int j=0; j<cur_match_candidates.size();j++ ){
			    			cur_candidate=cur_match_candidates.get(j);
			    			cur_candidate.final_weight=cur_candidate.matching_weight;
			    			cur_match_candidates.set(j, cur_candidate);
			    		}
			    	}
			    	match_matrix.add(cur_match_candidates);
			    }
			    catch (SQLException e) {
			    	con.rollback(spt1);
				    e.printStackTrace();
			    }
			    finally{
			    	con.commit();
			    }
		    }//finished updating the weight for the given taxi's all samples
		    
		    //use VITERBI algorithm to determine the most possible trajectory
		    /*for(int i=trajectory.size()-1; i>=1; i--){
		    	cur_match_candidates=match_matrix.get(i);
		    	
		    	MatchEntry best_candidate=null;
		    	for(int j=0;j<cur_match_candidates.size(); j++){
		    		MatchEntry cur_candidate=cur_match_candidates.get(j);
		    		if((cur_candidate.final_weight<0) && (best_candidate==null || cur_candidate.final_weight > best_candidate.final_weight)){
		    			best_candidate = cur_candidate;
		    		}
		    	}
		    	if(cur_match_candidates.size()<1){
		    		//System.out.println("Sample["+i+"]: No matching candidate!");
		    	}
		    	else if(best_candidate!=null){
		    		//System.out.println("Weight["+i+"]: "+best_candidate.final_weight);
		    	}
		    	else{
		    		//System.out.println("Weight["+i+"]: 1.0");
		    	}
		    }*/
		    
		    //boolean first_sample=true;
		    for(int i=trajectory.size()-1; i>=1; i--){
		    	
		    	//first_sample=true;
		    	//find the best candidate with largest final
		    	if(i==98){
    				//System.out.println();
    			}
		    	cur_match_candidates=match_matrix.get(i);
		    	
		    	MatchEntry best_candidate=null;
		    	for(int j=0;j<cur_match_candidates.size(); j++){
		    		MatchEntry cur_candidate=cur_match_candidates.get(j);
		    		//cur_candidate is valid and final_weight is maximum
		    		if((cur_candidate.final_weight<0) && (best_candidate==null || cur_candidate.final_weight > best_candidate.final_weight)){
		    			best_candidate = cur_candidate;
		    		}
		    	}
		    	
		    	//trace back to the predecessors
		    	cur_match_candidates=match_matrix.get(i-1);
		    	while( best_candidate!=null /*&& best_candidate.pre_entry_index >= 0 && best_candidate.pre_entry_index < cur_match_candidates.size()*/){
		    		//write_into_database best_candidate;
		    		//first_sample=false;
		    		cur_sample=trajectory.get(i);
		    		Savepoint spt3 = con.setSavepoint("svpt3");
		    		try{	    			
		    			sql="UPDATE "+ sample_table +" SET gid="+ best_candidate.road.gid +", edge_offset=" + best_candidate.offset +", route='"+ best_candidate.path +"' ";
		    			if(cur_sample.moving_distance>=0 && cur_sample.moving_distance<=temp_stop_threshold){
			    			sql+= ", stop="+Sample.TEM_STOP;
			    		}
		    			sql+= " WHERE suid="+suid+" and utc="+(int)(trajectory.get(i).utc.getTime()/1000);
		    			System.out.println(i+": "+sql);
		    			stmt.executeUpdate(sql);
		    		}
		    		catch (SQLException e) {
		    			con.rollback(spt3);
		    			e.printStackTrace();
		    		}
		    		//find the next candidate
		    		finally{
		    			con.commit();
		    			
			    		if(best_candidate.pre_entry_index >= 0 && best_candidate.pre_entry_index < cur_match_candidates.size()){
			    			best_candidate=cur_match_candidates.get(best_candidate.pre_entry_index);
			    			i--;
			    			if(i==98){
			    				//System.out.println();
			    			}
			    			if(i>=1){
				    			cur_match_candidates=match_matrix.get(i-1);
				    		}
				    		else{
				    			break;
				    		}
			    		}
			    		else{
			    			best_candidate=null;
			    			break;
			    		}
		    		}
		    	}
		    	
		    	if(i<1){ break;}
		    	
		    	Savepoint spt3 = con.setSavepoint("svpt3");
		    	/*sql="";
		    	if(i==308){
		    		System.out.println("here is 308");
		    	}*/
		    	//if(!first_sample ){
		    	
		    	if(best_candidate != null){
		    		cur_sample=trajectory.get(i);
		    		sql="UPDATE "+ sample_table +" SET gid="+best_candidate.road.gid+", edge_offset=" + best_candidate.offset;
		    		if( cur_sample.moving_distance>=0 && cur_sample.moving_distance <= temp_stop_threshold ){
		    			sql+= ", stop="+ Sample.TEM_STOP;
		    		}
		    		sql+=" WHERE suid="+suid+" and utc="+(int)(trajectory.get(i).utc.getTime()/1000);
		    		try{    			
			    		System.out.println(i+": "+sql);
			    		//System.out.println(i+": "+best_candidate.final_weight);
			    		stmt.executeUpdate(sql);
			    	}
			    	catch (SQLException e) {
			    		con.rollback(spt3);
			    		e.printStackTrace();
			    	}
			    	finally{
			    		con.commit();
			    	}
		    	}
		    	else{
		    		//sql="UPDATE "+ sample_table +" SET gid=null , edge_offset=null, route=null WHERE suid="+suid+" and utc="+(int)(trajectory.get(i).utc.getTime()/1000);	
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
		System.out.println("finished!");
	}
	
	public static void main(String[] args){
		
			try {
				/*
				long utc=((long)1231234903)*1000;

				System.out.println("TIME second:"+ utc);
				Date date=new Date(utc);
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				TimeZone zone=TimeZone.getTimeZone("GMT+8");
				format.setTimeZone(zone);
				System.out.println("TIME:"+format.format(date));
				
				
	    		String point= "POINT(116.352949508079 39.9391208659371)";
	    		int start=point.indexOf('(');
	    		int end=point.indexOf(')');
	    		int mid=point.indexOf(' ');
	    		System.out.println("|"+point.substring(start+1, mid)+"|");
	    		System.out.println("|"+point.substring(mid+1, end)+"|");	*/
				
				//ArrayList<Integer> roadsegments = new ArrayList<Integer>();
				
				if(args.length>0){
					DBconnector.clear("mydb", args[0]);
					ArrayList<String> taxi_list=new ArrayList<String>();
					DBconnector.get_suids("mydb", args[0], taxi_list);
					for(int i=0; i<taxi_list.size();i++){
						System.out.print((i+1)+"/"+taxi_list.size()+"	");
						DBconnector.mapMatching("mydb", taxi_list.get(i), args[0], "oneway_test", "intersection_test");
					}
				}
				else{
					//create table valid_gps_oneway as select * from valid_gps_test
					//update valid_gps_oneway SET gid=null , edge_offset=null, route=null
					String default_table="match_part_1";
					DBconnector.clear("mydb", default_table);
					ArrayList<String> taxi_list=new ArrayList<String>();
					DBconnector.get_suids("mydb", default_table, taxi_list);
					for(int i=0; i<taxi_list.size();i++){
						System.out.print((i+1)+"/"+taxi_list.size()+"	");
						DBconnector.mapMatching("mydb", taxi_list.get(i), default_table, "oneway_test", "intersection_test");
						//DBconnector.mapMatching("mydb", "14609", default_table, "oneway_test", "intersection_test");
					}
					
					/*String a="45638,45639,45632,45633,45634,31282,45625,9348,-1,";
					String[] ss=new String[50];
					ss=a.split(",");
					//System.out.println("");
					for(int j=0;j<ss.length;j++){
						//System.out.println("|"+ss[j]+"|");
						if(ss[j].matches("^[1-9][0-9]*$")){
							roadsegments.add(Integer.parseInt(ss[j]));
							System.out.println("|"+ss[j]+"|");
						}
					}*/
				}
				
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
}