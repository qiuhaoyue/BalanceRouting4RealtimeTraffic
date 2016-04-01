/** 
 * 2016年3月11日 
 * MapMatching.java 
 * author:ZhangYu
 */
import java.sql.*;
import java.util.ArrayList;
import java.lang.Math;
import java.util.Collections;

public class MapMatching {
	public static final double sigma_with_stop=6.636350348;
	public static final double sigma_without_stop=4.07;
	public static final double closest_beta=26.7127971075959;
	public static final double shortest_beta=2.7804772984;
	
	public static double getMatchWeight(double distance){
		
		double sigma=MapMatching.sigma_without_stop;
		
		return (-Math.log(Math.sqrt(2*Math.PI)*sigma)-0.5*(distance/sigma)*(distance/sigma));
	}
	
	
	public static double getTransitionWeight(double distance, double interval){

		double beta=(MapMatching.closest_beta+MapMatching.shortest_beta)*0.75;	
		double unit_time=120.0;
		
		beta=beta*interval/unit_time;
		
		return -(distance)/beta-Math.log(beta);
	}
	
	public static void mapMatching(String database, int suid, ArrayList<Sample> trajectory, String sample_table, String roadmap_table, String intersection_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt1;
		double distance_threshod=14.0; 		//paper sets this value as 200
		double temp_stop_threshold=13.0;
		int number_threshold=10; 			//paper sets this value as 5
		
		double unit_time=120.0;
		double time_threshold=330.0;
		double fade_factor=1.0;
		double travel_threshold=900;
		
		//Collections.sort(trajectory);
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			TRshortestPath routing_instance=new TRshortestPath();
			ArrayList<Path_element> path = new ArrayList<Path_element>();
			
			path.clear();
			//routing_instance.shortest_path(database, roadmap_table, intersection_table, 34650, 0.5, 12810, 0.5, path);
			//routing_instance.shortest_path(database, roadmap_table, intersection_table, 34650, 0.5, 12811, 0.5, path);
			
		    stmt = con.createStatement();
		    
		    //prepare the intersection_table
		    String sql=//"DROP TABLE match_temp; \n"+
	    			"CREATE TABLE match_temp(to_cost float8, target_id int4, via_path text); \n"+
	    			"INSERT INTO match_temp SELECT to_cost, to_way AS target_id, from_way AS via_path FROM "+intersection_table+" where to_way IS NOT NULL and from_way IS NOT NULL; \n";
	    	//System.out.println(sql);
	    	//stmt.executeUpdate(sql);
		    
		    //sql="select * from " + sample_table +" where suid = " +suid + " and (ostdesc not like '%定位无效%')  order by utc";
		    //System.out.println(sql);
		    //rs = stmt.executeQuery(sql);
		    
		    //ArrayList<Sample> trajectory= new ArrayList<Sample>();
		    //while(rs.next()){
		    //	trajectory.add(new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getLong("lat"), 
		    //					rs.getLong("lon"), (int)rs.getLong("head"), rs.getLong("speed"), rs.getLong("distance")));
		    //}
		    
		    ArrayList<ArrayList<MatchEntry>> match_matrix = new ArrayList<ArrayList<MatchEntry>>();
		    ArrayList<MatchEntry> pre_match_candidates=null, cur_match_candidates=null;
		    Sample cur_sample=null, pre_sample=null;
		    
		    for(int i=0; i<trajectory.size(); i++){
		    	pre_sample=cur_sample;
		    	cur_sample=trajectory.get(i);
		    	/*
		    	if(i==98){
		    		System.out.println(cur_sample.toString()); 
		    	}*/
			    spt1 = con.setSavepoint("svpt1");
			    pre_match_candidates = cur_match_candidates;
			    cur_match_candidates = new ArrayList<MatchEntry>();
			    
			    //Date time1=new Date();
			    
			    try{
			    	// find the segment whose distance to cur_sample is smaller than the threshold;
			    	sql="SELECT tmp.gid, tmp.x1, tmp.y1, tmp.x2, tmp.y2, tmp.dist, ST_Line_Locate_Point(tmp.the_geom, ST_GeometryFromText('POINT(" + cur_sample.lon + " "+cur_sample.lat + ")', 4326)) AS offset " +
			    			"FROM (SELECT gid, x1, y1, x2, y2, the_geom, " +
			    						"ST_Distance_Sphere(the_geom, ST_GeometryFromText('POINT(" + cur_sample.lon + " "+cur_sample.lat + ")', 4326)) AS dist "+  
			    				  " FROM "+ roadmap_table +" " +
			    				  "WHERE the_geom && ST_setsrid( \n" +
		            					"'BOX3D("+ (cur_sample.lon-0.001)+ " " + (cur_sample.lat-0.001) + "," + (cur_sample.lon+0.001)+ " " + (cur_sample.lat + 0.001)
		            					+ ")'::box3d, 4326) \n ) as tmp \n" +
		            		" WHERE tmp.dist <"+distance_threshod+" ORDER BY tmp.dist limit "+ number_threshold;
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
			    		/*String point= rs.getString("point");
			    		int start=point.indexOf('(');
			    		int end=point.indexOf(')');
			    		int mid=point.indexOf(' ');*/
			    		double p_x=0;//Double.parseDouble(point.substring(start+1, mid));
			    		double p_y=0;//Double.parseDouble(point.substring(mid+1, end));	
			    		
			    		double distance = rs.getDouble("dist");
			    		
			    		if(dynamic_threshold < 0 || distance < dynamic_threshold){
				    		MatchEntry newEntry=new MatchEntry(
				    				new RoadSegment(rs.getInt("gid"), rs.getDouble("x1"), rs.getDouble("y1"), rs.getDouble("x2"), rs.getDouble("y2")),
				    				rs.getDouble("offset"),
				    				p_x,
				    				p_y,
				    				distance);
				    		
				    		newEntry.matching_weight=MapMatching.getMatchWeight(cur_sample.distance);
				    		//newEntry.final_weight=DBconnector.getMatchWeight(cur_sample.distance);
			    		
				    		if(cur_sample.min_matching_distance<0 ){
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
			    	/*
			    	if(cur_sample.min_matching_distance >=0 ){
			    		System.out.println("SAMPLE["+i+"],"+cur_sample.toString());
			    	}*/
			    	
			    	MatchEntry cur_candidate=null, pre_candidate=null;
			    	if(i!=0 && pre_match_candidates != null && !pre_match_candidates.isEmpty()){
			    		//boolean route_success=true;
			    		
			    		//Date time5=new Date();
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
				    	//Date time6=new Date();
				    	//System.out.println("DISTANCE Time:"+(time6.getTime()-time5.getTime()));
				    	
				    	//store the moving distance to the previous sample, if the distance is too small then don't need to calculate the path;
				    	cur_sample.moving_distance=distance;
				    	trajectory.set(i, cur_sample);
			    		
				    	double timediff=Math.abs((cur_sample.utc.getTime()-pre_sample.utc.getTime())/1000);
				    	if( timediff >= time_threshold || (cur_sample.moving_distance >=0 && cur_sample.moving_distance < temp_stop_threshold) ){
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
				    			sql="SELECT * FROM pgr_trsp('select gid as id, source, target, to_cost as cost, reverse_cost from "+roadmap_table+"', "+
				    			pre_candidate.road.gid + ", " + pre_candidate.offset + ", " + cur_candidate.road.gid + ", " + cur_candidate.offset + ", " +
				    			"true, true, 'SELECT to_cost, target_id, via_path FROM match_temp') order by seq;";*/
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
				    				
				    				path.clear();
					    			routing_instance.shortest_path(database, roadmap_table, intersection_table, pre_candidate.road.gid, pre_candidate.offset, cur_candidate.road.gid, cur_candidate.offset, path);
					    			String new_path="";
					    			double new_cost=0.0;
					    			for(int x=0;x<path.size();x++){
					    				new_path+=""+path.get(x).edge_id+",";
					    				new_cost+=path.get(x).cost;
					    	    	}
					    			
					    			/*rs = stmt.executeQuery(sql);
					    			double cost=0.0;
					    			String str_path="";

							    	while(rs.next()){
							    		//"POINT(116.352949508079 39.9391208659371)"
							    		cost+=rs.getDouble("cost");
							    		str_path+=""+rs.getLong("id2")+",";
								    }
							    	
							    	System.out.println("new_cost="+new_cost);
					    			System.out.println("new_path="+new_path);
							    	System.out.println("str_cost="+cost);
					    			System.out.println("str_path="+str_path);
					    			if(!str_path.equals(new_path)){
					    				System.err.println("str_path != new_path");
					    				System.err.println("new_path="+new_path); 
					    				System.err.println("str_path="+str_path);
					    				str_path=new_path;
					    			}
					    			if(new_cost!=cost){
					    				System.err.println("cost != new_cost");
					    				System.err.println("new_cost=	"+new_cost); 
					    				System.err.println("cost=		"+cost);
					    				cost=new_cost;
					    			}*/
					    			
					    			new_cost = Math.abs(new_cost*1000-circle_distance);
							    	timediff=Math.abs((cur_sample.utc.getTime()-pre_sample.utc.getTime())/1000);
							    	
							    	if( timediff >= time_threshold || new_cost > travel_threshold*timediff/unit_time ){
							    		continue;
							    	}
							    	else{
								    	double weight = MapMatching.getTransitionWeight(new_cost, timediff) + (pre_candidate.final_weight)*fade_factor;
								    	if( weight<0 && (cur_candidate.transit_weight >0 || weight > cur_candidate.transit_weight)){
								    		cur_candidate.transit_weight = weight;
								    		cur_candidate.pre_entry_index = k;
								    		cur_candidate.path=new_path;
								    	}
							    	}
				    			}
				    			catch (Exception e) {
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
		    ArrayList<String> updates=new ArrayList<String>();
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
		    		//Savepoint spt3 = con.setSavepoint("svpt3");
		    		
		    		try{
			    		String newsql="UPDATE "+ sample_table +" SET gid="+ best_candidate.road.gid +", edge_offset=" + best_candidate.offset +", route='"+ best_candidate.path +"' ";
			    		if(cur_sample.moving_distance>=0 && cur_sample.moving_distance<=temp_stop_threshold){
				    		newsql+= ", stop="+Sample.TEM_STOP;
				    	}
			    		newsql+= " WHERE suid="+suid+" and utc="+(int)(trajectory.get(i).utc.getTime()/1000)+";\n";
			    		updates.add(newsql);
			    		if(updates.size()>500){
			    			sql="";
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
		    			//System.out.println(i+": "+sql);
		    		}
		    		catch (Exception e) {
		    			e.printStackTrace();
		    		}
		    		//find the next candidate
		    		finally{
		    			//con.commit();
		    			
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
		    	
		    	//Savepoint spt3 = con.setSavepoint("svpt3");
		    	/*sql="";
		    	if(i==308){
		    		System.out.println("here is 308");
		    	}*/
		    	//if(!first_sample ){
		    	
		    	if(best_candidate != null){
		    		cur_sample=trajectory.get(i);
		    		String newsql="UPDATE "+ sample_table +" SET gid="+best_candidate.road.gid+", edge_offset=" + best_candidate.offset;
		    		if( cur_sample.moving_distance>=0 && cur_sample.moving_distance <= temp_stop_threshold ){
		    			newsql+= ", stop="+ Sample.TEM_STOP;
		    		}
		    		newsql+=" WHERE suid="+suid+" and utc="+(int)(trajectory.get(i).utc.getTime()/1000)+";\n";
		    		
		    		updates.add(newsql);
		    		if(updates.size()>500){
		    			sql="";
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
		    	else{
		    		//sql="UPDATE "+ sample_table +" SET gid=null , edge_offset=null, route=null WHERE suid="+suid+" and utc="+(int)(trajectory.get(i).utc.getTime()/1000);	
		    	}
		    } //end for
		    if(updates.size()>0){
    			sql="";
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
		
		catch (SQLException e) {
		    e.printStackTrace();
			/*
			if (e instanceof BatchUpdateException)
            {
                BatchUpdateException bex = (BatchUpdateException) e;
                bex.getNextException().printStackTrace(System.out);
            }
			*/

		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    Common.dropConnection(con);
		}
		System.out.println("map matching finished!");
	}
}
