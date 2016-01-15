import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;


public class TrafficDelayRelation {

	public static void aggregate_time(String database, String roadmap_table, String allocation_table, String aggregation_table){
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
	    				"select gid, next_gid, sum(percent) as count, sum(time*interval) as weight_time, sum(interval*percent) as weight \n"+
	    				"from "+ allocation_table +" group by gid, next_gid) as temp \n" +
	    				"where temp.weight!=0 and count>0 order by gid, next_gid) as temp2, "+roadmap_table+" as roadmap where temp2.gid=roadmap.gid \n"+
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
	    				"select gid, sum(percent) as count, sum(time*interval) as weight_time, sum(interval*percent) as weight \n"+
	    				"from "+ allocation_table +" group by gid) as temp \n" +
	    				"where temp.weight!=0 and count>2 order by gid) as temp2, "+roadmap_table+" as roadmap where temp2.gid=roadmap.gid \n"+
	    				");";
	    		
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
		System.out.println("aggregate_time finished!");
	}
	
	public static void prepare_laneCount(String database, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null,spt1=null;

		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			stmt = con.createStatement();
			
			/*
			 * 
			 * 
		alter table oneway_matching add column manual_lane integer;
		update oneway_matching set manual_lane=1 where lane_count is not null and lane_count != 1.01;
		update oneway_matching set manual_lane=2 where lane_count is not null and lane_count = 1.01;
		update oneway_matching set manual_lane=3 where lane_count is null;

		select count(*), class_id from oneway_matching where lane_count is null group by class_id order by class_id;
		select sum(lane_count*length)/sum(length), class_id from oneway_matching where lane_count is not null group by class_id order by class_id;
		
		update oneway_matching
		set lane_count=b.default_lane 
		from (select sum(lane_count*length)/sum(length) as default_lane, class_id from oneway_matching where (manual_lane=2 or manual_lane=1) group by class_id )as b 
		where oneway_matching.class_id=b.class_id and oneway_matching.lane_count is null;

			 *
			 */
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+roadmap_table+" add column manual_lane integer;\n ";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    		
	    		sql="update "+roadmap_table+" set manual_lane=1 where lane_count is not null and lane_count != 1.01 ;\n" +
	    			"update "+roadmap_table+" set manual_lane=2 where lane_count is not null and lane_count = 1.01; \n" +
	    			"update "+roadmap_table+" set manual_lane=3 where lane_count is null;";
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
	    		spt1 = con.setSavepoint("svpt2");
	    		String sql="update "+roadmap_table+" set lane_count=b.default_lane from (select sum(lane_count*length)/sum(length) as default_lane, class_id from "+roadmap_table+
	    				" where (manual_lane=2 or manual_lane=1) group by class_id )as b where "+roadmap_table+".class_id=b.class_id and "+roadmap_table+".lane_count is null;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt1);
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
		System.out.println("aggregate_time finished!");
	}
	
	public static double[][] test(String database, String roadmap_table, HashMap<Integer, Integer> baseid2idx, 
			HashMap<Integer, Integer> objid2idx, ArrayList<RoadSegment> base_roads, ArrayList<RoadSegment> obj_roads){
		
		double[][] weight_matrix=null; 

		//calculate the static weight for each obj using the base roadsegment;
		int rows_count=2;
		int col_count=2;
		weight_matrix=new double[rows_count][col_count];
		for(int i=0;i<rows_count;i++){
			for(int j=0; j<col_count;j++){
				weight_matrix[i][j]=12;
			}
		}
		
		return weight_matrix;
		
	}
	
	public static void print(double[][] weight_matrix){
		
		//calculate the static weight for each obj using the base roadsegment;
		int rows_count=2;
		int col_count=2;
		
		for(int i=0;i<rows_count;i++){
			for(int j=0; j<col_count;j++){
				System.out.println(weight_matrix[i][j]);
			}
		}
		
		
	}
	
	public static double[][] prepare_static_weight(String database, String roadmap_table, HashMap<Integer, Integer> baseid2idx, 
			HashMap<Integer, Integer> objid2idx, ArrayList<RoadSegment> base_roads, ArrayList<RoadSegment> obj_roads){
		
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;
		ResultSet rs=null;
		double[][] weight_matrix=null; 
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return weight_matrix;
		}
		
		try{
			
			stmt = con.createStatement();
			
			if(baseid2idx==null){
				baseid2idx=new HashMap<Integer, Integer>();
			}
			baseid2idx.clear();
			//read the base taxi_ratio from the roadmap_table;
			if(base_roads==null){
				base_roads=new ArrayList<RoadSegment>();
			}
			base_roads.clear();
			
			try{
				spt = con.setSavepoint("svpt1");
				String sql="select gid, x1, y1, x2, y2, st_asText(the_geom) as geom, lane_count, taxi_ratio, " +
						"Degrees(ST_azimuth(st_point(x1,y1),st_point(x2,y2))) as degree, to_cost, reverse_cost from "+ roadmap_table +" where taxi_ratio is not null order by gid;";
				System.out.println(sql);
				rs=stmt.executeQuery(sql);
				while(rs.next()){
					RoadSegment road=new RoadSegment(rs.getInt("gid"), rs.getDouble("x1"), rs.getDouble("y1"), rs.getDouble("x2"), rs.getDouble("y2"),
							rs.getString("geom"),rs.getDouble("lane_count"));
					
					road.taxi_ratio=rs.getDouble("taxi_ratio");
					double to_cost=rs.getDouble("to_cost");
					//double reverse_cost=rs.getDouble("reverse_cost");
					if(to_cost>=RoadCostUpdater.inconnectivity/10){
						road.direction=((int)(rs.getDouble("degree")+180))%360;
					}
					else{
						road.direction=(int)(rs.getDouble("degree"));
					}
					
					baseid2idx.put(road.gid, base_roads.size());
					base_roads.add(road);
					
				}
			}
			catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
			//read the obj road segment from the time_table;
			if(objid2idx==null){
				objid2idx=new HashMap<Integer, Integer>();
			}
			objid2idx.clear();
			
			if(obj_roads==null){
				obj_roads=new ArrayList<RoadSegment>();
			}
			obj_roads.clear();
			try{
				spt = con.setSavepoint("svpt1");
				String sql="select gid, x1, y1, x2, y2, st_astext(the_geom) as geom, lane_count, Degrees(ST_azimuth(st_point(x1,y1),st_point(x2,y2))) as degree, " +
						"to_cost, reverse_cost from "+ roadmap_table +" where taxi_ratio is null order by gid;";
				System.out.println(sql);
				rs=stmt.executeQuery(sql);
				while(rs.next()){
					RoadSegment road=new RoadSegment(rs.getInt("gid"), rs.getDouble("x1"), rs.getDouble("y1"), rs.getDouble("x2"), rs.getDouble("y2"),
							rs.getString("geom"), rs.getDouble("lane_count"));
					//road.lane_count=rs.getDouble("lane_count");
					
					double to_cost=rs.getDouble("to_cost");
					//double reverse_cost=rs.getDouble("reverse_cost");
					if(to_cost>=RoadCostUpdater.inconnectivity/10){
						road.direction=((int)(rs.getDouble("degree")+180))%360;
					}
					else{
						road.direction=(int)(rs.getDouble("degree"));
					}
					objid2idx.put(road.gid, obj_roads.size());
					obj_roads.add(road);
					//obj_roads.get(obj_roads.size()-1).lane_count=-10;
				}
			}
			catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
			//calculatet the angle 
			
			//calculate the static weight for each obj using the base roadsegment;
			int rows_count=obj_roads.size();
			int col_count=base_roads.size();
			weight_matrix=new double[rows_count][col_count];
			for(int i=0;i<rows_count;i++){
				for(int j=0; j<col_count;j++){
					weight_matrix[i][j]=0;
				}
			}
			
			int last_gid=0;
			int step_size=250;
			for(int i=step_size; i<rows_count; i=(i+step_size>=rows_count)? rows_count-1 : i+step_size){
				int cur_gid=obj_roads.get(i).gid;
				
				System.out.println("----------prepare_static_weight ["+(i/step_size)+"/"+(rows_count/step_size)+"]------------");
				try{
					spt = con.setSavepoint("svpt1");
					String sql="select a.gid as obj_gid, b.gid as base_gid, ST_Distance(st_transform(a.the_geom,2163),st_transform(b.the_geom,2163)) as distance from "+
								"(select gid, the_geom from "+roadmap_table+" where taxi_ratio is null and gid>="+last_gid+" and gid<"+cur_gid+") as a, " +
								"(select gid, the_geom from "+roadmap_table+" where taxi_ratio is not null) as b;";
					//System.out.println(sql);
					rs=stmt.executeQuery(sql);
					while(rs.next()){
						double distance=0.0;
						double angle=0.0;
						double lane_diff=0.0;
						double weight=1;
						
						int obj_gid=rs.getInt("obj_gid");
						int base_gid=rs.getInt("base_gid");
						int row=objid2idx.get(obj_gid);
						int col=baseid2idx.get(base_gid);
						
						RoadSegment obj_road=obj_roads.get(row);
						RoadSegment base_road=base_roads.get(col);
						
						distance=rs.getDouble("distance");
						if(distance>1){
							//weight-=Math.log(distance);
							weight/=distance;
						}
								
						angle=((int)(obj_road.direction-base_road.direction+360))%180;
						if(angle>1){
							//weight-=Math.log(angle);
							weight/=angle;
						}
								
						lane_diff=Math.abs(obj_road.lane_count-base_road.lane_count);
						if(lane_diff>1){
							//weight-=Math.log(lane_diff);
							weight/=lane_diff;
						}
						
						//idx2weight.put(row*col_count+col, weight);
						weight_matrix[row][col]=weight;
					}
				}
				catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
				finally{
					last_gid=cur_gid;
				}
				
				if(i>=rows_count-1){
					break;
				}
			}
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}	
		
		return weight_matrix;
		
	}
	
	public static void prepare_trafficCount(String database, String time_table, String allocation_table, String roadmap_table, double[][] weight_matrix, HashMap<Integer, Integer> baseid2idx, 
			HashMap<Integer, Integer> objid2idx, ArrayList<RoadSegment> base_roads, ArrayList<RoadSegment> obj_roads){
		
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;
		ResultSet rs=null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try{
			
			stmt = con.createStatement();
			
			//calculate the taxi traffic count and average speed 
			TrafficDelayRelation.aggregate_time(database, roadmap_table, allocation_table, time_table);
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+time_table+" add column taxi_ratio double precision;";	    		
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
			
			//calculate the taxi_ratio for each obj using the base roadsegment;
			int rows_count=obj_roads.size();
			int col_count=base_roads.size();
			for(int i=0;i<rows_count;i++){
				obj_roads.get(i).traffic_count=0;
				obj_roads.get(i).taxi_ratio=0;
			}
			for(int j=0;j<col_count;j++){
				base_roads.get(j).traffic_count=0;
			}
			
			try{
				spt = con.setSavepoint("svpt1");
				String sql="select gid, reference from "+ time_table + " where next_gid is null and reference>0 order by gid";
				System.out.println(sql);
				rs=stmt.executeQuery(sql);
				while(rs.next()){
					int cur_gid=rs.getInt("gid");
					double taxi_count=rs.getDouble("reference");
					//RoadSegment test_road=null;
					if(objid2idx.containsKey(cur_gid)){
						//test_road=obj_roads.get(objid2idx.get(cur_gid));
						obj_roads.get(objid2idx.get(cur_gid)).taxi_count=taxi_count;
						//test_road=obj_roads.get(objid2idx.get(cur_gid));
					}
					else if(baseid2idx.containsKey(cur_gid)){
						//test_road=base_roads.get(baseid2idx.get(cur_gid));
						base_roads.get(baseid2idx.get(cur_gid)).taxi_count=taxi_count;
						//test_road=base_roads.get(baseid2idx.get(cur_gid));
					}
					else{
						System.err.println("ERROR: ---------ROAD NOT FOUND IN BOTH OBJ AND BASE ROAD LISTS!-----------");
					}
				}
			}
			catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback(spt);
			}
			
			ArrayList<String> updates=new ArrayList<String>();
			for(int i=0;i<rows_count;i++){
				double taxi_ratio=0;
				double sum_weight=0;
				double sum_weighted_taxi=0;
				
				for(int j=0; j<col_count; j++){
					//double weight=idx2weight.get(i*col_count+j);
					double weight=weight_matrix[i][j];
					
					double traffic_diff=Math.abs(obj_roads.get(i).taxi_count-base_roads.get(j).taxi_count);
					if(traffic_diff>1){
						weight/=traffic_diff;
					}
					sum_weight+=weight;
					sum_weighted_taxi+=base_roads.get(j).taxi_ratio*weight;
				}
				taxi_ratio=sum_weighted_taxi/sum_weight;
				
				try{
					spt = con.setSavepoint("svpt1");
					
					String newsql="update "+ time_table + " set taxi_ratio="+taxi_ratio+" where next_gid is null and gid="+obj_roads.get(i).gid+"; \n";
					updates.add(newsql);
			    	
			    	if(updates.size()>200){
			    		String sql="";
						try{
				    		for(int vi=0; vi<updates.size(); vi++){
				    			sql+=updates.get(vi);
		    		    	}
				    		//System.out.println("["+i+"/"+trips.size()+"]");
				    		//System.out.println(sql);
		    		    	stmt.executeUpdate(sql);
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
			}
			
			for(int j=0; j<col_count; j++){
				try{
					//spt = con.setSavepoint("svpt1");
					String newsql="update "+ time_table + " set taxi_ratio="+base_roads.get(j).taxi_ratio+" where next_gid is null and gid="+base_roads.get(j).gid+"; \n";
					
					updates.add(newsql);
			    	if(updates.size()>200){
			    		String sql="";
						try{
				    		for(int vi=0; vi<updates.size(); vi++){
				    			sql+=updates.get(vi);
		    		    	}
				    		//System.out.println("["+i+"/"+trips.size()+"]");
				    		//System.out.println(sql);
		    		    	stmt.executeUpdate(sql);
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
			}
			
			if(updates.size()>0){
	    		String sql="";
				try{
		    		for(int vi=0; vi<updates.size(); vi++){
		    			sql+=updates.get(vi);
    		    	}
		    		//System.out.println("["+i+"/"+trips.size()+"]");
		    		System.out.println(sql);
    		    	stmt.executeUpdate(sql);
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
	
	public static void prepare_learningdata(String database, ArrayList<String> time_tables, String speed_roadmap, String merged_timetable){
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
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="drop table "+ merged_timetable +";";
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
			
			if(time_tables!=null && time_tables.size()>0){
				String part_table=time_tables.get(0);
				try{
		    		spt = con.setSavepoint("svpt2");
		    		String sql="create table "+ merged_timetable +" as select a.gid, a.reference as taxi_count, a.taxi_ratio, a.time, a.average_speed, a.length," +
		    				" a.class_id, b.capacity, b.max_speed from " + part_table +" as a, " +
		    				speed_roadmap + " as b where a.next_gid is null and a.reference>0 and a.gid=b.gid order by a.gid;";
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
		    		String sql="alter table "+ merged_timetable +" add column seq integer;";
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
		    		String sql="update "+ merged_timetable +" set seq=0 where seq is null;";
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
				
				for(int i=1;i<time_tables.size();i++){
					part_table=time_tables.get(i);
					try{
			    		spt = con.setSavepoint("svpt3");
			    		String sql="insert into "+ merged_timetable +"(gid, taxi_count, taxi_ratio, time, average_speed, length, class_id, capacity, max_speed) " +
			    				"select a.gid, a.reference as taxi_count, a.taxi_ratio, a.time, a.average_speed, a.length," +
			    				" a.class_id, b.capacity, b.max_speed from " + part_table +" as a, " +
			    				speed_roadmap + " as b where a.next_gid is null and a.reference>0 and a.gid=b.gid order by a.gid;";
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
			    		String sql="update "+ merged_timetable +" set seq="+(i)+" where seq is null;";
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
			}
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="CREATE INDEX "+merged_timetable+"_gid_idx ON "+ merged_timetable +"(gid);";
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

	public static void prepare_capacity(String database, String speed_table, String roadmap_table){
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
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ speed_table +" add column lane_count double precision;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    		
	    		sql="update "+ speed_table +" set lane_count = "+roadmap_table+".lane_count from "+roadmap_table+
	    				" where "+roadmap_table+".gid="+speed_table+".gid;";
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
	    		String sql="alter table "+ speed_table +" add column capacity double precision;";
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
			
			ArrayList<Double> speed_list=new ArrayList<Double>();
			ArrayList<Double> capacity_list=new ArrayList<Double>();
			speed_list.add(100.0);
			capacity_list.add(2200.0);
			speed_list.add(80.0);
			capacity_list.add(2100.0);
			speed_list.add(60.0);
			capacity_list.add(1800.0);
			speed_list.add(50.0);
			capacity_list.add(1700.0);
			speed_list.add(40.0);
			capacity_list.add(1650.0);
			speed_list.add(30.0);
			capacity_list.add(1600.0);
			speed_list.add(20.0);
			capacity_list.add(1400.0);
			
			int list_len=speed_list.size();
			for(int i=0; i<list_len; i++){
				if(i==0){
					try{
			    		spt = con.setSavepoint("svpt2");
			    		String sql="update "+ speed_table +" set capacity="+ capacity_list.get(i)+"*lane_count " +
			    				"where max_speed>="+(speed_list.get(i)/3.6)+";";
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
					try{
				    	spt = con.setSavepoint("svpt2");
				    	String sql="update "+ speed_table +" set capacity="+capacity_list.get(i)+"*lane_count " +
				    			"where max_speed < "+(speed_list.get(i-1)/3.6)+" and max_speed>="+(speed_list.get(i)/3.6)+";";
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
					
					if(i==list_len-1){
						try{
				    		spt = con.setSavepoint("svpt2");
				    		String sql="update "+ speed_table +" set capacity="+capacity_list.get(i)+"*lane_count " +
				    				"where max_speed <"+(speed_list.get(i)/3.6)+";";
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
	
	public static void main(String[] args){
		
		if(args.length>0){
			
			String time_table_prefix=args[0];//"ring5_time_static_";// gps_20100413ring5_time_
			String alloc_table_prefix=args[1];//"ring5_allocation_static_"; gps_20100406_allocation_
			
			int least_seq=54;
			int step=1;
			int max_seq=96;
			
			ArrayList<String> time_tables=new ArrayList<String>();	
			ArrayList<String> alloc_tables=new ArrayList<String>();	
			
			for(int i=least_seq;i<=max_seq; i+=step){
				time_tables.add(time_table_prefix+i);
				alloc_tables.add(alloc_table_prefix+i);
			}
			
			String speed_table=args[2];//"ring5_roads_initspeed";//init_speed_labeled_gps_20100406
			//String merged_timetable=args[3];//"ring5_time";//ring5_time_gps_20100406;
			
			String database="mydb";
			String roadmap_table="oneway_matching";
	
			//TrafficDelayRelation.prepare_laneCount(database, roadmap_table);
			HashMap<Integer, Integer> baseid2idx=new HashMap<Integer, Integer>();
			HashMap<Integer, Integer> objid2idx=new HashMap<Integer, Integer>();
			ArrayList<RoadSegment> base_roads=new ArrayList<RoadSegment>();
			ArrayList<RoadSegment> obj_roads=new ArrayList<RoadSegment>();
			double[][] weight_matrix = prepare_static_weight(database, roadmap_table, baseid2idx, objid2idx, base_roads, obj_roads);
			
			for(int i=0; i<time_tables.size(); i++){
				String time_table=time_tables.get(i);//"ring2_time_test";
				String allocation_table=alloc_tables.get(i);//"ring2_allocation_1";
				prepare_trafficCount(database, time_table, allocation_table, roadmap_table, weight_matrix, baseid2idx, objid2idx, base_roads, obj_roads);
			}
			
			//TrafficDelayRelation.prepare_capacity(database, speed_table, roadmap_table);
			//TrafficDelayRelation.prepare_learningdata(database, time_tables, speed_table, merged_timetable);
		}
	}

	
}