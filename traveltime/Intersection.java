import java.sql.*;
import java.util.ArrayList;
 
public class Intersection {
	
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
	
	public static void intersection_update(String database, String roadmap_table, String nodes_table, String intersection_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		double inconnectivity=RoadCostUpdater.inconnectivity;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		ArrayList<Turning> intersection_list=null;
		Long max_intersect_id=0L;
		
		try {
			
			try{
				stmt = con.createStatement();
				
				//no_left_turn && no_right_turn && no_straight_on
				//from_way
				String sql=" UPDATE "+intersection_table+"\n SET from_way="+ roadmap_table+".gid \n FROM "+roadmap_table + "," + nodes_table 
				    			+ "\n WHERE ("+ intersection_table + ".detail = 'no_left_turn' or "+ intersection_table + ".detail = 'no_right_turn' or "+ intersection_table + ".detail = 'no_straight_on') and "
				    			+ intersection_table + ".from_edge is not null and "+ intersection_table + ".from_edge="+ roadmap_table
				    			+".osm_id and "+nodes_table+".id="+ intersection_table + ".node_id and \n (("+ nodes_table+".lon="+ roadmap_table + ".x1 and "+
				    			nodes_table+".lat="+ roadmap_table + ".y1 and "+ roadmap_table +".reverse_cost>0 and "+ roadmap_table + ".reverse_cost <"+inconnectivity
				    			+") \n or ("+ nodes_table+".lon="+ roadmap_table + ".x2 and "+ nodes_table+".lat="+ roadmap_table + ".y2 and "
				    			+ roadmap_table +".to_cost>0 and "+ roadmap_table + ".to_cost <"+ inconnectivity +"));";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				
				//to_way
				sql=" UPDATE "+intersection_table+"\n SET to_way="+ roadmap_table+".gid \n FROM "+roadmap_table + "," + nodes_table 
								+ "\n WHERE ("+ intersection_table + ".detail = 'no_left_turn' or "+ intersection_table + ".detail = 'no_right_turn' or "+ intersection_table + ".detail = 'no_straight_on') and "
				    			+ intersection_table + ".to_edge is not null and "+ intersection_table + ".to_edge="+ roadmap_table
				    			+".osm_id and "+nodes_table+".id="+ intersection_table + ".node_id and \n (("+ nodes_table+".lon="+ roadmap_table + ".x1 and "+
				    			nodes_table+".lat="+ roadmap_table + ".y1 and "+ roadmap_table +".to_cost>0 and "+ roadmap_table + ".to_cost <"+inconnectivity
				    			+") \n or ("+ nodes_table+".lon="+ roadmap_table + ".x2 and "+ nodes_table+".lat="+ roadmap_table + ".y2 and "
				    			+ roadmap_table +".reverse_cost>0 and "+ roadmap_table + ".reverse_cost <"+ inconnectivity +"));";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				
				//no_u_turn
				//from_way
				sql=" UPDATE "+intersection_table+"\n SET from_way="+ roadmap_table+".gid \n FROM "+roadmap_table + "," + nodes_table 
		    			+ "\n WHERE ("+ intersection_table + ".detail = 'no_u_turn' ) and "
		    			+ intersection_table + ".from_edge is not null and "+ intersection_table + ".from_edge="+ roadmap_table
		    			+".osm_id and "+nodes_table+".id="+ intersection_table + ".node_id and \n (("+ nodes_table+".lon="+ roadmap_table + ".x1 and "+
		    			nodes_table+".lat="+ roadmap_table + ".y1 and "+ roadmap_table +".reverse_cost>0 and "+ roadmap_table + ".reverse_cost <"+inconnectivity
		    			+") \n or ("+ nodes_table+".lon="+ roadmap_table + ".x2 and "+ nodes_table+".lat="+ roadmap_table + ".y2 and "
		    			+ roadmap_table +".to_cost>0 and "+ roadmap_table + ".to_cost <"+ inconnectivity +"));";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				
				//to_way
				sql=" UPDATE "+intersection_table+"\n SET to_way="+ roadmap_table+".gid \n FROM "+roadmap_table + "," + nodes_table 
						+ "\n WHERE ("+ intersection_table + ".detail = 'no_u_turn' ) and "
		    			+ intersection_table + ".to_edge is not null and "+ intersection_table + ".to_edge="+ roadmap_table
		    			+".osm_id and "+nodes_table+".id="+ intersection_table + ".node_id and \n (("+ nodes_table+".lon="+ roadmap_table + ".x1 and "+
		    			nodes_table+".lat="+ roadmap_table + ".y1 and "+ roadmap_table +".to_cost>0 and "+ roadmap_table + ".to_cost <"+inconnectivity
		    			+") \n or ("+ nodes_table+".lon="+ roadmap_table + ".x2 and "+ nodes_table+".lat="+ roadmap_table + ".y2 and "
		    			+ roadmap_table +".reverse_cost>0 and "+ roadmap_table + ".reverse_cost <"+ inconnectivity +"));";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				
				//only_right_turn && only_straight_on
				//from_way
				sql=" UPDATE "+intersection_table+"\n SET to_cost=0.0, from_way="+ roadmap_table+".gid \n FROM "+roadmap_table + "," + nodes_table 
		    			+ "\n WHERE ("+ intersection_table + ".detail like 'only_%' ) and "
		    			+ intersection_table + ".from_edge is not null and "+ intersection_table + ".from_edge="+ roadmap_table
		    			+".osm_id and "+nodes_table+".id="+ intersection_table + ".node_id and \n (("+ nodes_table+".lon="+ roadmap_table + ".x1 and "+
		    			nodes_table+".lat="+ roadmap_table + ".y1 and "+ roadmap_table +".reverse_cost>0 and "+ roadmap_table + ".reverse_cost <"+inconnectivity
		    			+") \n or ("+ nodes_table+".lon="+ roadmap_table + ".x2 and "+ nodes_table+".lat="+ roadmap_table + ".y2 and "
		    			+ roadmap_table +".to_cost>0 and "+ roadmap_table + ".to_cost <"+ inconnectivity +"));";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				
				//to_way
				sql= " UPDATE "+intersection_table+"\n SET to_cost=0.0, to_way="+ roadmap_table+".gid \n FROM "+roadmap_table + "," + nodes_table 
						+ "\n WHERE ("+ intersection_table + ".detail like 'only_%' ) and "
		    			+ intersection_table + ".to_edge is not null and "+ intersection_table + ".to_edge="+ roadmap_table
		    			+".osm_id and "+nodes_table+".id="+ intersection_table + ".node_id and \n (("+ nodes_table+".lon="+ roadmap_table + ".x1 and "+
		    			nodes_table+".lat="+ roadmap_table + ".y1 and "+ roadmap_table +".to_cost>0 and "+ roadmap_table + ".to_cost <"+inconnectivity
		    			+") \n or ("+ nodes_table+".lon="+ roadmap_table + ".x2 and "+ nodes_table+".lat="+ roadmap_table + ".y2 and "
		    			+ roadmap_table +".reverse_cost>0 and "+ roadmap_table + ".reverse_cost <"+ inconnectivity +"));";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				
				sql="select * from "+ intersection_table + " order by intersection_id desc";
				System.out.println(sql);
				rs = stmt.executeQuery(sql);
				while(rs.next()){
					max_intersect_id=rs.getLong("intersection_id");
					if(max_intersect_id>0){
						break;
					}
			    }
				
				sql="select * from "+ intersection_table + " where detail like 'only%' order by intersection_id";
				System.out.println(sql);
				rs = stmt.executeQuery(sql);
		    
				//select * from intersection_test where detail like 'only%'
				intersection_list= new ArrayList<Turning>();
			    while(rs.next()){
			    	Turning cur_turn=new Turning(rs.getLong("intersection_id"), rs.getInt("from_edge"), rs.getInt("to_edge"), rs.getLong("node_id"), 
			    			rs.getInt("from_way"), rs.getInt("to_way"), rs.getString("hour_on"), rs.getString("hour_off"), rs.getString("examption"));
			    	intersection_list.add(cur_turn);
			    }
			}
			catch (Exception e) {
			    e.printStackTrace();
		    }
		    finally{
		    	con.commit();
		    }
			
		    //intersection_list.clear();
		    ArrayList<RoadSegment> roadmap= new ArrayList<RoadSegment>();
		    for(int i=0;i<intersection_list.size();i++){
		    	Turning cur_turn=intersection_list.get(i);
		    	try{
			    	String sql="select * from " + roadmap_table + " where gid="+ cur_turn.to_way +" or gid="+ cur_turn.from_way;
			    	System.out.println(sql);
			    	rs = stmt.executeQuery(sql);
			    	
			    	roadmap.clear();
			    	while(rs.next()){
				    	RoadSegment road=new RoadSegment(rs.getInt("gid"), rs.getLong("osm_id"), rs.getInt("source"), rs.getInt("target"));
				    	road.reverse_cost=rs.getDouble("reverse_cost");
				    	road.to_cost=rs.getDouble("to_cost");
				    	roadmap.add(road);
				    }
		    	}
		    	catch (Exception e) {
				    e.printStackTrace();
				    continue;
		    	}
		    	
		    	if(roadmap.size()!=2){
		    		System.out.println("found not exactly 2 road as from_way and to_way !!!! intersection_id="+cur_turn.intersection_id );
		    		continue;
		    	}
		    	//else
		    	//compare source[0],target[0] and source[1],target[1], to get the common point;
		    	RoadSegment from_road=null, to_road=null;
		    	long common_node=-1;
		    	if(roadmap.get(0).osm_id==cur_turn.from_edge){
		    		from_road=roadmap.get(0);
		    		to_road=roadmap.get(1);
		    	}
		    	else{
		    		from_road=roadmap.get(1);
		    		to_road=roadmap.get(0);
		    	}
		    	
		    	if(from_road.source==to_road.source || from_road.source==to_road.target){
		    		if(from_road.reverse_cost>0 && from_road.reverse_cost<RoadCostUpdater.inconnectivity){
		    			common_node=from_road.source;
		    		}
		    		else{
		    			System.out.println("Never use the intersection !!!! intersection_id="+cur_turn.intersection_id );
		    		}
		    	}
		    	else{
		    		if(from_road.to_cost>0 && from_road.to_cost<RoadCostUpdater.inconnectivity){
		    			common_node=from_road.target;
		    		}
		    		else{
		    			System.out.println("Never use the intersection !!!! intersection_id="+cur_turn.intersection_id );
		    		}
		    	}
		    	
		    	//select * from oneway_test where (target = common_point and reverse_cost reverse_cost>0 and "+ roadmap_table + ".reverse_cost <"+ inconnectivity)
				  //or source = common_point and to_cost>0 and "+ roadmap_table + ".to_cost <"+ inconnectivity;
		    	if(common_node>=0){
		    		try{
		    			String sql="select * from "+roadmap_table+" where (target = "+common_node+" and reverse_cost>0 and reverse_cost <"+ RoadCostUpdater.inconnectivity+ 
			    				") or (source = "+common_node+" and to_cost>0 and to_cost <"+ + RoadCostUpdater.inconnectivity+ ")";
		    			System.out.println(sql);
				    	rs = stmt.executeQuery(sql);
				    	roadmap.clear();
				    	while(rs.next()){
					    	RoadSegment road=new RoadSegment(rs.getInt("gid"), rs.getLong("osm_id"), rs.getInt("source"), rs.getInt("target"));
					    	road.reverse_cost=rs.getDouble("reverse_cost");
					    	road.to_cost=rs.getDouble("to_cost");
					    	if(road.gid != to_road.gid){
					    		roadmap.add(road);
					    	}
					    }
				    }
		    		catch (Exception e) {
					    e.printStackTrace();
					    continue;
				    }
			    	
			    	//add each of the selected gids as to_way and detail as 'derived from intersection_id'
			    	for(int j=0; j<roadmap.size();j++){
			    		to_road=roadmap.get(j);
			    		max_intersect_id++;
			    		try{
			    			String sql="insert into " +intersection_table + "(intersection_id, to_cost, from_edge, to_edge";	
			    			String values=" values (" + max_intersect_id + ", 'Infinity', " + from_road.osm_id + "," + to_road.osm_id;
			    			if(cur_turn.hour_on != null){
			    				sql+=", hour_on";
			    				values+=",'" + cur_turn.hour_on+"'";
			    			}
			    			if(cur_turn.hour_off != null){
			    				sql+=", hour_off";
			    				values+=",'" + cur_turn.hour_off+"'";
			    			}
			    			if(cur_turn.examption != null){
			    				sql+=", examption";
			    				values+=",'" + cur_turn.examption+"'";
			    			}
			    			
			    			sql+=", detail, to_way, from_way)" + values + ", '"+ cur_turn.intersection_id + "',"+ from_road.gid + ","+ to_road.gid+ ");";
				    		
				    		System.out.println(sql);
							stmt.executeUpdate(sql);
			    		}
			    		catch (Exception e) {
						    e.printStackTrace();
						    continue;
					    }
					    finally{
					    	con.commit();
					    }
			    	}
		    	}
		    }
		    //String sql= "delete from intersection_test where detail not like 'no%' and detail not like 'only%';";
			//System.out.println(sql);
			//stmt.executeUpdate(sql);
		}
		//catch (SQLException e) {
		   // e.printStackTrace();
		//}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    DBconnector.dropConnection(con);
		}
		System.out.println("finished!");
	}
	
	public static void main(String[] args){
		/*
		 	drop table oneway_test;
			create table oneway_test as select * from ways_test;
			update intersection_test set hour_on='15:00:00', hour_off='18:00:00' where intersection_id=234;
		 */
		try {
			//
			Intersection.intersection_update("mydb", "oneway_test", "nodes", "intersection_test");
				
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}