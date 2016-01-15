import java.sql.*;
import java.util.ArrayList;

 
public class SpliteDualWay {
	
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
	
	public static void splitways(String database, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		Savepoint spt=null;
		double inconnectivity=RoadCostUpdater.inconnectivity;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "p@ssw0rd");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="alter table "+ roadmap_table +" add column old_gid integer;";
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
			//插入旧的gid以便新插入的单行线识别相反路线
			try{
	    		spt = con.setSavepoint("svpt4");
	    		String sql="update "+ roadmap_table +" set old_gid=gid;";
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

		    System.out.println("select * from " + roadmap_table + " order by gid");
		    rs = stmt.executeQuery("select * from " + roadmap_table + " order by gid");
			
			//提取所有的道路并得到gid的最大值
		    ArrayList<RoadSegment> roadmap= new ArrayList<RoadSegment>();
		    long max_gid=0;
		    while(rs.next()){
		    	RoadSegment cur_road=new RoadSegment(rs.getInt("gid"), rs.getDouble("to_cost"), rs.getDouble("reverse_cost"), rs.getDouble("length"), rs.getInt("class_id"));
		    	cur_road.geom=rs.getString("the_geom");
		    	cur_road.max_speed=rs.getInt("maxspeed_forward");
		    	cur_road.source=rs.getInt("source");
		    	cur_road.target=rs.getInt("target");
		    	cur_road.osm_id=rs.getLong("osm_id");
		    	cur_road.x1=rs.getDouble("x1");
		    	cur_road.x2=rs.getDouble("x2");
		    	cur_road.y1=rs.getDouble("y1");
		    	cur_road.y2=rs.getDouble("y2");
		    	cur_road.priority=rs.getDouble("priority");
		    	cur_road.name=rs.getString("name");
		    	roadmap.add(cur_road);
		    	if(cur_road.gid>max_gid){
		    		max_gid=cur_road.gid;
		    	}
		    }
		    //将现有的双行线的reverse_cost设为不可达并插入对应的单行线(即将reverse_cost和to_cost拆分成2条记录)
		    RoadSegment cur_road = null;
		    for(int i=0; i<roadmap.size(); i++){
		    	cur_road=roadmap.get(i);
				//无法转换为双行线的道路
		    	if((cur_road.to_cost>10000.0 || cur_road.to_cost<0) || (cur_road.reverse_cost>10000.0 || cur_road.reverse_cost <0)){
		    		continue;
		    	}
		    	else{
		    		try{
				    	//remove the reverse_cost;
				    	String sql="UPDATE "+roadmap_table +" SET reverse_cost="+ inconnectivity+ " WHERE gid="+cur_road.gid;
				    	System.out.println("["+i+"]"+sql);
				    	stmt.executeUpdate(sql);
				    	max_gid++;
				    	if(cur_road.name.contains("'")){
				    		//System.out.println("["+i+"]"+cur_road.name);
				    		cur_road.name=cur_road.name.replaceAll("'", "''");
				    		//System.out.println("["+i+"]"+cur_road.name);
				    	}
				    	sql="INSERT INTO "+roadmap_table +"(gid, class_id, length, name, x1, y1,x2, y2, reverse_cost, rule, to_cost, " +
				    			"maxspeed_forward, maxspeed_backward, osm_id,priority,the_geom,source,target, old_gid) VALUES ("+ max_gid +"," +cur_road.class_id+"," +cur_road.length+",'"+cur_road.name +"'," +cur_road.x1+"," +cur_road.y1+","
				    			+cur_road.x2+","+cur_road.y2+","+cur_road.reverse_cost+",'',"+ inconnectivity +","+cur_road.max_speed+","+cur_road.max_speed+","
				    			+cur_road.osm_id+","+cur_road.priority+",'"+cur_road.geom+"',"+cur_road.source+","+cur_road.target+","+cur_road.gid+")";
				    	System.out.println("["+i+"]"+sql);
				    	stmt.executeUpdate(sql);
				    }
				    //catch (SQLException e) {
		    		catch (Exception e) {
					    e.printStackTrace();
				    }
				    finally{
				    	con.commit();
				    }
		    	}
		    }//finished updating the weight for the given taxi's all samples
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
		/*
		 	drop table oneway_test;
			create table oneway_test as select * from ways_test;
			alter table oneway_test add column old_gid integer;
			update oneway_test set old_gid=gid;
		 */
		try {
			//
			SpliteDualWay.splitways("routing", "oneway_test");
				
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
