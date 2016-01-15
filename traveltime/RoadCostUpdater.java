import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.TimeZone;
import java.lang.Math;
 
public class RoadCostUpdater {
	
	public static final double inconnectivity=1000000.0;
	
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
	
	//根据道路的class_id,即道路级别重新设定to_cost,reverse_cost
	public static void updateWeight(String database, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "p@ssw0rd");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
		    stmt = con.createStatement();
		    System.out.println("select * from " + roadmap_table);
		    rs = stmt.executeQuery("select * from " + roadmap_table);
		    
		    ArrayList<RoadSegment> roadmap= new ArrayList<RoadSegment>();
		    while(rs.next()){
		    	roadmap.add(new RoadSegment(rs.getInt("gid"), rs.getDouble("to_cost"), rs.getDouble("reverse_cost"), rs.getDouble("length"), rs.getInt("class_id")));
		    }
		    
		    RoadSegment cur_road = null;
		    double priority=1.0;
		    
		    for(int i=0; i<roadmap.size(); i++){
		    	
		    	cur_road=roadmap.get(i);
		    	switch(cur_road.class_id){
		    	case 101://
		    		priority=1.0;
		    		break;
		    	case 102:
		    	case 103:
		    		priority=1.10;
		    		break;
		    	case 104:
		    		priority=1.04;
		    		break;
		    	case 105:
		    		priority=1.12;
		    		break;
		    	case 106:
		    		priority=1.08;
		    		break;
		    	case 107:
		    		priority=1.15;
		    		break;
		    	case 108:
		    		priority=1.10;
		    		break;
		    	case 109:
		    		priority=1.20;
		    		break;
		    	case 110:
		    		priority=1.12;
		    		break;
		    	case 111:
		    		priority=1.25;
		    		break;
		    	case 112:
		    		priority=1.30;
		    		break;
		    	case 113:
		    		priority=1.50;
		    		break;
		    	case 114:
		    		priority=1.75;
		    		break;
		    	case 117:
		    		priority=1.30;
		    		break;
		    	default: 
		    		priority=1.30;
		    		break;
		    	}
		    	
		    	if(cur_road.to_cost>10000.0 || cur_road.to_cost<0 ){
		    		//System.out.println("infinite solved");
		    		cur_road.to_cost=inconnectivity;
		    	}
		    	else{
		    		cur_road.to_cost=cur_road.length*priority;
		    	}
		    	
		    	if(cur_road.reverse_cost>10000.0 || cur_road.reverse_cost <0){
		    		//System.out.println("infinite solved");
		    		cur_road.reverse_cost=inconnectivity;
		    	}
		    	else{
		    		cur_road.reverse_cost=cur_road.length*priority;
		    	}

			    try{
			    	// find the segment whose distance to cur_sample is smaller than the threshold;
			    	String sql="UPDATE "+roadmap_table +" SET to_cost="+cur_road.to_cost+ ", reverse_cost="+cur_road.reverse_cost +" WHERE gid="+cur_road.gid;
			    	System.out.println("["+i+"]"+sql);
			    	stmt.executeUpdate(sql);
			    }
			    catch (SQLException e) {
				    e.printStackTrace();
			    }
			    finally{
			    	con.commit();
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
		
		for(int i=0; i<1; i++){
			try {
				//RoadCostUpdater.updateWeight("mydb", "ways_test");
				RoadCostUpdater.updateWeight("routing", "oneway_test");
				
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
