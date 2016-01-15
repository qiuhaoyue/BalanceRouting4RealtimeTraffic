import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.TimeZone;
import java.lang.Math;
import java.util.HashMap;
import java.text.DecimalFormat;
 
public class CitationCounter {
	
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
	
	
	public static void get_citation(String database, String roadmap_table, String count_col, String trip_table, String path_col){
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
		
		try{
			stmt = con.createStatement();
			
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="alter table "+ roadmap_table +" add column "+count_col+" bigint;";
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
	    		String sql="update "+ roadmap_table +" set "+count_col+" = 0 ;";
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
			
			//insert the turning_specific travel time
			try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="select * from "+trip_table+" where "+path_col+" is not null and valid=1 order by start_utc, suid;"; 
	    				
	    		System.out.println(sql);
	    		rs=stmt.executeQuery(sql);
	    		
	    		String route;	
	    		ArrayList<String> routes=new ArrayList<String>();
	    		
		    	while(rs.next()){
			    	route=rs.getString(path_col);
			    	if(route!=null && !route.equals("")){
			    		routes.add(route);
			    	}
			    }
		    	
		    	Savepoint spt1=null;
		    	for(int i=0; i<routes.size();i++){
		    		String condition_clause="";
		    		String[] roads=null;
		    		route=routes.get(i);
		    		int gid=0;
			    	if(route!=null){
			    		roads=route.split(",");
			    		for(int j=0;j<roads.length;j++){
			    			
			    			if(!roads[j].equals("")){
			    				gid=Integer.parseInt(roads[j]);
			    				if(condition_clause.equals("")){
			    					condition_clause+=" gid="+gid;
			    				}
			    				else{
			    					condition_clause+=" or gid=";
			    					condition_clause+=gid;
			    				}
			    			}
			    			else{
			    				continue;
			    			}

			    		}
			    	}
		    		
		    		try{
			    		spt1 = con.setSavepoint("svpt1");
			    		sql="update "+ roadmap_table +" set "+count_col+"="+count_col+"+1 where "+ condition_clause+ ";"; 
			    		System.out.println("["+i+"/"+routes.size()+"]:"+sql);
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
	
	public static void main(String[] args){
		
		if(args.length>0){
			try {
				String type=args[0];
				String sample_table="ring2_samples_"+type;
				String roadmap_table="ring2_time_"+type;
				String trip_table="ring2_trip_"+type;
				int min_sample_thres=2;
				
				double max_tempstop_precent=Double.parseDouble(args[1]);//0.2;
				int min_trip_interval=Integer.parseInt(args[2]);//600;
				
				String trip_extention="_";
				trip_extention+=(int)(max_tempstop_precent*10);
				trip_extention+="_"+min_trip_interval;
				trip_table+=trip_extention;

				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else{
			try {
				String roadmap_table="ring2_roads_nonrush";
				String trip_table="ring2_trip_nonrush";
				double max_tempstop_precent=0.5;
				int min_trip_interval=600;

				String extention="_";
				extention+=(int)(max_tempstop_precent*10);
				extention+="_"+min_trip_interval;
				trip_table+=extention;
				
				String path_col="ebksp";
				String count_col=path_col+extention;
				CitationCounter.get_citation("mydb", roadmap_table, count_col, trip_table, path_col);
				
				path_col="trsp";
				count_col=path_col+extention;
				CitationCounter.get_citation("mydb", roadmap_table, count_col, trip_table, path_col);
				
				path_col="rksp";
				count_col=path_col+extention;
				CitationCounter.get_citation("mydb", roadmap_table, count_col, trip_table, path_col);
				
				path_col="eebksp";
				count_col=path_col+extention;
				CitationCounter.get_citation("mydb", roadmap_table, count_col, trip_table, path_col);
				
				path_col="route";
				count_col=path_col+extention;
				CitationCounter.get_citation("mydb", roadmap_table, count_col, trip_table, path_col);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}