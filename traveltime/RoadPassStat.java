import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.TimeZone;
import java.lang.Math;
import java.util.HashMap;
 
public class RoadPassStat {
	
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
	
	public static void get_gids(String database, String roadmap_table, ArrayList<String> road_list){
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
		    String sql="select gid from " + roadmap_table + " order by gid";

		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    if(road_list==null){
		    	return;
		    }
		    road_list.clear();
		    while(rs.next()){
		    	road_list.add(rs.getString("gid"));
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
		System.out.println("get_gids finished!");
	}
	
	public static void passStat(String database, /*String road, */String sample_table, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		int reference_count=0;
		Savepoint spt=null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {

		    stmt = con.createStatement();
		    /*String sql="select count(*) from " + sample_table + " where route like '%,"+road+",%' or route like '"+road+",%' and (ostdesc not like '%定位无效%') ;";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);

		    reference_count=0;
		    while(rs.next()){
		    	reference_count=rs.getInt("count");
		    }*/
		    /*
		    String sql="select speed from " + sample_table + " where gid="+road+" and (stop is null or stop!=1) and (ostdesc not like '%定位无效%') order by speed desc";
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);*/
		    
		    /*
		    try{
			    String sql="create table "+ "init_speed_"+sample_table +" as select gid, count(*) as count, avg(speed)/100 as avg_speed, max(speed)/100 as max_speed from " + sample_table + " where (gid is not null) and (stop is null or stop!=1) and (ostdesc not like '%定位无效%') and speed>0 group by gid";
			    System.out.println(sql);
			    rs = stmt.executeQuery(sql);
		    }
		    catch (Exception e) {
				e.printStackTrace();
			}
			finally{
			   con.commit();
			}
		    
		    try{
			    String sql="create index "+ "init_speed_"+sample_table +"_gid_idx on "+ "init_speed_"+sample_table +"(gid);";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
		    }
		    catch (Exception e) {
				e.printStackTrace();
			}
			finally{
			   con.commit();
			}*/
		    
		    /*
		    ArrayList<Double> speed_list=new ArrayList<Double>();
		    double everage_speed=0;
		    double max_speed=0;
		    double count=0;
		    while(rs.next()){
		    	speed_list.add((double)rs.getInt("speed")/100.0);
		    }
		    for(int i=0; i<speed_list.size(); i++){
		    	if(speed_list.get(i)>0){
		    		everage_speed+=speed_list.get(i);
		    		count++;
		    	}
		    }
		    if(count==0){
		    	everage_speed=0;
		    }
		    else{
		    	everage_speed/=count;
		    }
		    if(speed_list.size()>0){
		    	max_speed=speed_list.get(0);
		    }*/

		    try{
			    String sql="UPDATE "+roadmap_table +" SET reference=0, average_speed=0, max_speed=0;";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			finally{
			   con.commit();
			}
		    
			try{
			    String sql="UPDATE "+roadmap_table +" SET reference="+"init_speed_"+sample_table+".count, average_speed="+"init_speed_"+sample_table+".avg_speed, max_speed="+"init_speed_"+sample_table+".max_speed" +
			    		" from init_speed_"+sample_table+" WHERE "+roadmap_table +".gid="+"init_speed_"+sample_table+".gid;";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
			}
			catch (Exception e) {
				e.printStackTrace();
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
		finally {
		    DBconnector.dropConnection(con);
		}
		System.out.println("passStat!");
	}
	

	public static void create_index_on_samples(String database, String sample_table){
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {

		    stmt = con.createStatement();
			try{
				spt = con.setSavepoint("svpt3");
				String sql="DROP INDEX "+sample_table+"_gid_idx;";
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
				String sql="CREATE INDEX "+sample_table+"_gid_idx ON "+ sample_table +"(gid);";
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
		finally {
		    DBconnector.dropConnection(con);
		}
		System.out.println("passStat!");
	}
	
	public static void change_scheme(String database, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		Savepoint spt=null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {

		    stmt = con.createStatement();
		    try{
	    		spt = con.setSavepoint("svpt1");
	    		String sql="ALTER TABLE "+ roadmap_table +" DROP COLUMN reference;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" ADD COLUMN reference integer;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" DROP COLUMN average_speed;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" ADD COLUMN average_speed double precision;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" DROP COLUMN max_speed;";

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
	    		String sql="ALTER TABLE "+ roadmap_table +" ADD COLUMN max_speed double precision;";

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
		finally {
		    DBconnector.dropConnection(con);
		}
		System.out.println("passStat!");
	}
	
	public static void get_class2speed(String database, String roadmap_table, HashMap<Integer, RoadClass> hmp_class2speed){
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
		    String sql="select class_id, count(*), avg(max_speed) as avg_max, stddev(max_speed) as dev_max, " +
		    		"avg(average_speed) as avg_avg, stddev(average_speed) as dev_avg from " + roadmap_table + 
		    		" where max_speed>0 and average_speed >0 group by class_id order by class_id";
		    
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    
		    if(hmp_class2speed==null){
		    	return;
		    }
		    hmp_class2speed.clear();
		    while(rs.next()){
		    	hmp_class2speed.put(rs.getInt("class_id"), new RoadClass(rs.getDouble("avg_avg"),rs.getDouble("dev_avg"), rs.getDouble("avg_max"),rs.getDouble("dev_max"), rs.getLong("count")));
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
		System.out.println("get_gids finished!");
	}
	
	public static void improve_speed(String database, String roadmap_table, String new_roadmap, HashMap<Integer, RoadClass> hmp_class2speed){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList<RoadSegment> roadmap=new ArrayList<RoadSegment>();
		Savepoint spt=null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    if(new_roadmap!=null){
		    	try{
		    		spt = con.setSavepoint("svpt1");
		    		String sql="drop table "+new_roadmap;
		    		stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
				catch (Exception e) {
				    e.printStackTrace();
				}
				finally {
					con.commit();
				}
		    	try{
		    		spt = con.setSavepoint("svpt1");
			    	String sql="create table "+new_roadmap+" as select * from " + roadmap_table;
			    	stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
				catch (Exception e) {
				    e.printStackTrace();
				}
				finally {
					con.commit();
				}
		    	try{
		    		spt = con.setSavepoint("svpt1");
			    	String sql="create index "+new_roadmap+"_gid_index on " + new_roadmap +"(gid);";
			    	stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
				catch (Exception e) {
				    e.printStackTrace();
				}
				finally {
					con.commit();
				}
		    }
		    String sql="select * from " + new_roadmap;
		    //System.out.println(sql);
		    rs = stmt.executeQuery(sql);
		    while(rs.next()){
		    	RoadSegment road=new RoadSegment(rs.getInt("gid"), rs.getDouble("to_cost"), rs.getDouble("reverse_cost"), rs.getDouble("length"), rs.getInt("class_id"));
		    	road.avg_inst_speed=rs.getDouble("average_speed");
		    	road.max_inst_speed=rs.getDouble("max_speed");
		    	road.reference=rs.getInt("reference");
		    	roadmap.add(road);
			}
		    
		    for(int i =0; i<roadmap.size(); i++){
		    	boolean default_update=false;
		    	boolean specific_update=false;
		    	
		    	RoadSegment cur_road=roadmap.get(i);
		    	double class_avg_speed=hmp_class2speed.get(cur_road.class_id).avg_speed;
		    	double class_dev_avg=hmp_class2speed.get(cur_road.class_id).dev_avg;
		    	if(cur_road.avg_inst_speed==0){
		    		cur_road.avg_inst_speed=class_avg_speed;
		    		default_update=true;
		    	}
		    	else{
		    		if(cur_road.avg_inst_speed>class_avg_speed+class_dev_avg){
		    			cur_road.avg_inst_speed=class_avg_speed+class_dev_avg;
		    			specific_update=true;
		    		}
		    		else if(cur_road.avg_inst_speed<class_avg_speed-class_dev_avg) {
		    			cur_road.avg_inst_speed=class_avg_speed-class_dev_avg;
		    			specific_update=true;
		    		}	
		    	}
		    	
		    	double class_max_speed=hmp_class2speed.get(cur_road.class_id).max_speed;
		    	double class_dev_max=hmp_class2speed.get(cur_road.class_id).dev_max;
		    	if(cur_road.max_inst_speed==0){
		    		cur_road.max_inst_speed=class_max_speed;
		    		default_update=true;
		    	}
		    	else{
		    		if(cur_road.max_inst_speed>class_max_speed+class_dev_max){
		    			cur_road.max_inst_speed=class_max_speed+class_dev_max;
		    			specific_update=true;
		    		}
		    		else if(cur_road.avg_inst_speed<class_max_speed-class_dev_max) {
		    			cur_road.max_inst_speed=class_max_speed-class_dev_max;
		    			specific_update=true;
		    		}	
		    	}
		    	
		    	if(!specific_update && !default_update){
		    		continue;
		    	}
		    	
		    	try{
		    		spt = con.setSavepoint("svpt1");
		    		sql="UPDATE "+ new_roadmap +" SET max_speed="+cur_road.max_inst_speed+ ", average_speed="+cur_road.avg_inst_speed +" WHERE gid="+ cur_road.gid;
		    		/*
		    		String reason="";
		    		if(specific_update){
		    			reason+="specific_update";
		    		}
		    		else{
		    			reason+="default_update";
		    		}*/
				    System.out.println("improve_speed:["+i+"/"+roadmap.size()+"]"+ sql + " cur_road.class_id="+cur_road.class_id);
				    stmt.executeUpdate(sql);
		    	}
		    	catch (SQLException e) {
				    e.printStackTrace();
				    con.rollback(spt);
				}
				catch (Exception e) {
				    e.printStackTrace();
				}
				finally {
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
		System.out.println("update_speed finished!");
	}

	
	public static void main(String[] args){
		
		/*
		 *  create table ring2_roads as 
			select * from oneway_test 
			where y1>39.86480 and y1<=39.95050 and x1>116.33990 and x1<=116.44220 
				and y2>39.86480 and y2<=39.95050 and x2>116.33990 and x2<=116.44220
				
			alter table ring2_roads add column reference integer;
			alter table ring2_roads add column average_speed double precision;
			alter table ring2_roads add column max_speed double precision;
				
			create table ring2_samples_non_rush as select * from match_part_3
			insert into ring2_samples_non_rush select * from match_part_4
			
		 */
			try {
				
				/*
				ArrayList<String> gid_list=new ArrayList<String>();
				RoadPassStat.change_scheme("mydb", "ring2_roads");
				RoadPassStat.get_gids("mydb", "ring2_roads", gid_list);
				for(int i=0; i<gid_list.size();i++){
					System.out.print((i+1)+"/"+gid_list.size()+"	");
					RoadPassStat.passStat("mydb", gid_list.get(i), "ring2_samples_nonrush", "ring2_roads");
				}
				
				HashMap<Integer, RoadClass> hmp_class2speed= new HashMap<Integer, RoadClass>();
				RoadPassStat.get_class2speed("mydb", "ring2_roads", hmp_class2speed);
				RoadPassStat.improve_speed("mydb", "ring2_roads", "ring2_roads_speed", hmp_class2speed);*/
				
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/*
			 * 
			 * 
			 	select class_id, avg(max_speed), stddev(max_speed), avg(average_speed), stddev(average_speed), count(*) 
					from ring2_roads 
					where max_speed!=0 and average_speed !=0 
					group by class_id 
					order by class_id
			 * 
			 * 
			 *  class_id; max_speed, stddev(max_speed), avg_speed, stddev(avg_speed), count(*)
			 	
				motorway: 		101;	14.5907142857143;	5.22027035003672;	10.8067885031635;	5.29460738891671;	14
				motorway_link 	102;	11.6061904761905;	4.00286418941881;	8.70425903991551;	3.33033621659711;	42
				trunk 			104;	14.4472222222222;	4.02903782395571;	8.02535097338118;	3.96582237329326;	270
				trunk_link 		105;	9.64919191919192;	3.96979806061929;	7.07015639581161;	3.33490697989385;	297
				primary 		106;	13.03722323049;		4.05449637081932;	7.54452375048608;	2.83873592928136;	551
				primary_link 	107;	8.68971014492753;	3.66252833217493;	6.72580177742949;	3.241490574985;		138
				secondary		108;	9.75989989989991;	3.43978362877894;	6.05060135748775;	2.44952979850291;	999
				secondary_link	109;	6.92228723404255;	3.03039002845213;	5.0132033013582;	2.34884684085835;	188
				tertiary		110;	8.85370828182942;	3.42857115652804;	5.88244004587638;	2.53043581285042;	2427
				tertiary_link	111;	6.94631386861314;	3.48196882172781;	5.09351686584752;	2.59394383175926;	274
				residential		112;	5.80692615912994;	3.02900866077545;	4.47973522050739;	2.42669138411856;	1747
				service			114;	4.5169214876033;	2.99615817257122;	4.05157523433866;	2.74357825676193;	484
				unclassified	117;	6.732;				3.02506610300275;	5.39758894139654;	2.72101338728285;	70
				
				100;3.44166666666667;2.57745158376771;3.44166666666667;2.57745158376771;6
				113;0.1;;0.0666666666666667;;1

			 */
		
	}
}