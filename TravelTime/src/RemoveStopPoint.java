/** 
 * 2016��3��11�� 
 * RemoveStopPoint.java 
 * author:ZhangYu
 */

import java.sql.*;
import java.util.ArrayList;

public class RemoveStopPoint {
	
	//����λ��Ч�ĵ�ɸȥ,��Ч��¼����match_table
	public static void create_matchtable(String database, String sample_table, String match_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    
		    try{
	    		String sql="create TABLE "+match_table+" as select * from "+sample_table
	    				+" where ostdesc not like '%��λ��Ч%';";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
			    sql="CREATE INDEX suid_idx ON " + match_table + " (suid);";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
			    sql="CREATE INDEX suid_utc_idx ON " + match_table + " (suid,utc);";
			    System.out.println(sql);
			    stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback();
			}
			finally{
				con.commit();
			}
		    
		    try{
	    		String sql="ALTER TABLE "+match_table+" add column stop integer;";
	    		System.out.println(sql);
	    		stmt.executeUpdate(sql);
	    	}
	    	catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback();
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
		    Common.dropConnection(con);
		}
		//System.out.println("get_suids finished!");
	}
	
	//����stop��ֵ,������ʱ�����Լ���������Ϊ�жϱ�׼
	public static void label_stop(String database, int suid, String sample_table, ArrayList<Sample> trajectory){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		double interval_threshold = 300.0;
		double distance_threshold = 50.0;
		double temp_distance_threshold = MapMatching.sigma_with_stop*2;
		
		con = Common.getConnection(database);
		if (con == null) {
			System.out.println("Failed to make connection!");
			return;
		}
		
		try {
			stmt = con.createStatement();
			String sql="";
			int start_pos=0;
			int cur_pos=0;
			double start_time=trajectory.get(start_pos).utc.getTime()/1000;
			double current_time=0;
			double distance=0;
			Sample cur_sample=null, pre_sample=null;
			int pre_start_pos=0;
			for(cur_pos=1; cur_pos<trajectory.size(); cur_pos++){
				cur_sample=trajectory.get(cur_pos);
			    current_time=cur_sample.utc.getTime()/1000;
			    pre_start_pos=start_pos;
			    start_pos=cur_pos;
			    	
				//�õ��൱ǰ����볬����ֵ�ĵ���Ϊ��ʼ��
			    //get the distance_threshold-based clustering
			    for(int j=cur_pos-1; j>=pre_start_pos; j--){
			    	pre_sample=trajectory.get(j);
			    	sql="select ST_Distance_Sphere(ST_GeomFromText('POINT(" +
			    			pre_sample.lon + " " + pre_sample.lat + ")', 4326),"+
			    			"ST_GeomFromText('POINT(" +
			    			cur_sample.lon + " " + cur_sample.lat + ")', 4326));";
			    	//System.out.println(sql);
			    	rs = stmt.executeQuery(sql);
		    		distance=0.0;
				    while(rs.next()){
				    	distance=rs.getDouble("st_distance_sphere");
					}
				    if(distance>distance_threshold){
				    	break;
				    }
				    start_pos=j;
			    }
			    	
			    //test whether interval of cluster exceeds the interval_threshold;
			    start_time=trajectory.get(start_pos).utc.getTime()/1000;
			    if(current_time-start_time>interval_threshold){
			    	for(int j=cur_pos; j>=start_pos; j--){
				    	cur_sample=trajectory.get(j);
				    	if(cur_sample.stop != Sample.LONG_STOP){
					    	cur_sample.stop=Sample.LONG_STOP;
					    	trajectory.set(j, cur_sample);
				    	}
				    }
			    }
			}
			    
			//write the test result into database
			ArrayList<String> updates=new ArrayList<String>();
			//String static_sql="UPDATE "+ sample_table +" SET stop="+ cur_sample.stop +" WHERE suid="+suid+" and ";
			for(cur_pos=0; cur_pos<trajectory.size(); cur_pos++){
			    cur_sample=trajectory.get(cur_pos);
			    if(cur_sample.stop==0){
			    	continue;
			    }
			    String newsql=" UPDATE "+ sample_table +" SET stop="+ cur_sample.stop +" WHERE suid="+suid+" and utc="+(int)(cur_sample.utc.getTime()/1000)+";\n";
			    updates.add(newsql);
		    	if(updates.size()>500){
		    		//sql="";
					try{
						for(int vi=0; vi<updates.size(); vi++){
		    		    	//sql+=updates.get(vi);
				    		stmt.addBatch(updates.get(vi));
		    		    }
				    	//System.out.println("["+i+"/"+trips.size()+"]");
		    		    //stmt.executeUpdate(sql);
				    	stmt.executeBatch();
		    		    	
				    }
				    catch (SQLException e) {
				    	//System.err.println(sql);
						e.printStackTrace();
						con.rollback();
					}
					finally{
						con.commit();
						updates.clear();
					}	
		    	}
			}
			if(updates.size()>0){
	    		//sql="";
				try{
			    	for(int vi=0; vi<updates.size(); vi++){
	    		    	//sql+=updates.get(vi);
			    		stmt.addBatch(updates.get(vi));
	    		    }
			    	//System.out.println("["+i+"/"+trips.size()+"]");
	    		    //stmt.executeUpdate(sql);
	    		    stmt.executeBatch();
			    }
			    catch (SQLException e) {
			    	//System.err.println(sql);
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
			if (e instanceof BatchUpdateException)
    		{
        		BatchUpdateException bex = (BatchUpdateException) e;
        		bex.getNextException().printStackTrace(System.out);
    		}
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		finally {
		    Common.dropConnection(con);
		}
		System.out.println("label_stop finished!");
	}
}
