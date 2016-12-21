/** 
* 2016Äê8ÔÂ17ÈÕ 
* HistoryTraffic.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.experiment;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.road.AllocationRoadsegment;

//to generate default road speed

public class HistoryTraffic {
	
	private static double[][] history_traffic = null;
	
	private static String week0201_Date_Suffix = "_2016_08_20";
	private static String week0202_Date_Suffix = "_2016_08_21";
	private static String week0203_Date_Suffix = "_2016_08_22";
	private static String week0204_Date_Suffix = "_2016_08_23";
	private static String week0205_Date_Suffix = "_2016_08_24";
	private static String week0207_Date_Suffix = "_2016_08_04";
	private static String week0208_Date_Suffix = "_2016_08_11";
	
	public static void main(String[] args) throws SQLException{		
		Common.init(40000);
		Common.init_roadlist();//initialize roadlist
		
		int length = Common.roadlist.length;
		history_traffic = new double[length][(int)Common.max_seg + 1];
		//clear_history_table();
		generate_history_traffic();
		//add_class_id();
	}
	
	//use week data to generate default road speed
	public static void generate_history_traffic(){
		try{
			RoadTrafficAnalysis week_0201_analyzer = new RoadTrafficAnalysis(week0201_Date_Suffix,108);
			/*TrafficAnalysis week_0202_analyzer = new TrafficAnalysis(week0202_Date_Suffix);
			TrafficAnalysis week_0203_analyzer = new TrafficAnalysis(week0203_Date_Suffix);
			TrafficAnalysis week_0204_analyzer = new TrafficAnalysis(week0204_Date_Suffix);
			TrafficAnalysis week_0205_analyzer = new TrafficAnalysis(week0205_Date_Suffix);
			TrafficAnalysis week_0207_analyzer = new TrafficAnalysis(week0207_Date_Suffix);
			TrafficAnalysis week_0208_analyzer = new TrafficAnalysis(week0208_Date_Suffix);*/
			
			//read traffic
			for(int i=1; i<= Common.max_seg; i++){
				for(int j=0; j<Common.roadlist.length; j++){
					double ave_speed = 0;
					int counter = 0;
					
					if(week_0201_analyzer.road_traffic[j][i] > 0){
						counter++;
						ave_speed += week_0201_analyzer.road_traffic[j][i];
					}
					/*if(week_0202_analyzer.road_traffic[j][i] > 0){
						counter++;
						ave_speed += week_0202_analyzer.road_traffic[j][i];
					}
					if(week_0203_analyzer.road_traffic[j][i] > 0){
						counter++;
						ave_speed += week_0203_analyzer.road_traffic[j][i];
					}
					if(week_0204_analyzer.road_traffic[j][i] > 0){
						counter++;
						ave_speed += week_0204_analyzer.road_traffic[j][i];
					}
					if(week_0205_analyzer.road_traffic[j][i] > 0){
						counter++;
						ave_speed += week_0205_analyzer.road_traffic[j][i];
					}
					if(week_0207_analyzer.road_traffic[j][i] > 0){
						counter++;
						ave_speed += week_0207_analyzer.road_traffic[j][i];
					}
					if(week_0208_analyzer.road_traffic[j][i] > 0){
						counter++;
						ave_speed += week_0208_analyzer.road_traffic[j][i];
					}*/
					
					ave_speed /= counter;
					if(ave_speed > 0){
						Common.history_traffic_updater.insert(j, i, ave_speed);
					}			
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//clear slice  table in certain date
	public static void clear_history_table() throws SQLException{
		Connection con = Common.getConnection();
		try{
			Statement stmt = con.createStatement();
			for(int i=1; i<= Common.max_seg; i++){
				//drop road and turning time table
				String road_slice_table = Common.history_road_slice_table + i;
				String sql = "DROP TABLE IF EXISTS " + road_slice_table + ";";
				Common.logger.debug(sql);
				stmt.executeUpdate(sql);
					
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			con.commit();
			Common.logger.debug("clear traffic_slice_table finished");
		}
	}

	//alter table to add column class_id and set corresponding value
	public static void add_class_id(){
		try{
			Connection con = Common.getConnection();
			Statement stmt = con.createStatement();
			String sql = "";
			for(int i=1; i<= Common.max_seg; i++){
				String table = Common.history_road_slice_table + i;
				sql = "alter table " + table + " add column class_id integer";
				stmt.executeUpdate(sql);
				for(int j=0; j< Common.roadlist.length; j++){
					AllocationRoadsegment road = Common.roadlist[j];
					sql = "update " + table + " set class_id=" + road.class_id + 
							" where gid= " + j;
					stmt.executeUpdate(sql);
				}
				Common.logger.debug("finish " + i);
			}
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		
	}
}
