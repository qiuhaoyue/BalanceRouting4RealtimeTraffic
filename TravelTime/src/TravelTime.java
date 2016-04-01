import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

/** 
 * 2016年3月11日 
 * TravelTime.java 
 * author:ZhangYu
 */

public class TravelTime {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String DataBase    = "taxi_data";
		String OneWayTable = "oneway_test";
		String OriginSampleTable = "gps";
		String ValidSampleTable  = "valid_gps_test2";
		//String FilterSampleTable = "gps_ring2";
		String FilterSampleTable = ValidSampleTable;
		String IntersectionTable = "oneway_intersection";
		
		SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String datetime = tempDate.format(new java.util.Date());
    	System.out.println("-----travel time process start:	"+datetime+"-------!");
    		
		try {
			// commented code are some operations on map, just run once
			/*
			//create table oneway_test as select * from ways;
			//根据道路的class_id,即道路级别重新设定to_cost,reverse_cost
			RoadCostUpdater.updateWeight(DataBase, OneWayTable);
			
			//将ways表中的单行线划分成双行线,即将reverse_cost和to_cost拆分成2条记录
			SpliteDualWay.splitways(DataBase, OneWayTable);
			
			//将定位有效的点筛出存入新表
			RemoveStopPoint.create_matchtable(DataBase, OriginSampleTable, ValidSampleTable);//add by zyu
			
			*/
			long min_utc=1270483200;
			long max_utc=1270569600;

			//2nd_ring area;
			long min_x=11633990L;
			long max_x=11644220L;
			long min_y=3986480L;
			long max_y=3995050L;
		
			//all area
			/*
			long min_x=11618690L;
			long max_x=11656400L;
			long min_y=3974050L;
			long max_y=4003970L;
			*/
			//long start_utc=min_utc; 
			//long end_utc=max_utc;
			long start_utc=0; 
			long end_utc=0;
			Common.change_schema(DataBase,ValidSampleTable);
			Common.filter_samples(DataBase, ValidSampleTable, FilterSampleTable, 
					start_utc, end_utc, min_x, max_x, min_y, max_y);
			
			int max_suid;
			System.out.println("-----get taxi data start:	"+datetime+"-------!");
			max_suid = Common.get_taxi_data(DataBase, FilterSampleTable);
			if(max_suid == -1){
				System.out.print("get_taxi_data error");
				return;
			}
			datetime = tempDate.format(new java.util.Date());
			
			System.out.println("-----get taxi data finish:	"+datetime+"-------!");
			System.out.println("-----label stop point and map matching for every suid:	"+datetime+"-------!");
			//for(int i=0; i<max_suid;i++){
			for(int i=17250; i<max_suid;i++){
				if(Common.taxi[i] == null){
					continue;
				}
				System.out.println("suid " + i +": ");
				
				//根据距离与时间间隔插入long_stop值
				RemoveStopPoint.label_stop(DataBase, i, FilterSampleTable, Common.taxi[i]);
				
				datetime = tempDate.format(new java.util.Date());
		    	System.out.println("-----map matching start:	"+datetime+"-------!");
		    	//Common.clear(DataBase, ValidSampleTable);
				//gps数据匹配道路，使用隐马尔科夫算法(HMMM)算法(vertibi动态规划有待优化,可以进行剪枝),
				//添加：Gid，Edge_offset,route三个字段，分别表示将这个点匹配的位置，偏移以及这个点的前一个点到当前sample通过的路径。
				MapMatching.mapMatching("taxi_data", i, Common.taxi[i], FilterSampleTable, 
						OneWayTable, IntersectionTable);
						
			}
			datetime = tempDate.format(new java.util.Date());
			System.out.println("-----finished label stop point and map matching:	"+datetime+"-------!");
			
			
			 
			try {
				System.out.println("-----start initialize road speed:	"+datetime+"-------!");
				//calculate initial road_speed,just run once
				ArrayList<String> gid_list=new ArrayList<String>();
				RoadInitState.change_scheme(DataBase, "oneway_test");
				/*RoadInitState.get_gids("taxi_data", "oneway_test", gid_list);
				for(int i=0; i<gid_list.size();i++){
					System.out.print((i+1)+"/"+gid_list.size()+"	");
					RoadInitState.passStat("taxi_data", gid_list.get(i), "gps_ring2", "oneway_test");
				}*/
				RoadInitState.passStat(DataBase, FilterSampleTable, OneWayTable);
				HashMap<Integer, RoadClass> hmp_class2speed= new HashMap<Integer, RoadClass>();
				RoadInitState.get_class2speed(DataBase, OneWayTable, hmp_class2speed);
				RoadInitState.improve_speed(DataBase, OneWayTable, "all_roads_speed", hmp_class2speed);
				
				datetime = tempDate.format(new java.util.Date());
				System.out.println("-----finish initialize road speed:	"+datetime+"-------!");
				/*TravelTimeAllocation.change_schema(DataBase, FilterSampleTable);
				for(int i=0; i<max_suid;i++){
					if(Common.taxi[i] == null){
						continue;
					}
					System.out.print("suid " + i +": ");
					TravelTimeAllocation.preprocess_intervals(DataBase, FilterSampleTable, i, Common.taxi[i]);
							
				}	*/
				ArrayList<String> taxi_list=new ArrayList<String>();
				TravelTimeAllocation.get_suids("taxi_data", FilterSampleTable, taxi_list);
				datetime = tempDate.format(new java.util.Date());
				System.out.println("-----start preprocess interval:	"+datetime+"-------!");
				TravelTimeAllocation.preprocess_intervals(DataBase, FilterSampleTable, taxi_list);
				datetime = tempDate.format(new java.util.Date());
				System.out.println("-----finish preprocess interval:	"+datetime+"-------!");
				
				System.out.println("-----start calculate travel time:	"+datetime+"-------!");
				//for different period, use different Travel_time
				AllocationRoadsegment[] roadlist=TravelTimeAllocation.get_roadlist(DataBase, OneWayTable, false);
				end_utc=1270569600L;//1231218000-1231221600
				long cur_utc=1270483200L;
				long period=900L;
				long period_end_utc=cur_utc+period;
				long max_seg=(end_utc-cur_utc)/period;
				long seq_num=0;

				String allocation_table="allroad_allocation_nonrush_";
				String time_table="allroad_time_nonrush_";
				while(period_end_utc <= end_utc){
					seq_num=max_seg-(end_utc-period_end_utc)/period;
					datetime = tempDate.format(new java.util.Date());
					System.out.println("-----seq number:"+ seq_num + "/" + max_seg + "	" + datetime + "-------!");
					TravelTimeAllocation.allocate_time(DataBase, roadlist, FilterSampleTable, allocation_table+seq_num, /*taxi_list, */cur_utc, period_end_utc);
					TravelTimeAllocation.aggregate_time(DataBase, OneWayTable, allocation_table+seq_num, time_table+seq_num, cur_utc, period_end_utc);

					roadlist=TravelTimeAllocation.get_roadlist(DataBase, time_table+seq_num, true);
					cur_utc=period_end_utc;
					period_end_utc=cur_utc+period;
				}
					
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		datetime = tempDate.format(new java.util.Date());
		System.out.println("-----all done:	"+datetime+"-------!");
	}
}
