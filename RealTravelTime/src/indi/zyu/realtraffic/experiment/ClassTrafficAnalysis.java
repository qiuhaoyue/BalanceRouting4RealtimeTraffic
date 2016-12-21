/** 
* 2016Äê11ÔÂ7ÈÕ 
* ClassTrafficAnalysis.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.experiment;

import indi.zyu.realtraffic.common.Common;

import java.sql.SQLException;
import java.util.ArrayList;

public class ClassTrafficAnalysis {

	private static String date = "_2010_04_08";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Common.init(40000);
		Common.init_roadlist();//initialize roadlist
		
		int[] class_id_list = {106,120,107,304,118,125,303,305,119,117,104,102,122,111,108,105
									,123,100,109,112,201,101,110,124,202,113,301,114};
		int length = Common.roadlist.length;
		ArrayList<Double> per_list = new ArrayList<Double>();
		try {
			for(int class_id:class_id_list){
				//only select records where is_sensed = true, modify sql String in RoadTrafficAnalysis
				RoadTrafficAnalysis class_analyzer = new RoadTrafficAnalysis(date, class_id);
				//1~287
				for(int i=1; i < (int)Common.max_seg; i++){
					double class_speed = class_analyzer.default_class_traffic[class_id][i];
					Common.logger.debug("class speed: " + class_speed);
					if(class_speed <= 0){
						Common.logger.debug("class speed error");
						continue;
					}
					//compare change of road speed and class speed
					for(int j=0; j<length; j++){
						//
						double cur_speed = class_analyzer.road_traffic[j][i];
						double next_speed = class_analyzer.road_traffic[j][i+1];
						if(cur_speed > 0 && next_speed > 0){
							double diff_speed = Math.abs(cur_speed - next_speed);
							double per = diff_speed / class_speed;
							per_list.add(per);
						}
					}
				}
			}
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int counter = 0;
		for(double i= 0.1; i<=1; i+=0.1){
			counter = 0;
			for(double per:per_list){
				if(per > i){
					counter++;
				}
			}
			double ratio = (double)counter / per_list.size();
			Common.logger.debug("per>" + i + ":" + ratio);
		}
	}

}
