/** 
* 2016年7月2日 
* Chart.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.experiment;

import indi.zyu.realtraffic.common.Common;

import java.awt.Color;
import java.awt.Font;
import java.awt.RenderingHints;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.Plot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.general.DatasetUtilities;

public class Chart {
	private static final String CHART_PATH = "/home/zyu/";
	
	private static String normal_Date_Suffix = "_2016_07_22";
	private static String speed2_Date_Suffix = "_2016_07_23";
	private static String speed4_Date_Suffix = "_2016_07_24";
	private static String speed8_Date_Suffix = "_2016_07_25";
	
	private static String window4_Date_Suffix = "_2016_07_23";
	private static String window6_Date_Suffix = "_2016_07_26";
	private static String window8_Date_Suffix = "_2016_07_27";
	private static String window10_Date_Suffix = "_2016_07_28";
	private static String window12_Date_Suffix = "_2016_07_29";
	
	private static String week0201_Date_Suffix = "_2016_07_30";
	private static String week0202_Date_Suffix = "_2016_07_31";
	private static String week0203_Date_Suffix = "_2016_08_01";
	private static String week0204_Date_Suffix = "_2016_08_02";
	private static String week0205_Date_Suffix = "_2016_08_03";
	private static String week0207_Date_Suffix = "_2016_08_04";
	private static String week0208_Date_Suffix = "_2016_08_11";
	
	private double[][] data = null;
	private String[] rowKeys = null;
	private String[] columnKeys = null;
	
	public Chart(double[][] data, String[] rowKeys, String[] columnKeys){
		this.data = data;
		this.rowKeys = rowKeys;
		this.columnKeys = columnKeys;
	}
	
	public static void main(String[] args) throws SQLException { 
		
		Common.start_utc = 1265472000L;
		Common.end_utc = 1265558400L;
		Common.init(40000);
		//Common.clear_travel_table("_2016_08_17");
		//Common.clear_travel_table("");
		Common.init_roadlist();//initialize roadlist
		//showHistoryTraffic();
		//compareWeekTraffic();
		compareTrafficBySpeedClass();
		compareTrafficBySpeed();
		//compareTrafficByWindowSize2();
		//compareMapMatching();
		
    }
	
	public static void compareMapMatching(){
		double[][] error_rate = {{0.1488865595653197,0.1285676406639444,0.11698409068635458,0.111594864542853,
			0.10807177978953172,0.10582868119334894,0.10485676199099575,0.10613759710545918,0.10579139918547663
			,0.10433786402274302,0.10605214426383704,0.10610540433444082,0.10901013082445542,0.10873792497804541,
			0.10906992553712867,0.10775620159389382,0.108879808650588,0.10609153770167928,0.10633342896197917},
			{0.06305804766289104,0.018096712699023566,0.01906744609275906,0.020967465427254372,0.020964796365310092,
				0.021767965649395246,0.022259843443570963,0.022650465961048426,0.022987050796825358,0.02250364491052807,
				0.02241779111132518,0.022455403987408185,0.02493201702530149,0.022404450490572346,0.022696274500789235,
				0.022343711005739956,0.02164000750426193,0.021107054982873134,0.02115214525307478}};
		
		String[] rowKeys = {"map-matching error rate","routing error rate"};
		String[] columnKeys = new String[19];
		for(int i=0; i< 19; i++){
			columnKeys[i] = String.valueOf(i+2);
		}
		Chart chart = new Chart(error_rate, rowKeys, columnKeys);
		chart.makeLineAndShapeChart("error_rate.png");
		
	}
	
	public static void compareTrafficBySpeedClass(){
		int gid = 195980;
		//int gid = 203888;
		try {
			//read traffic data from database
			//int seg = 12;//12:00-13:00
			int trunk_id = 104;
			int primary_id = 106;
			int secondary_id = 108;
			int tertiary_id = 109;
			TrafficAnalysis trunk_analyzer = new TrafficAnalysis("_2010_02_07", trunk_id);
			TrafficAnalysis primary_analyzer = new TrafficAnalysis("_2010_02_07", primary_id);
			TrafficAnalysis secondary_analyzer = new TrafficAnalysis("_2010_02_07", secondary_id);
			TrafficAnalysis tertiary_analyzer = new TrafficAnalysis("_2010_02_07", tertiary_id);
		
			//TrafficAnalysis offline_analyzer = new TrafficAnalysis(seg);
			
			
			int seg = (int)Common.max_seg;
			//TrafficAnalysis offline_analyzer = new TrafficAnalysis(seg);
			
			//double[][] traffic = new double[3][(int)Common.max_seg];
			double[][] traffic = new double[4][seg];
			int total_traffic = 0;
			for(int i=1; i<= seg; i++){
				//traffic[0][i-1] = normal_analyzer.average_speed[i+144];
			//	traffic[0][i-1] = normal_analyzer.road_traffic[gid][i+144];
				//traffic[0][i-1] = normal_analyzer.road_traffic[gid][i];
				traffic[0][i-1] = trunk_analyzer.average_speed[i];
				traffic[1][i-1] = primary_analyzer.average_speed[i];
				traffic[2][i-1] = secondary_analyzer.average_speed[i];
				traffic[3][i-1] = tertiary_analyzer.average_speed[i];
				//total_traffic += normal_analyzer.traffic_counter[i];
				//Common.logger.debug(total_traffic);
			}
			//Common.logger.debug("total traffic number: " + total_traffic);
			
			//caculate error rate
			/*for(int i=1; i<= seg; i++){
				int counter = 0;
				for(int j=1; j< Common.roadlist.length; j++){
					if(normal_analyzer.road_traffic[j][i+144] * offline_analyzer.road_traffic[j][i] >0){
						counter++;
						//double error_rate = normal_analyzer.road_traffic[j][i+144] - offline_analyzer.road_traffic[j][i];
						//error_rate /= offline_analyzer.road_traffic[j][i];
						//traffic[0][i-1] += error_rate;
					}
				}
				traffic[0][i-1] += counter;
			}*/
			
			//traffic[0] = analyzer.road_traffic[gid];
			//String[] rowKeys = {"normal","2 times rate","4 times rate"};
			//String[] rowKeys = {"real","offline"};
			String[] rowKeys = {"trunk","primary","secondary","tertiary"};
			String[] columnKeys = new String[seg];
			for(int i=1; i<= seg; i++){
				columnKeys[i-1] = String.valueOf(i);
			}
			Chart chart = new Chart(traffic, rowKeys, columnKeys);
			chart.makeLineAndShapeChart("week_0207.png");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void compareTrafficBySpeed(){
		int gid = 195980;
		//int gid = 203888;
		try {
			//read traffic data from database
			//int seg = 12;//12:00-13:00
			int trunk_id = 104;
			int primary_id = 106;
			int secondary_id = 108;
			int tertiary_id = 109;
			TrafficAnalysis analyzer = new TrafficAnalysis("_2010_02_07", -1);		
			
			int seg = (int)Common.max_seg;
			
			//double[][] traffic = new double[3][(int)Common.max_seg];
			double[][] traffic = new double[1][seg];
			int total_traffic = 0;
			for(int i=1; i<= seg; i++){
				//traffic[0][i-1] = normal_analyzer.average_speed[i+144];
			//	traffic[0][i-1] = normal_analyzer.road_traffic[gid][i+144];
				//traffic[0][i-1] = normal_analyzer.road_traffic[gid][i];
				traffic[0][i-1] = analyzer.average_speed[i];
				//total_traffic += normal_analyzer.traffic_counter[i];
				//Common.logger.debug(total_traffic);
			}
			//Common.logger.debug("total traffic number: " + total_traffic);
			
			String[] rowKeys = {"0207"};
			String[] columnKeys = new String[seg];
			for(int i=1; i<= seg; i++){
				columnKeys[i-1] = String.valueOf(i);
			}
			Chart chart = new Chart(traffic, rowKeys, columnKeys);
			chart.makeLineAndShapeChart("init_week_0207.png");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void compareTrafficByWindowSize(){
		try {
			//read traffic data from database
			
			TrafficAnalysis window4_analyzer = new TrafficAnalysis(window4_Date_Suffix, -1);
			TrafficAnalysis window6_analyzer = new TrafficAnalysis(window6_Date_Suffix, -1);
			TrafficAnalysis window8_analyzer = new TrafficAnalysis(window8_Date_Suffix, -1);
			TrafficAnalysis window10_analyzer = new TrafficAnalysis(window10_Date_Suffix, -1);
			TrafficAnalysis window12_analyzer = new TrafficAnalysis(window12_Date_Suffix, -1);
			
			int seg = (int)Common.max_seg;
			//TrafficAnalysis offline_analyzer = new TrafficAnalysis(seg);
			
			//ArrayList diff_counter[] = new ArrayList[5];
			//ArrayList error_counter[] = new ArrayList[5];
			
			//double[][] traffic = new double[3][(int)Common.max_seg];
			double[][] diff_counter = new double[4][seg];
			double[][] error_rate = new double[4][seg];
			int[] error_counter = new int[4];
			
			int total_traffic = 0;
			for(int i=1; i<= seg; i++){
				//traffic[0][i-1] = normal_analyzer.average_speed[i+144];
				//traffic[0][i-1] = normal_analyzer.road_traffic[gid][i];
				total_traffic += window12_analyzer.traffic_counter[i];
				for(int j=0; j<Common.roadlist.length; j++){
					double speed_12 = window12_analyzer.road_traffic[j][i];//baseline
					double speed_4 = window4_analyzer.road_traffic[j][i];
					double speed_6 = window6_analyzer.road_traffic[j][i];
					double speed_8 = window8_analyzer.road_traffic[j][i];
					double speed_10 = window10_analyzer.road_traffic[j][i];
					
					//window size 4
					//if traffic not empty
					if(speed_4 > 0){
						if(speed_12 > 0){
							double diff_speed = Math.abs(speed_12 - speed_4);
							error_rate[0][i] += diff_speed/speed_12;
							error_counter[0] ++;
						}
						//wrong traffic
						else{
							diff_counter[0][i]++;
						}
					}
					
					//window size 6
					if(speed_6 > 0){
						if(speed_12 > 0){
							double diff_speed = Math.abs(speed_12 - speed_6);
							error_rate[1][i] += diff_speed/speed_12;
							error_counter[1] ++;
						}
						//wrong traffic
						else{
							diff_counter[1][i]++;
						}
					}
					
					//window size 8
					if(speed_8 > 0){
						if(speed_12 > 0){
							double diff_speed = Math.abs(speed_12 - speed_8);
							error_rate[2][i] += diff_speed/speed_12;
							error_counter[2] ++;
						}
						//wrong traffic
						else{
							diff_counter[2][i]++;
						}
					}
					
					//window size 10
					if(speed_10 > 0){
						if(speed_12 > 0){
							double diff_speed = Math.abs(speed_12 - speed_10);
							error_rate[3][i] += diff_speed/speed_12;
							error_counter[3] ++;
						}
						//wrong traffic
						else{
							diff_counter[3][i]++;
						}
					}
				}
				int counter = 0;
				if(error_counter[0] > 0){
					error_rate[0][i] /= error_counter[0];
					if(error_rate[0][i] >= 1){
						counter ++;
						error_rate[0][i] = 1;
					}
					error_counter[0] = 0;
				}
				if(error_counter[1] > 0){
					error_rate[1][i] /= error_counter[1];
					if(error_rate[1][i] >= 1){
						counter ++;
						error_rate[1][i] = 1;
					}
					error_counter[1] = 0;
				}
				if(error_counter[2] > 0){
					error_rate[2][i] /= error_counter[2];
					if(error_rate[2][i] >= 1){
						counter ++;
						error_rate[2][i] = 1;
					}
					error_counter[2] = 0;
				}
				if(error_counter[3] > 0){
					error_rate[3][i] /= error_counter[3];
					if(error_rate[3][i] >= 1){
						counter ++;
						error_rate[3][i] = 1;
					}
					error_counter[3] = 0;
				}
				
				
				//Common.logger.debug(total_traffic);
			}
			
			
			//traffic[0] = analyzer.road_traffic[gid];
			//String[] rowKeys = {"normal","2 times rate","4 times rate"};
			//String[] rowKeys = {"real","offline"};
			String[] rowKeys = {"window_size-4", "window_size-6", "window_size-8", "window_size-10"};
			String[] columnKeys = new String[seg];
			for(int i=1; i<= seg; i++){
				columnKeys[i-1] = String.valueOf(i);
			}
			Chart chart_diff_counter = new Chart(diff_counter, rowKeys, columnKeys);
			chart_diff_counter.makeLineAndShapeChart("diff_counter.png");
			
			Chart chart_error_rate = new Chart(error_rate, rowKeys, columnKeys);
			chart_error_rate.makeLineAndShapeChart("error_rate.png");
			
			Common.logger.debug("total traffic number: " + total_traffic);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void compareTrafficByWindowSize2(){
		try {
			//read traffic data from database
			
			TrafficAnalysis window4_analyzer = new TrafficAnalysis(window4_Date_Suffix, -1);
			TrafficAnalysis window6_analyzer = new TrafficAnalysis(window6_Date_Suffix, -1);
			TrafficAnalysis window8_analyzer = new TrafficAnalysis(window8_Date_Suffix, -1);
			TrafficAnalysis window10_analyzer = new TrafficAnalysis(window10_Date_Suffix, -1);
			TrafficAnalysis window12_analyzer = new TrafficAnalysis(window12_Date_Suffix, -1);
			
			int seg = (int)Common.max_seg;
			//TrafficAnalysis offline_analyzer = new TrafficAnalysis(seg);
			
			//ArrayList diff_counter[] = new ArrayList[5];
			//ArrayList error_counter[] = new ArrayList[5];
			
			//double[][] traffic = new double[3][(int)Common.max_seg];
			double[] diff_counter = new double[4];
			double[] error_rate = new double[4];
			int[] error_counter = new int[5];
			
			int total_traffic = 0;
			
			int[] counter = new int[5];
			
			for(int i=0; i< Common.roadlist.length; i++){
				//traffic[0][i-1] = normal_analyzer.average_speed[i+144];
				//traffic[0][i-1] = normal_analyzer.road_traffic[gid][i];
				double ave_speed_12 = 0;
				double ave_speed_4 = 0;
				double ave_speed_6 = 0;
				double ave_speed_8 = 0;
				double ave_speed_10 = 0;
							
				for(int j=12; j <= seg ; j+=12){
					double speed_12 = 0;//baseline
					double speed_4 = 0;//baseline
					double speed_6 = 0;//baseline
					double speed_8 = 0;//baseline
					double speed_10 = 0;//baseline
					int[] temp_counter = new int[5];
					for(int k= j-11; k<=j; k++){
						if(window12_analyzer.road_traffic[i][k] > 0){
							speed_12 += window12_analyzer.road_traffic[i][k];
							temp_counter[4]++;
						}
						if(window4_analyzer.road_traffic[i][k] > 0){
							speed_4 += window4_analyzer.road_traffic[i][k];
							temp_counter[0]++;
						}
						if(window6_analyzer.road_traffic[i][k] > 0){
							speed_6 += window6_analyzer.road_traffic[i][k];
							temp_counter[1]++;
						}
						if(window8_analyzer.road_traffic[i][k] > 0){
							speed_8 += window8_analyzer.road_traffic[i][k];
							temp_counter[2]++;
						}
						if(window10_analyzer.road_traffic[i][k] > 0){
							speed_10 += window10_analyzer.road_traffic[i][k];
							temp_counter[3]++;
						}
					}
					if(temp_counter[4] > 0){
						speed_12 /= temp_counter[4];
					}
					if(temp_counter[0] > 0){
						speed_4 /= temp_counter[0];
					}
					if(temp_counter[1] > 0){
						speed_6 /= temp_counter[1];
					}
					if(temp_counter[2] > 0){
						speed_8 /= temp_counter[2];
					}
					if(temp_counter[3] > 0){
						speed_10 /= temp_counter[3];
					}
					
					if(speed_12 > 0){
						ave_speed_12 += speed_12;
						error_counter[4]++;
					}
					
					if(speed_4 > 0){
						ave_speed_4 += speed_4;
						error_counter[0]++;
					}
					
					if(speed_6 > 0){
						ave_speed_6 += speed_6;
						error_counter[1]++;
					}
					
					if(speed_8 > 0){
						ave_speed_8 += speed_8;
						error_counter[2]++;
					}
					
					if(speed_10 > 0){
						ave_speed_10 += speed_10;
						error_counter[3]++;
					}
				}
				if(error_counter[4] > 0){
					ave_speed_12 /= error_counter[4];
				}
				if(error_counter[0] > 0){
					ave_speed_4 /= error_counter[0];
				}
				if(error_counter[1] > 0){
					ave_speed_6 /= error_counter[1];
				}
				if(error_counter[2] > 0){
					ave_speed_8 /= error_counter[2];
				}
				if(error_counter[3] > 0){
					ave_speed_10 /= error_counter[3];
				}
				
				
				
				if(ave_speed_4 > 0){
					if(ave_speed_6 > 0){
						double diff_speed = Math.abs(ave_speed_6 - ave_speed_4);
						error_rate[0] += diff_speed/ave_speed_6;
						counter[0]++;
					}
					else{
						diff_counter[0]++;
					}
				}
				
				if(ave_speed_6 > 0){
					if(ave_speed_8 > 0){
						double diff_speed = Math.abs(ave_speed_8 - ave_speed_6);
						error_rate[1] += diff_speed/ave_speed_8;
						counter[1]++;
					}
					else{
						diff_counter[1]++;
					}
				}
				
				if(ave_speed_8 > 0){
					if(ave_speed_10 > 0){
						double diff_speed = Math.abs(ave_speed_10 - ave_speed_8);
						error_rate[2] += diff_speed/ave_speed_10;
						counter[2]++;
					}
					else{
						diff_counter[2]++;
					}
				}
				
				if(ave_speed_10 > 0){
					if(ave_speed_12 > 0){
						double diff_speed = Math.abs(ave_speed_12 - ave_speed_10);
						error_rate[3] += diff_speed/ave_speed_12;
						counter[3]++;
					}
					else{
						diff_counter[3]++;
					}
				}
				
				
				//Common.logger.debug(total_traffic);
			}
			
			if(counter[0] > 0){
				error_rate[0] /= counter[0];
				
			}
			Common.logger.debug(error_rate[0] + "; " + diff_counter[0]);
			
			if(counter[1] > 0){
				error_rate[1] /= counter[1];		
			}
			Common.logger.debug(error_rate[1] + "; " + diff_counter[1]);
			
			if(counter[2] > 0){
				error_rate[2] /= counter[2];
			}
			Common.logger.debug(error_rate[2] + "; " + diff_counter[2]);
			
			if(counter[3] > 0){
				error_rate[3] /= counter[3];
			}
			Common.logger.debug(error_rate[3] + "; " + diff_counter[3]);
			
			//traffic[0] = analyzer.road_traffic[gid];
			//String[] rowKeys = {"normal","2 times rate","4 times rate"};
			//String[] rowKeys = {"real","offline"};
			String[] rowKeys = {"window_size-4", "window_size-6", "window_size-8", "window_size-10"};
			String[] columnKeys = new String[seg];
			for(int i=1; i<= seg; i++){
				columnKeys[i-1] = String.valueOf(i);
			}
			/*Chart chart_diff_counter = new Chart(diff_counter, rowKeys, columnKeys);
			chart_diff_counter.makeLineAndShapeChart("diff_counter.png");
			
			Chart chart_error_rate = new Chart(error_rate, rowKeys, columnKeys);
			chart_error_rate.makeLineAndShapeChart("error_rate.png");
			
			Common.logger.debug("total traffic number: " + total_traffic);*/
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void compareWeekTraffic(){
		try {
			//read traffic data from database
			//int seg = 12;//12:00-13:00
			/*TrafficAnalysis week_0201_analyzer = new TrafficAnalysis(week0201_Date_Suffix);
			TrafficAnalysis week_0202_analyzer = new TrafficAnalysis(week0202_Date_Suffix);
			TrafficAnalysis week_0203_analyzer = new TrafficAnalysis(week0203_Date_Suffix);
			TrafficAnalysis week_0204_analyzer = new TrafficAnalysis(week0204_Date_Suffix);*/
			TrafficAnalysis week_0205_analyzer = new TrafficAnalysis(week0205_Date_Suffix, -1);
			TrafficAnalysis week_0207_analyzer = new TrafficAnalysis(week0207_Date_Suffix, -1);
			TrafficAnalysis week_0208_analyzer = new TrafficAnalysis(week0208_Date_Suffix, -1);
			
			
			int seg = (int)Common.max_seg;
			//TrafficAnalysis offline_analyzer = new TrafficAnalysis(seg);
			
			//double[][] traffic = new double[3][(int)Common.max_seg];
			double[][] traffic = new double[3][seg];
			int total_traffic = 0;
			for(int i=1; i<= seg; i++){
				//traffic[0][i-1] = normal_analyzer.average_speed[i+144];
			//	traffic[0][i-1] = normal_analyzer.road_traffic[gid][i+144];
				//traffic[0][i-1] = normal_analyzer.road_traffic[gid][i];
				/*traffic[0][i-1] = week_0201_analyzer.average_speed[i];
				traffic[1][i-1] = week_0202_analyzer.average_speed[i];
				traffic[2][i-1] = week_0203_analyzer.average_speed[i];
				traffic[3][i-1] = week_0204_analyzer.average_speed[i];*/
				traffic[0][i-1] = week_0205_analyzer.average_speed[i];
				traffic[1][i-1] = week_0207_analyzer.average_speed[i];
				traffic[2][i-1] = week_0208_analyzer.average_speed[i];
				total_traffic += week_0207_analyzer.traffic_counter[i];
				//Common.logger.debug(total_traffic);
			}
			Common.logger.debug("total traffic number: " + total_traffic);
			
			//traffic[0] = analyzer.road_traffic[gid];
			String[] rowKeys = {"0205", "0207", "0208"};
			String[] columnKeys = new String[seg];
			for(int i=1; i<= seg; i++){
				columnKeys[i-1] = String.valueOf(i);
			}
			Chart chart = new Chart(traffic, rowKeys, columnKeys);
			chart.makeLineAndShapeChart("comparison.png");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void makeLineAndShapeChart(String file_name){  
	        //CategoryDataset dataset = getBarData(data, rowKeys, columnKeys);  
	        CategoryDataset dataset = DatasetUtilities.createCategoryDataset(rowKeys, columnKeys, data);
	        createTimeXYChar("traffic-0207", 
	        		"x轴", "y轴", dataset, file_name); 
	}
	
	public String createTimeXYChar(String chartTitle, String x, String y,  
            CategoryDataset xyDataset, String charName) {  
  
        JFreeChart chart = ChartFactory.createLineChart(chartTitle, x, y,  
                xyDataset, PlotOrientation.VERTICAL, true, true, false);  
  
        chart.setTextAntiAlias(false);  
        chart.setBackgroundPaint(Color.WHITE);  
        // 设置图标题的字体重新设置title  
        Font font = new Font("隶书", Font.BOLD, 30);  
        TextTitle title = new TextTitle(chartTitle);  
        title.setFont(font);  
        chart.setTitle(title);  
        
        //关闭抗锯齿，使文字更清晰
        chart.getRenderingHints().put(RenderingHints.KEY_TEXT_ANTIALIASING,RenderingHints.VALUE_TEXT_ANTIALIAS_OFF);
        
        // 设置面板字体  
        Font labelFont = new Font("隶书", Font.PLAIN, 12);  
  
        chart.setBackgroundPaint(Color.WHITE);  
  
        CategoryPlot categoryplot = (CategoryPlot) chart.getPlot();  
        // x轴 // 分类轴网格是否可见  
        categoryplot.setDomainGridlinesVisible(true);  
        // y轴 //数据轴网格是否可见  
        categoryplot.setRangeGridlinesVisible(true);  
  
        categoryplot.setRangeGridlinePaint(Color.WHITE);// 虚线色彩  
  
        categoryplot.setDomainGridlinePaint(Color.WHITE);// 虚线色彩  
  
        categoryplot.setBackgroundPaint(Color.lightGray);  
  
        // 设置轴和面板之间的距离  
        // categoryplot.setAxisOffset(new RectangleInsets(5D, 5D, 5D, 5D));  
  
        CategoryAxis domainAxis = categoryplot.getDomainAxis();  
        
  
        domainAxis.setLabelFont(labelFont);// 轴标题  
  
        domainAxis.setTickLabelFont(labelFont);// 轴数值  
  
        domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45); // 横轴上的  
        

        // Lable  
        // 45度倾斜  
        // 设置距离图片左端距离  
  
        domainAxis.setLowerMargin(0.0);  
        // 设置距离图片右端距离  
        domainAxis.setUpperMargin(0.0); 

  
        NumberAxis numberaxis = (NumberAxis) categoryplot.getRangeAxis();  
        numberaxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());  
        
        numberaxis.setAutoRangeIncludesZero(true);  
        

        for(int i = 0; i<Common.max_seg; i++)
        {
        	if(i%20 ==0)
        	{
        		domainAxis.setTickLabelPaint(Integer.toString(i), Color.black);
        	}
        	else{
        		domainAxis.setTickLabelPaint(Integer.toString(i), Color.white);
        	}
        }
  
        // 获得renderer 注意这里是下嗍造型到lineandshaperenderer！！  
        LineAndShapeRenderer lineandshaperenderer = (LineAndShapeRenderer) categoryplot.getRenderer();  
  
        lineandshaperenderer.setBaseShapesVisible(true); // series 点（即数据点）可见  
  
        lineandshaperenderer.setBaseLinesVisible(true); // series 点（即数据点）间有连线可见  
  

        
        
        
        // 显示折点数据  
        // lineandshaperenderer.setBaseItemLabelGenerator(new  
        // StandardCategoryItemLabelGenerator());  
        // lineandshaperenderer.setBaseItemLabelsVisible(true);  
  
        FileOutputStream fos_jpg = null;  
        try {  
            //isChartPathExist(CHART_PATH);  
            String chartName = CHART_PATH + charName;  
            fos_jpg = new FileOutputStream(chartName);  
  
            // 将报表保存为png文件  
            ChartUtilities.writeChartAsPNG(fos_jpg, chart, 1000, 1000);  
  
            return chartName;  
        } catch (Exception e) {  
            e.printStackTrace();  
            return null;  
        } finally {  
            try {  
                fos_jpg.close();  
                System.out.println("create time-createTimeXYChar.");  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
    }  
	
	public static void showHistoryTraffic() throws SQLException{
		int gid = 2;
		try {
			//read traffic data from database
			TrafficAnalysis history_analyzer = new TrafficAnalysis("", -1);		
			int seg = (int)Common.max_seg;
			//TrafficAnalysis offline_analyzer = new TrafficAnalysis(seg);
			
			//double[][] traffic = new double[3][(int)Common.max_seg];
			double[][] traffic = new double[1][seg];
			int total_traffic = 0;
			for(int i=1; i<= seg; i++){
				//traffic[0][i-1] = history_analyzer.road_traffic[gid][i];
				traffic[0][i-1] = history_analyzer.average_speed[i];
				//total_traffic += history_analyzer.traffic_counter[i];
				//Common.logger.debug(total_traffic);
			}
			//Common.logger.debug("total traffic number: " + total_traffic);
			
			String[] rowKeys = {"history traffic"};
			String[] columnKeys = new String[seg];
			for(int i=1; i<= seg; i++){
				columnKeys[i-1] = String.valueOf(i);
			}
			Chart chart = new Chart(traffic, rowKeys, columnKeys);
			chart.makeLineAndShapeChart("history_traffic.png");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
