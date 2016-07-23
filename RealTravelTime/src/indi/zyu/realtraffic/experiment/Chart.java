/** 
* 2016年7月2日 
* Chart.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.experiment;

import indi.zyu.realtraffic.common.Common;

import java.awt.Color;
import java.awt.Font;
import java.io.FileOutputStream;
import java.sql.SQLException;

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
	private static String normal_Date_Suffix = "_2016_07_01";
	private static String speed2_Date_Suffix = "_2016_07_03";
	private static String speed4_Date_Suffix = "_2016_07_04";
	private static String normal_Date_Suffix2 = "_2016_07_17";
	private static String speed2_Date_Suffix2 = "_2016_07_18";
	private static String speed4_Date_Suffix2 = "_2016_07_19";
	private double[][] data = null;
	private String[] rowKeys = null;
	private String[] columnKeys = null;
	
	public Chart(double[][] data, String[] rowKeys, String[] columnKeys){
		this.data = data;
		this.rowKeys = rowKeys;
		this.columnKeys = columnKeys;
	}
	
	public static void main(String[] args) throws SQLException {  
 
		//Common.clear_travel_table("_2016_06_22");
		Common.init(40000);
		Common.init_roadlist();//initialize roadlist
		
		int gid = 195980;
		//int gid = 203888;
		try {
			//read traffic data from database
			//int seg = 12;//12:00-13:00
			TrafficAnalysis normal_analyzer = new TrafficAnalysis(normal_Date_Suffix2);
			TrafficAnalysis speed2_analyzer = new TrafficAnalysis(speed2_Date_Suffix2);
			TrafficAnalysis speed4_analyzer = new TrafficAnalysis(speed4_Date_Suffix2);
			//TrafficAnalysis offline_analyzer = new TrafficAnalysis(seg);
			
			int seg = (int)Common.max_seg;
			//TrafficAnalysis offline_analyzer = new TrafficAnalysis(seg);
			
			//double[][] traffic = new double[3][(int)Common.max_seg];
			double[][] traffic = new double[3][seg];
			int total_traffic = 0;
			for(int i=1; i<= seg; i++){
				//traffic[0][i-1] = normal_analyzer.average_speed[i+144];
			//	traffic[0][i-1] = normal_analyzer.road_traffic[gid][i+144];
				//traffic[0][i-1] = normal_analyzer.road_traffic[gid][i];
				traffic[0][i-1] = normal_analyzer.average_speed[i];
				traffic[1][i-1] = speed2_analyzer.average_speed[i];
				traffic[2][i-1] = speed4_analyzer.average_speed[i];
				//total_traffic += speed4_analyzer.traffic_counter[i];
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
			String[] rowKeys = {"normal", "2speed", "4speed"};
			String[] columnKeys = new String[seg];
			for(int i=1; i<= seg; i++){
				columnKeys[i-1] = String.valueOf(i);
			}
			Chart chart = new Chart(traffic, rowKeys, columnKeys);
			chart.makeLineAndShapeChart();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
	public void makeLineAndShapeChart(){  
	        //CategoryDataset dataset = getBarData(data, rowKeys, columnKeys);  
	        CategoryDataset dataset = DatasetUtilities.createCategoryDataset(rowKeys, columnKeys, data);
	        createTimeXYChar("comparison of average speed", "x轴", "y轴", dataset, "offline_normal.png"); 
	}
	
	public String createTimeXYChar(String chartTitle, String x, String y,  
            CategoryDataset xyDataset, String charName) {  
  
        JFreeChart chart = ChartFactory.createLineChart(chartTitle, x, y,  
                xyDataset, PlotOrientation.VERTICAL, true, true, false);  
  
        chart.setTextAntiAlias(false);  
        chart.setBackgroundPaint(Color.WHITE);  
        // 设置图标题的字体重新设置title  
        Font font = new Font("隶书", Font.BOLD, 25);  
        TextTitle title = new TextTitle(chartTitle);  
        title.setFont(font);  
        chart.setTitle(title);  
        // 设置面板字体  
        Font labelFont = new Font("SansSerif", Font.TRUETYPE_FONT, 10);  
  
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
        	if(i%10 ==0)
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
}
