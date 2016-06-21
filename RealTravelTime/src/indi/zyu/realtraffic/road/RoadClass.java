/** 
 * 2016Äê3ÔÂ12ÈÕ 
 * RoadClass.java 
 * author:ZhangYu
 */
package indi.zyu.realtraffic.road;

public class RoadClass {
	public double avg_speed;
	public double dev_avg;
	public double max_speed;
	public double dev_max;
	public long count;
	
	public RoadClass(double avg_speed, double dev_avg, double avg_max, double dev_max, long count){
		this.avg_speed=avg_speed;
		this.dev_avg=dev_avg;
		this.max_speed=avg_max;
		this.dev_max=dev_max;
		this.count=count;
	}
}
