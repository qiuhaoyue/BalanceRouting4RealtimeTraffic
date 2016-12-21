/** 
 * 2016Äê3ÔÂ11ÈÕ 
 * Sample.java 
 * author:ZhangYu
 */

package indi.zyu.realtraffic.gps;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Sample implements Comparable<Sample>{
	public long suid;
	public Date utc;
	public double lat;
	public double lon;
	public int head;
	public double speed;
	public long distance;
	public double min_matching_distance;
	public int stop; //0 stands for movement, 1 stands for long stop, 2 stands for temporary stop;
	public double moving_distance;
	
	public String date;
	
	public int gid;
	public double offset;
	public String route;
	public long interval;
	public int pre_gid;
	public double pre_offset;
	
	public int passenager; //0 has no passenger and 1 has passenager
	
	public Sample(String date, long suid, long utc, long lat, long lon, int head){
		this.date = date;
		this.suid=suid;
		this.utc=new Date(utc*1000L);
		this.lat=lat/100000.0;
		this.lon=lon/100000.0;
		this.head=head;
		this.min_matching_distance=-1.0;
		this.stop=0;
		this.moving_distance=-1;
	}
	
	public Sample(long suid, long utc, double lat, double lon, int head){
		this.suid=suid;
		this.utc=new Date(utc*1000L);
		this.lat=lat;
		this.lon=lon;
		this.head=head;
		this.min_matching_distance=-1.0;
		this.stop=0;
		this.moving_distance=-1;
	}
	
	public Sample(String date, long suid, long utc, int gid, double offset, String route, int stop, long interval){
		this.suid=suid;
		this.utc=new Date(utc*1000L);
		this.gid=gid;
		this.offset=offset;
		this.route=route;
		this.stop=stop;
		this.interval=interval;
	}
	
	Sample(String date, long suid, long utc, int gid, double offset, String route, int stop){
		this.suid=suid;
		this.utc=new Date(utc*1000L);
		this.gid=gid;
		this.offset=offset;
		this.route=route;
		this.stop=stop;
	}
	
	public String toString(){
		String output="("+suid+",";	
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		TimeZone zone=TimeZone.getTimeZone("GMT+8");
		format.setTimeZone(zone);
		output+=format.format(this.utc)+")	";		
		output+="lat:" + lat + ",lon:" + lon;		
		return output;
	}
	public String getAttributeForInsert(){
		String sql = " (" + suid + ", " + utc.getTime()/1000 + ", " + lat + ", " + lon + ", " + head + ", " 
						+ stop + ", " + gid + ", "  + offset + ", " + "'" + route + "'" + ", " + interval + 
						", " + pre_gid + ", " + pre_offset + ") ";
		return sql;
	}
	public String getSimpleAttributeForInsert(){
		String sql = suid + ", " + utc.getTime()/1000 + ", " + lat + ", " + lon + ", " + head;
		return sql;
	}
	public int compareTo(Sample a){
		return this.utc.compareTo(a.utc);
	}
}
