import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Sample {

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
	
	public static final int MOVING=0;
	public static final int LONG_STOP=1;
	public static final int TEM_STOP=2;
	
	public int gid;
	public double offset;
	public String route;
	public long interval;
	public int pre_gid;
	public double pre_offset;
	
	public int passenager; //0 has no passenger and 1 has passenager
	
	Sample(long suid, long utc, long lat, long lon, int head, long speed, long distance){
		this.suid=suid;
		this.utc=new Date(utc*1000L);
		this.lat=lat/100000.0;
		this.lon=lon/100000.0;
		this.head=head;
		this.speed=speed/100.0;
		this.distance=distance;
		this.min_matching_distance=-1.0;
		this.stop=0;
		this.moving_distance=-1;
	}
	
	Sample(long suid, long utc, int gid, double offset, String route, int stop, long interval){
		this.suid=suid;
		this.utc=new Date(utc*1000L);
		this.gid=gid;
		this.offset=offset;
		this.route=route;
		this.stop=stop;
		this.interval=interval;
	}
	
	Sample(long suid, long utc, int gid, double offset, String route, int stop){
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
		output+="lat:" + lat + ",lon:" + lon + ",head:" + head + ",speed:" + speed + ",distance:" + distance+ ",min_matching_distance:" + min_matching_distance+"";		
		return output;
	}
	
}
