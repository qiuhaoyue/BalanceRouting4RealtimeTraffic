import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.TimeZone;
import java.lang.Math;
 

public class Test {
	
	public static void main(String[] args){
		
		//20090106 1:00pm-2:00pm 1231218000-1231221600  522743() samples
		//20090106 5:00-6:00pm 1231232400-1231236000 622989() samples
		
		//Inside 2nd Ring Road, 116.3399 --- 116.4422  39.9505--39.8648
		
		long utc=((long)1231236000)*1000;
		System.out.println("TIME second:"+ utc);
		Date date=new Date(utc);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		TimeZone zone=TimeZone.getTimeZone("GMT+8");
		format.setTimeZone(zone);
		System.out.println("TIME:"+format.format(date));
		
		String s="12,14,";
		String[] route_gids=s.split(",");
		for(int i=0;i<route_gids.length;i++){
			System.out.println("|"+route_gids[i]+"|");
		}
		System.out.println("|finished!|");
		
		//select count(*) from table where utc>=1231218000 and utc<1231221600 and lat>11633990 and lat<11644220 and lon>3986480 and lon<=3995050
		//select count(*) from valid_gps where lat>3986480 and lat<=3995050 and lon>11633990 and lon<=11644220 and utc>=1231232400 and utc<1231236000
		
		/*
		create table labeled_gps as select * from gps_part_1;
		insert into labeled_gps select * from gps_part_2;
		insert into labeled_gps select * from gps_part_3;
		insert into labeled_gps select * from gps_part_4;
		insert into labeled_gps select * from gps_part_5;
		insert into labeled_gps select * from gps_part_6;
		insert into labeled_gps select * from gps_part_7;
		insert into labeled_gps select * from gps_part_8;
		insert into labeled_gps select * from gps_part_9;
		insert into labeled_gps select * from gps_part_10;
		insert into labeled_gps select * from gps_part_11;*/
		
		//select count(*), suid from (select * from labeled_gps where (stop is null or stop!=1) and lat>3986480 and lat<=3995050 and lon>11633990 and lon<=11644220 and utc>=1231232400 and utc<1231236000) as result group by suid order by suid
		
		
		//select count(*) from labeled_gps where (stop is null or stop!=1) and lat>3986480 and lat<=3995050 and lon>11633990 and lon<=11644220 and utc>=1231232400 and utc<1231236000
		
		//in total:137257 samples
		
		//select count(*), suid from (select * from labeled_gps where (stop is null or stop!=1) and lat>3986480 and lat<=3995050 and lon>11633990 and lon<=11644220 and utc>=1231218000 and utc<1231221600) as result group by suid order by suid
		//select count(*) from labeled_gps where (stop is null or stop!=1) and lat>3986480 and lat<=3995050 and lon>11633990 and lon<=11644220 and utc>=1231218000 and utc<1231221600
		// in total: 113483 samples

	}
}
