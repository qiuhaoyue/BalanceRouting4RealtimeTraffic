
public class MatchEntry {
	
	//location of the candidate
	public RoadSegment road;
	public double offset;
	double lat;
	double lon;
	
	//local matching distance and weight
	public double matching_weight;
	public double matching_distance;
	
	//transit distance and weight
	double transit_weight;
	double transit_distance;
	double circle_distance;
	
	int pre_entry_index;
	double final_weight=-1.0;
	
	String path="";
	
	MatchEntry(RoadSegment can_road, double percent, double lon, double lat, double local_distance){
		this.road=can_road;
		this.offset=percent;
		this.lat=lat;
		this.lon=lon;
		
		this.matching_distance=local_distance;
		this.matching_weight=1.0;
		
		this.circle_distance=-1.0;
		this.transit_distance=-1.0;
		this.transit_weight=1.0;
		
		this.pre_entry_index=-1;
		this.final_weight=1.0;
		
		this.path="";
	}
	
	MatchEntry(RoadSegment can_road, double local_distance){
		this.road=can_road;
		this.matching_distance=local_distance;
		this.matching_weight=1.0;
		
		this.offset=0;
		this.lat=0;
		this.lon=0;
		
		this.circle_distance=-1.0;
		this.transit_distance=-1.0;
		this.transit_weight=1.0;
		
		this.pre_entry_index=-1;
		this.final_weight=1.0;
		
		this.path="";
	}
	
	
	public String toString(){
		String output="MATCH_ENTRY: "+this.road.toString();	
		output+=" offset(" + offset + ")	pos(" + lon + "," + lat + ")	matching_distance(" + matching_distance 
				+ ")	circle_distance(" + circle_distance  + ")	transit_distance(" + transit_distance+")";		
		return output;
	}
	
}
