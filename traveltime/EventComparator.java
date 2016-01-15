import java.util.Comparator;


public class EventComparator implements Comparator<Event>{
	public int compare(Event a, Event b){
		if(a.utc<b.utc){
			return -1;
		}
		else if(a.utc>b.utc){
			return 1;
		}
		else{
			if(a.type<=b.type){
				return -1;
			}
			else{
				return 1;
			}
		}
	}
}
