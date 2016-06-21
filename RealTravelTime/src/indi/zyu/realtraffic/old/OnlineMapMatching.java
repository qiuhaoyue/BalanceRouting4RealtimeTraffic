package indi.zyu.realtraffic.old;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bmwcarit.barefoot.matcher.Matcher;
import com.bmwcarit.barefoot.markov.KState;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.matcher.MatcherTransition;
import com.bmwcarit.barefoot.road.PostGISReader;
import com.bmwcarit.barefoot.roadmap.Road;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.bmwcarit.barefoot.roadmap.RoadPoint;
import com.bmwcarit.barefoot.roadmap.Route;
import com.bmwcarit.barefoot.roadmap.TimePriority;
import com.bmwcarit.barefoot.spatial.Geography;
import com.bmwcarit.barefoot.topology.Dijkstra;
import com.esri.core.geometry.Point;
/** 
 * 2016Äê4ÔÂ17ÈÕ 
 * OnlineMapMatching.java 
 * author:ZhangYu
 */

public class OnlineMapMatching {
	
	//match a entire sequence of gps point to road
	public  boolean match_seq( ArrayList<Sample> trajectory){
	
		// Instantiate matcher and state data structure
		Matcher matcher = new Matcher(Common.map, new Dijkstra<Road, RoadPoint>(),
		            new TimePriority(), new Geography());
		KState<MatcherCandidate, MatcherTransition, MatcherSample> state
			= new KState<MatcherCandidate, MatcherTransition, MatcherSample>();

		// Input as sample batch (offline) or sample stream (online)
		//List<MatcherSample> samples = new LinkedList<MatcherSample>();
		// Iterative map matching of sample batch (offline) or sample stream (online)
		for (Sample sample : trajectory) {
			MatcherSample matcher_sample = new MatcherSample(String.valueOf(sample.suid), 
					sample.utc.getTime()/1000, new Point(sample.lon, sample.lat));
		    Set<MatcherCandidate> vector = matcher.execute(state.vector(), state.sample(),
		    		matcher_sample);
		    state.update(vector, matcher_sample);

		    // Online map matching result
		    /*MatcherCandidate estimate = state.estimate(); // most likely position estimate
		    if(estimate.point() == null){
		    	continue;
		    }
		    int id = (int)estimate.point().edge().id(); // road id
		    Point position = estimate.point().geometry(); // position
		    double offset = estimate.point().fraction(); // offset
		    sample.gid = id;
		    sample.offset = offset;
		    if(estimate.transition() != null ){
		    	Route route = estimate.transition().route(); // route to position
		    	sample.route = route.toString();
		    	Common.logger.debug("id: " + id + ", position: " + position + "offset: " 
		    			+ offset + ", route: " + route.toString());
		    	continue;
		    }
		    Common.logger.debug("id: " + id + ", position: " + position+ "offset: " + offset);*/
		}

		// Offline map matching results
		try{
			List<MatcherCandidate> sequence = state.sequence(); // most likely sequence of positions
			if(sequence == null){
				//Common.logger.debug("sequence null: " + trajectory.toString());
				return false;
				
			}
			//System.out.println(sequence.size() + "; " + trajectory.size());
			for(int i=0; i<sequence.size(); i++ ){
				MatcherCandidate estimate = sequence.get(i);
				if(estimate.point() == null){
			    	continue;
			    }
			    int id = (int)estimate.point().edge().id(); // road id
			    Point position = estimate.point().geometry(); // position
			    double offset = estimate.point().fraction(); // offset
			    Sample sample = trajectory.get(i);
			    sample.gid = id;
			    sample.offset = offset;
			    if(estimate.transition() != null ){
			    	Route route = estimate.transition().route(); // route to position
			    	sample.route = route.toString();
			    	//Common.logger.debug("id: " + id + ", position: " + position + "offset: " 
			    	//		+ offset + ", route: " + route.toString());
			    }
			    trajectory.set(i, sample);
			    //Common.logger.debug("id: " + id + ", position: " + position+ "offset: " + offset);
			}
		}
		catch(Exception e){
			 e.printStackTrace();
			 Common.logger.debug("sequence null: " + trajectory.toString());
			 //store unmatched gps
			 if(Common.unkown_gps_updater.addGPS(trajectory)){
				 Common.logger.debug("insert unknown gps successfully");
			 }
			 else{
				 Common.logger.debug("insert unknown gps failed");
			 }
			 return false;
		}
		
		return true;
	}
	
	public  boolean match_single_gps( ArrayList<Sample> trajectory){
		// Instantiate matcher and state data structure
		Matcher matcher = new Matcher(Common.map, new Dijkstra<Road, RoadPoint>(),
				    new TimePriority(), new Geography());
		KState<MatcherCandidate, MatcherTransition, MatcherSample> state
			= new KState<MatcherCandidate, MatcherTransition, MatcherSample>();
		return true;
	}
}
