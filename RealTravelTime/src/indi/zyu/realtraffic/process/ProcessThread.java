package indi.zyu.realtraffic.process;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.bmwcarit.barefoot.markov.KState;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.matcher.MatcherTransition;
import com.bmwcarit.barefoot.roadmap.Route;
import com.esri.core.geometry.Point;

/** 
 * 2016Äê4ÔÂ16ÈÕ 
 * ProcessThread.java 
 * author:ZhangYu
 */

public class ProcessThread implements Runnable {

	//ArrayList<Sample> gps_seq = null;
	int suid;
	Sample gps;
	//KState<MatcherCandidate, MatcherTransition, MatcherSample> state;
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//System.out.println(gps_seq.size());
		//Collections.sort(gps_seq);
		/*RemoveStopPoint.label_stop(gps_seq);	
		List<MatcherCandidate> sequence = this.state.sequence();
		if(sequence == null){
			//Common.logger.debug("sequence null: " + trajectory.toString());
			return;
		}
		int seq_size = gps_seq.size() - 1;
		for(int i=sequence.size()-1 ; seq_size >=0 ; i--,seq_size-- ){
			//Common.logger.debug("real gid: " + gps_seq.get(i).gid);
			MatcherCandidate estimate = sequence.get(i);
			if(estimate.point() == null){
		    	continue;
		    }
		    int id = (int)estimate.point().edge().id(); // road id
		    Point position = estimate.point().geometry(); // position
		    String str = "";
		    str += "match: " + position.getX() + ";" + position.getY();
		    str += "sample: " + gps_seq.get(seq_size).lon + ";" + gps_seq.get(seq_size).lat;
		    //Common.logger.debug("match: " + position.getX() + ";" + position.getY());
		    //Common.logger.debug("sample: " + gps_seq.get(i).lon + ";" + gps_seq.get(i).lat);
		    Common.logger.debug(str);
		    double offset = estimate.point().fraction(); // offset
		    //Sample sample = gps_seq.get(i);
		    Sample sample = gps_seq.get(seq_size);

		    sample.gid = id;
		    sample.offset = offset;
		    if(estimate.transition() != null ){
		    	Route route = estimate.transition().route(); // route to position
		    	sample.route = route.toString();
		    	//Common.logger.debug("id: " + id + ", position: " + position + "offset: " 
		    	//		+ offset + ", route: " + route.toString());
		    }
		    //gps_seq.set(i, sample);
		    gps_seq.set(seq_size, sample);
		    //Common.logger.debug("trajectory gid: " + gps_seq.get(i).gid);
		    //Common.logger.debug("id: " + id + ", position: " + position+ "offset: " + offset);
		}
		
		PostProcess.postprocess_intervals(gps_seq);
		
		//insert gps, not necessary, for experiment
		
		if(Common.gps_updater.addGPS(gps_seq)){
			Common.logger.debug("insert gps successfully");
		}
		else{
			Common.logger.debug("insert gps failed");
		}
		
		//sense traffic slice
		ArrayList<String> updater = TravelTimeSlice.allocate_time(Common.roadlist, gps_seq, Common.traffic_slice_table);
	    
		//insert traffic sensed by gps_seq
		if(Common.traffic_updater.addTraffic(updater)){
			Common.logger.debug("insert traffic slice successfully. suid: " + suid);
		}
		else{
			Common.logger.debug("insert traffic slice failed. suid: " + suid);
		}*/
		Common.taxi[suid].add_gps(gps);
		
		//Common.logger.debug(traffic_updater.toString());
	}
	
	public ProcessThread(int suid, Sample gps){
		this.suid = suid;
		//this.gps_seq = new ArrayList<Sample>();
		//this.gps_seq.addAll(0, gps_seq);
		//this.state = state;
		this.gps = gps;
		
	}
}
