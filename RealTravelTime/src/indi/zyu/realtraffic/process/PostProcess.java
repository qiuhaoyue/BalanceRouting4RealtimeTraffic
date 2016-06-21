package indi.zyu.realtraffic.process;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;

import java.util.ArrayList;

/** 
 * 2016Äê4ÔÂ28ÈÕ 
 * PostProcess.java 
 * author:ZhangYu
 */

public class PostProcess {
	
	public static void postprocess_intervals(ArrayList<Sample> trajectory){
			
		Sample cur_sample=null;
		Sample pre_sample=null;
		for(int j=0;j<trajectory.size(); j++){
			try{
				pre_sample=cur_sample;
				cur_sample=trajectory.get(j);
					
				if(pre_sample==null || cur_sample.stop==1 || pre_sample.stop==1){
					continue;
				}
				else{
					long cur_utc=cur_sample.utc.getTime()/1000;
					cur_sample.interval=cur_utc-pre_sample.utc.getTime()/1000;
					if(cur_sample.interval<=0){
						continue;
					}
					cur_sample.pre_gid = pre_sample.gid;
					cur_sample.pre_offset = pre_sample.offset;
					trajectory.set(j, cur_sample);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}	
		Common.logger.debug("postprocess_interval finished!");
	}
}
