package indi.zyu.realtraffic.traffic;

import indi.zyu.realtraffic.common.Common;
import indi.zyu.realtraffic.gps.Sample;
import indi.zyu.realtraffic.updater.RoadCostUpdater;

import java.util.ArrayList;


/** 
 * 2016Äê5ÔÂ5ÈÕ 
 * TravelTimeSlice.java 
 * author:ZhangYu
 */

public class TravelTimeSlice {
	
	//calculate traffic by route between points and return update sql
	public static ArrayList<String> allocate_time(/*AllocationRoadsegment[] roadlist, */ArrayList<Sample> samplelist, String table_prefix){
			
		Sample cur_sample=null; //samplelist.get(0);
		//Sample pre_sample=null;
		//long seq=0;
		long pre_suid=-1;
		long pre_interval=-1;
		
		long max_seg=(Common.end_utc-Common.start_utc)/Common.period;//96
		long  pre_utc = Common.start_utc;
		int  seq_num;
				
		ArrayList<String> updates=new ArrayList<String>();
		for(int j=0;j<samplelist.size(); j++){
				
			cur_sample=samplelist.get(j);
				
			//if(cur_sample.stop==1 || (cur_sample.stop==2 && cur_sample.suid!=pre_suid)){
			if(cur_sample.stop == 1){
				continue;
			}
			else{
				long cur_utc=cur_sample.utc.getTime()/1000;
				long interval = cur_sample.interval;
				int pre_gid=cur_sample.pre_gid;
				double pre_offset=cur_sample.pre_offset;
				int origin_gid=0;
				boolean changed_gid=false;
				//update no need to do time_allocation
				if( cur_sample.stop==2 || (cur_sample.gid== pre_gid && (cur_sample.interval<=10 || Math.abs(cur_sample.pre_offset-cur_sample.offset)<=0.1))){
					if(cur_sample.stop==2 && cur_sample.gid!= pre_gid ){
						//special handling for duplicated record;
						if((j+1<samplelist.size()) && cur_sample.suid==samplelist.get(j+1).suid && cur_sample.utc.getTime()==samplelist.get(j+1).utc.getTime()){
							j++;
							System.out.println("Duplicated["+cur_sample.suid+","+cur_utc+"]: temperary stop happens among multiple road segments!");
							continue;
						}
						Common.logger.debug("Exception["+cur_sample.suid+","+cur_utc+"]: temperary stop happens among multiple road segments!");
								
						origin_gid=cur_sample.gid;
						cur_sample.gid=pre_gid;
						cur_sample.offset=pre_offset;
						changed_gid=true;
						/*
						if(pre_gid<roadlist.length && roadlist[pre_gid].to_cost>=0 && roadlist[pre_gid].to_cost < RoadCostUpdater.inconnectivity-1){
							cur_sample.offset=1.0;
						}
						else if(pre_gid<roadlist.length && roadlist[pre_gid].reverse_cost>=0 && roadlist[pre_gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ // negative road
							cur_sample.offset=0.0;
						}
						else{
							//alien_road=true;
							continue;
						}*/
								
						if((j+1<samplelist.size()) && samplelist.get(j+1).suid==cur_sample.suid){
							samplelist.get(j+1).pre_gid=cur_sample.gid;
							samplelist.get(j+1).pre_offset=cur_sample.offset;
						}
					}
							
					double added_coverage=0;
					if(pre_gid<Common.roadlist.length && Common.roadlist[pre_gid].to_cost>=0 && Common.roadlist[pre_gid].to_cost < RoadCostUpdater.inconnectivity-1){ //positive road
						if(cur_sample.stop==2 && cur_sample.offset<pre_offset){
							cur_sample.offset=pre_offset;
						}
						added_coverage=cur_sample.offset-pre_offset;
					}
					else if(pre_gid<Common.roadlist.length && Common.roadlist[pre_gid].reverse_cost>=0 && Common.roadlist[pre_gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ // negative road
						if(cur_sample.stop==2 && cur_sample.offset>pre_offset){
							cur_sample.offset=pre_offset;
						}
						added_coverage=pre_offset-cur_sample.offset;
					}
					else{
						//alien_road=true;
						continue;
					}
							
					if(added_coverage>=0){

						//generate update sql		
						if(interval<pre_interval){
							interval=pre_interval;
						}
						String sql="";
						String result_table = table_prefix;
						seq_num=(int)(max_seg-(Common.end_utc-pre_utc)/Common.period);
						result_table = result_table + seq_num + Common.Date_Suffix;
						if(changed_gid){
							seq_num=(int)(max_seg-(Common.end_utc-pre_utc)/Common.period);
							sql="UPDATE "+ result_table + " SET time=time+" + cur_sample.interval + ", percent=percent+"+ added_coverage + ", next_gid="+ origin_gid
							     + ", interval="+ interval +" WHERE gid="+ cur_sample.gid + " and utc=" + pre_utc + ";";
						}
						else{
						    sql="UPDATE "+ result_table + " SET time=time+" + cur_sample.interval + ", percent=percent+"+ added_coverage
							    	+ ", interval="+ interval +" WHERE gid="+ cur_sample.gid + " and utc=" + pre_utc + ";";
						}
						//System.out.println(sql);
						//updates.add(sql);//cannot update by batch, should do it separately
						continue;
					}
				}
						
				//insert new record to result_table
				//else{
					if(cur_sample.route==null || cur_sample.route.equals("")){
						continue;
					}
					/*if(cur_sample.utc.getTime()/1000==1231233148L && cur_sample.suid==4231){
						System.out.println();
					}
					if(cur_sample.suid==2476 && cur_sample.utc.getTime()/1000==1231233039){
						System.out.println("here");
					}*/
					String[] route_gids=cur_sample.route.split(",");
					ArrayList<Integer> gidlist=new ArrayList<Integer>();
					ArrayList<Double> timelist=new ArrayList<Double>();
					ArrayList<Double> coverlist=new ArrayList<Double>();
					ArrayList<Double> startposlist=new ArrayList<Double>();
					double total_time=0;
					boolean alien_road=false;
					for(int k=0;k<route_gids.length;k++){
						double coverage=-1.0;
						double startpos=-1.0;
						try{
							int gid=Integer.parseInt(route_gids[k]);
							if(gid<0){
								break;
							}
							//System.out.println("gid:"+gid);
							//start road
							if( k==0 && gid == cur_sample.pre_gid){
								startpos = pre_offset;
								if( gid != cur_sample.gid){
									if(gid<Common.roadlist.length && Common.roadlist[gid].reverse_cost>=0 && Common.roadlist[gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ //negative road
										//coverage = 1-pre_offset;
										coverage = pre_offset;
									}
									else if(gid<Common.roadlist.length && Common.roadlist[gid].to_cost>=0 && Common.roadlist[gid].to_cost < RoadCostUpdater.inconnectivity-1){ // positive road
										//coverage = pre_offset;
										coverage = 1-pre_offset;
									}
									else{
										alien_road=true;
										break;
									}
								}
								else{
									if(gid<Common.roadlist.length && Common.roadlist[gid].reverse_cost>=0 && Common.roadlist[gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ //negative road
										coverage= pre_offset-cur_sample.offset;
									}
									else if(gid<Common.roadlist.length && Common.roadlist[gid].to_cost>=0 && Common.roadlist[gid].to_cost < RoadCostUpdater.inconnectivity-1){ // positive road
										coverage= cur_sample.offset-pre_offset;
									}
									else{
										alien_road=true;
										break;
									}
								}
							}
							//last road
							else if(k==route_gids.length-1 && gid==cur_sample.gid){
								if(gid<Common.roadlist.length && Common.roadlist[gid].reverse_cost>=0 && Common.roadlist[gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ //negative road
									startpos = 1.0;
									coverage= 1-cur_sample.offset;
								}
								else if(gid<Common.roadlist.length && Common.roadlist[gid].to_cost>=0 && Common.roadlist[gid].to_cost < RoadCostUpdater.inconnectivity-1){ // positive road
									startpos = 0.0;
									coverage= cur_sample.offset;
								}
								else{
									alien_road=true;
									break;
								}
							}
							else{
								coverage=1.0;
								if( gid>=Common.roadlist.length || (Common.roadlist[gid].reverse_cost< 0 && Common.roadlist[gid].to_cost< 0)){
									alien_road=true;
									break;
								}
								else{
									if(Common.roadlist[gid].reverse_cost>=0 && Common.roadlist[gid].reverse_cost < RoadCostUpdater.inconnectivity-1){ //negative road
										startpos = 1.0;
									}
									else if(Common.roadlist[gid].to_cost>=0 && Common.roadlist[gid].to_cost < RoadCostUpdater.inconnectivity-1){ // positive road
										startpos = 0.0;
									}
								}
							}
									
							gidlist.add(gid);
							startposlist.add(startpos);
							coverlist.add(coverage);
							int next_gid=-1;
							if(k+1<route_gids.length){
								next_gid=Integer.parseInt(route_gids[k+1]);
							}
							total_time+=coverage*Common.roadlist[gid].length/Common.roadlist[gid].get_speed(next_gid);
							timelist.add(total_time);
						}
						catch(Exception e){
							e.printStackTrace();
							//break;
						}
					}
					if(alien_road){
						continue;
					}
							
					long start_time=cur_sample.utc.getTime()/1000-cur_sample.interval;
					//String sql="Insert into "+ result_table +" (seq, gid, next_gid, time, percent, interval, tmstp, suid, utc, start_pos) values \n";
					for(int k=0; k<gidlist.size();k++){
						int gid=gidlist.get(k);
						double coverage=coverlist.get(k);
						double start_pos=startposlist.get(k);
						/*if(cur_sample.suid==2234 && cur_sample.utc.getTime()/1000==1231233049){
							System.out.println("here");
						}*/
						int next_gid=-1;
						if(k+1< gidlist.size()){
							next_gid=gidlist.get(k+1);
						}
						double travel_time=cur_sample.interval*Common.roadlist[gid].length/Common.roadlist[gid].get_speed(next_gid)*coverage/total_time;
						if(Double.isNaN(travel_time)){
							Common.logger.debug("NaN: " + cur_sample.interval + " ;" + Common.roadlist[gid].length + " ;" + Common.roadlist[gid].get_speed(next_gid)
									+ " ;" + coverage + " ;" + total_time);//to check why NaN
							continue;
						}
						long tmsp= start_time + Math.round(timelist.get(k)-travel_time);
								
						//if(cur_sample.suid==4232){
							//System.out.println("here");
						//} 
						seq_num=(int)(max_seg-(Common.end_utc-cur_sample.utc.getTime()/1000)/Common.period);   
						//Common.logger.debug(cur_sample.utc.getTime()/1000 + ";seq number: " + seq_num);
						String sql="Insert into "+ table_prefix + seq_num + Common.Date_Suffix +" (gid, next_gid, time, percent, interval, tmstp, suid, utc, start_pos) values \n";
						String newsql="";
						if(k<gidlist.size()-1){
							newsql=" ("+gid+", "+gidlist.get(k+1)+", "+travel_time+", "+coverage+", "+cur_sample.interval+", "+tmsp+", "+cur_sample.suid+", "+cur_sample.utc.getTime()/1000+", "+start_pos+"); ";
						}
						else{
						    newsql=" ("+gid+", NULL , "+travel_time+", "+coverage+", "+cur_sample.interval+", "+tmsp+", "+cur_sample.suid+", "+cur_sample.utc.getTime()/1000+", "+start_pos+"); ";
						}
						pre_utc = cur_sample.utc.getTime()/1000;
						updates.add(sql + newsql);
						pre_suid=cur_sample.suid;
						    	
					}
				}
			}
		return updates;
	}
}
