package indi.zyu.realtraffic.process;

import indi.zyu.realtraffic.common.Common;

import java.util.TimerTask;

public class TrafficChecker extends TimerTask {

	int period = 1;
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//check whether speed of all roads in current period is empty
		/*for(int i=0;i<Common.roadlist.length;i++){
			if(Common.roadlist[i].get_seq() < period && period != 1){
				double default_speed = Common.default_traffic[i][period];
				if(default_speed > 0){
					Common.roadlist[i].update_speed_seq(default_speed, period);
				}
			}
		}*/
		
	}

}
