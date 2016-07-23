package indi.zyu.realtraffic.process;

import indi.zyu.realtraffic.common.Common;
import java.util.ArrayList;

/** 
 * 2016Äê4ÔÂ16ÈÕ 
 * ProcessThread.java 
 * author:ZhangYu
 */

public class ProcessThread extends Thread {

	ArrayList<Integer> suid_list = null;
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		
		while(true){
			//Common.logger.debug("processing");
			try {
				Object[] temp_suid_list;
				synchronized(suid_list){
					temp_suid_list = suid_list.toArray();
				}
				for(int i=0; i< temp_suid_list.length; i++){
					int suid = (int) temp_suid_list[i];
					Common.taxi[suid].process();
				}
			
				Thread.sleep(2*1000);
				//Common.logger.debug(suid_list.toString());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public ProcessThread(){
		this.suid_list = new ArrayList<Integer>();
		
	}
	
	synchronized public void put_suid(int suid){
		if(suid_list.contains(suid)){
			return;
		}
		synchronized(suid_list){
			suid_list.add(suid);
		}		
	}
}
