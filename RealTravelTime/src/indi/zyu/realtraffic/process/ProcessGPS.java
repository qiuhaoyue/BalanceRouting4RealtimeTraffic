package indi.zyu.realtraffic.process;

import indi.zyu.realtraffic.gps.Sample;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** 
 * 2016Äê4ÔÂ16ÈÕ 
 * ProcessGPS.java 
 * author:ZhangYu
 */

public class ProcessGPS {
	public static ArrayList taxi[] = null;//taxi sample
	public static ExecutorService fixedThreadPool = null;//thread pool
	//public static ExecutorService cachedThreadPool = null;//thread pool
	
	//some configuration
	int max_queue_len;//start a thread to process data if the number of points in a queue exceed max_queue_len
	int leave_number; //leave some point to keep continuity, not flush all point 
	int expiration_time;//flush all data if a queue is expired
	
	int counter;//to record times of execute thread
	private static Lock lock = null;
	
	ProcessGPS(int max_queue_len, int leave_number, int expiration_time, int max_suid, int pool_size){
		this.max_queue_len   = max_queue_len;
		this.leave_number    = leave_number;
		this.expiration_time = expiration_time;
		taxi = new ArrayList[max_suid + 1];
		//fixedThreadPool = Executors.newFixedThreadPool(pool_size);
		//Runtime.getRuntime().availableProcessors()
		int size = Runtime.getRuntime().availableProcessors();
		System.out.println("pool size: " + size);
		//fixedThreadPool = Executors.newFixedThreadPool(pool_size);
		fixedThreadPool = Executors.newFixedThreadPool(size);
		//cachedThreadPool = Executors.newCachedThreadPool();
		lock = new ReentrantLock();
		counter = 0;
		//start timer to aggregate traffic every certain time
		/*Timer timer = new Timer();
		timer.schedule(new TimerTask(){
			public void run(){
				flush_all();//flush all points
			}  
		}, 15 * 60 * 1000, expiration_time * 60 * 1000);*/
	}
	//get a sample instance and process
	public void process(Sample gps){
		//add point to queue by suid
		int suid = (int) gps.suid;
		if(taxi[suid] == null){
			taxi[suid] = new ArrayList<Sample>();		
		}
		//lock.lock();
		taxi[suid].add(gps);
		if(taxi[suid].size() > max_queue_len){
			counter++;	
			//fixedThreadPool.execute(new ProcessThread(suid, taxi[suid]));
			//cachedThreadPool.execute(new ProcessThread(suid, taxi[suid]));
			taxi[suid].clear();
			//Common.logger.debug("process: " + counter);
		}
		//lock.unlock();
	}
	/*public void flush_all(){
		for(int i=0; i<taxi.length; i++){			
			if(taxi[i] != null && taxi[i].size() > 0){
				lock.lock();
				fixedThreadPool.execute(new ProcessThread(i, taxi[i]));
				taxi[i].clear();
				lock.unlock();
			}
			
		}
		Common.logger.debug("flush all points done!");
	}*/
	public void clear(){
		fixedThreadPool.shutdown();
		//cachedThreadPool.shutdown();
	}
}
