/** 
* 2016Äê11ÔÂ10ÈÕ 
* TurningTimeForExp.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.experiment;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class TurningTimeForExp {
	public int size;
	public int[] gid_list;
	public int[] next_gid_list;
	public double[] turning_time_list;
	public HashMap<Integer, Double> ave_turning_time;
	//public ArrayList<Triplet<Integer,Integer,Double>> turning_list = null;
	public TurningTimeForExp(int length){
		gid_list = new int[length];
		next_gid_list = new int[length];
		turning_time_list = new double[length];
		ave_turning_time = new HashMap<Integer, Double>();
	}
	
	public int add_turning_time(int gid, int next_gid, double time){
		gid_list[size] = gid;
		next_gid_list[size] = next_gid;
		turning_time_list[size] = time;
		size++;
		return size;
	}
	
	public double get_turning_time(int gid, int next_gid){
		for(int i=0; i<size; i++){
			if(gid_list[i] == gid && next_gid_list[i] == next_gid){
				return turning_time_list[i];
			}
		}
		//get default average turning time
		/*if(ave_turning_time.get(gid) != null){
			return ave_turning_time.get(gid);
		}*/
		return -1;
	}
	
	

}
