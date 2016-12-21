/** 
* 2016Äê11ÔÂ12ÈÕ 
* TravelTimeInfo.java 
* author:ZhangYu
*/ 
package indi.zyu.realtraffic.experiment;

import indi.zyu.realtraffic.gps.Sample;

import java.util.ArrayList;

public class TravelTimeInfo {
	private int gen_id;
	private ArrayList<Integer> gid_list = new ArrayList<Integer>();
	private ArrayList<Sample>  old_sample_list = new ArrayList<Sample>();
	private ArrayList<Sample>  new_sample_list = new ArrayList<Sample>();
	private ArrayList<Integer> seq_list = new ArrayList<Integer>();
	private ArrayList<Double> time_list = new ArrayList<Double>();
	private int    sample_number;
	private double error_rate;
	private double real_time;
	private double exp_time;
	
	public TravelTimeInfo(int gen_id, ArrayList<Integer> gid_list, ArrayList<Integer> seq_list,  ArrayList<Double> time_list,
			ArrayList<Sample> old_sample_list, ArrayList<Sample> new_sample_list, int sample_number, double error_rate, double real_time, double exp_time){
		this.gen_id = gen_id;
		this.gid_list.addAll(gid_list);
		this.seq_list.addAll(seq_list);
		this.time_list.addAll(time_list);
		this.old_sample_list.addAll(old_sample_list);
		this.new_sample_list.addAll(new_sample_list);
		this.sample_number = sample_number;
		this.error_rate = error_rate;
		this.real_time = real_time;
		this.exp_time = exp_time;
	}
	
	public String toString(){
		String info = "";
		info += "gen_id: " + gen_id;
		info += "\ngis_list: " + gid_list.toString();
		info += "\nseq_list: " + seq_list.toString();
		info += "\ntime_list: " + time_list.toString();
		info += "\nold_sample_list: " + old_sample_list.toString();
		info += "\nnew_sample_list: " + new_sample_list.toString();
		info += "\nsample number: " + sample_number;
		info += "\nerror_rate: " + error_rate;
		info += "\ntime: " + real_time + "; " + exp_time;
		return info;
	}
}
