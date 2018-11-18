package master2018.flink;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple6;

public class AvgReport {
	private long t1; //timestamp begin
	private long t2; //timestamp end
	private long vid;
	private long xway;
	private long dir;
	private double avgspd;
	private long count_acc;
	private Set<Long> segments;
	
	public AvgReport(long t1, long t2, long vid, long xway, long dir, double avgspd, long count_acc, long segment) {
		this.t1 = t1;
		this.t2 = t2;
		this.vid = vid;
		this.xway = xway;
		this.dir = dir;
		this.avgspd = avgspd;
		this.count_acc = count_acc;
		segments = new HashSet<>();
		segments.add(segment);
	}
	
	public AvgReport(long t1, long t2, long vid, long xway, long dir, double avgspd, long count_acc, Set<Long> segs1, Set<Long> segs2) {
		this.t1 = t1;
		this.t2 = t2;
		this.vid = vid;
		this.xway = xway;
		this.dir = dir;
		this.avgspd = avgspd;
		this.count_acc = count_acc;
		this.segments = new HashSet<>();
		this.segments.addAll(segs1);
		this.segments.addAll(segs2);
	}
	
	public long getT1() {
		return this.t1;
	}
	
	public long getT2() {
		return this.t2;
	}
	
	public double getAvgSpeed() {
		return this.avgspd;
	}
	
	public long getVID() {
		return this.vid;
	}
	
	public long getXWay() {
		return this.xway;
	}
	
	public long getDirection() {
		return this.dir;
	}
	
	public long getCount() {
		return this.count_acc;
	}
	
	public Set<Long> getSegments(){
		return this.segments;
	}
	
	public double computeAvg() {
		if(count_acc > 0)
			this.avgspd = this.avgspd / this.count_acc;
		
		return this.avgspd;
	}
	
	public Tuple6<Long,Long,Long,Long,Long,Double> toAvgSpeedFinesFormat(){
		//format: T1,T2,VID,XWay,Dir,AvgSpd
		return new Tuple6<>(t1,t2,vid,xway,dir,avgspd);
	}
	
}
