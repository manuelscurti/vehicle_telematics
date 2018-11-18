package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple6;

public class Report {
	private long timestamp;
	private long vid; //vehicle ID
	private long spd; //speed
	private long xway; //highway ID
	private long lane; //lane occupied by the vehicle
	private long dir; //direction
	private long seg; //segment
	private long pos; //horizontal position
	
	public Report(String input) {
		String split[] = input.split(",");
		this.timestamp = Long.parseLong(split[0]);
		this.vid = Long.parseLong(split[1]);
		this.spd = Long.parseLong(split[2]);
		this.xway = Long.parseLong(split[3]);
		this.lane = Long.parseLong(split[4]);
		this.dir = Long.parseLong(split[5]);
		this.seg = Long.parseLong(split[6]);
		this.pos = Long.parseLong(split[7]);
	}
	
	public long getSpeed() {
		return this.spd;
	}
	
	public long getSegment() {
		return this.seg;
	}
	
	public long getTimestamp() {
		return this.timestamp;
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

	public long getPos() { return pos; }

	public Tuple6<Long,Long,Long,Long,Long,Long> toSpeedFinesFormat(){
		//format: Time,VID,XWay,Seg,Dir,Spd
		return new Tuple6<>(timestamp,vid,xway,seg,dir,spd);
	}
	
	
	
	public String toString() {
		return timestamp+", "+vid+", "+spd+", "+xway+", "+lane+", "+dir+", "+seg+", "+pos;
	}
	
}
