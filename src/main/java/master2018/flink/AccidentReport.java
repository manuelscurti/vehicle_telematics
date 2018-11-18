package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple7;

public class AccidentReport {

    private long t1; //timestamp begin
    private long t2; //timestamp end
    private long vid;
    private long xway;
    private long seg;
    private long dir;
    private long pos;
    private long counter;

    public AccidentReport(long t1, long t2, long vid, long xway, long seg, long dir, long pos, long counter) {
        this.t1 = t1;
        this.t2 = t2;
        this.vid = vid;
        this.xway = xway;
        this.seg = seg;
        this.dir = dir;
        this.pos = pos;
        this.counter = counter;
    }

    public long getT1() {
        return t1;
    }

    public long getT2() {
        return t2;
    }

    public long getVid() {
        return vid;
    }

    public long getXway() {
        return xway;
    }

    public long getSeg() {
        return seg;
    }

    public long getDir() {
        return dir;
    }

    public long getPos() {
        return pos;
    }

    public long getCounter() {
        return counter;
    }

    public Tuple7<Long, Long, Long, Long, Long, Long, Long> toAccidentFinesFormat() {
        //format: T1,T2,VID,XWay,Dir,AvgSpd
        return new Tuple7<>(t1, t2, vid, xway, seg, dir, pos);
    }
}
