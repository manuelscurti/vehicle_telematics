package master2018.flink;

import java.util.HashSet;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class VehicleTelematics {
	// --- CONFIGURATION PARAMETERS --- //
	private static final long MAX_SPEED = 90;
	private static final long MAX_AVG_SPEED = 60;
	private static final String OUTPUT_FILE_SPEEDRADAR = "speedfines.csv";
	private static final String OUTPUT_FILE_AVGSPEEDRADAR = "avgspeedfines.csv";
	private static final String OUTPUT_FILE_ACCIDENT = "accidents.csv";
	private static final long START_SEGMENT = 52;
	private static final long END_SEGMENT = 56;
	private static final long NUM_SEGMENTS_BETWEEN = 5;
	private static final long NUM_EVENTS_STOPPED= 4;
	
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	
		//specified by command line
		final String inputFile = args[0];
		final String outputPath = args[1];
		
		//convert input strings to report objects
		SingleOutputStreamOperator<Report> reports = env.readTextFile(inputFile).map(s -> {
			return new Report(s);
		});
		
		// --- SPEED RADAR --- //
		reports
			.filter(r -> r.getSpeed() > MAX_SPEED)
			.map(r -> r.toSpeedFinesFormat())
			.setParallelism(10)
			.writeAsCsv(outputPath+"/"+OUTPUT_FILE_SPEEDRADAR, FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);
		
		// --- AVERAGE SPEED CONTROL --- //
		reports
			.filter(r -> r.getSegment() >= START_SEGMENT && r.getSegment() <= END_SEGMENT)
			.map(r -> new AvgReport(r.getTimestamp(),r.getTimestamp(),r.getVID(),r.getXWay(),r.getDirection(),r.getSpeed(),1,r.getSegment()))
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AvgReport>() { 
				// reference: https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/event_timestamps_watermarks.html
				@Override
				public long extractAscendingTimestamp(AvgReport a) {
					return a.getT1() * 1000; //both timestamps and watermarks are specified as milliseconds
				}
			})
			.keyBy(r -> new Tuple3<Long,Long,Long>(r.getVID(), r.getXWay(), r.getDirection()))
			.window(EventTimeSessionWindows.withGap(Time.seconds(31)))
			.reduce((r1,r2) -> new AvgReport(
					Long.min(r1.getT1(), r2.getT1()),
					Long.max(r1.getT2(), r2.getT2()),
					r1.getVID(),
					r1.getXWay(),
					r1.getDirection(),
					r1.getAvgSpeed() + r2.getAvgSpeed(),
					r1.getCount() + r2.getCount(),
					r1.getSegments(),
					r2.getSegments()))
			.filter(r -> r.computeAvg() > MAX_AVG_SPEED && r.getSegments().size() >= NUM_SEGMENTS_BETWEEN)
			.map(a -> a.toAvgSpeedFinesFormat())
			.setParallelism(10)
			.writeAsCsv(outputPath+"/"+OUTPUT_FILE_AVGSPEEDRADAR, FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		SingleOutputStreamOperator<Report> reportsAccident = env.readTextFile(inputFile).map(s -> {
			return new Report(s);
		});
		// --- ACCIDENT REPORTER --- //
		reports
				.filter(r -> r.getSpeed() == 0)
				.map(r -> new AccidentReport(r.getTimestamp(), r.getTimestamp(), r.getVID(), r.getXWay(), r.getSegment(), r.getDirection(), r.getPos(), 1))
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AccidentReport>() {
					// reference: https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/event_timestamps_watermarks.html
					@Override
					public long extractAscendingTimestamp(AccidentReport a) {
						return a.getT1() * 1000; //both timestamps and watermarks are specified as milliseconds
					}
				})
				.keyBy(r -> new Tuple5<Long,Long,Long,Long,Long>(r.getVid(), r.getXway(), r.getSeg(), r.getDir(), r.getPos()))
				.window(SlidingProcessingTimeWindows.of(Time.minutes(2), Time.seconds(30)))
				.reduce((r1,r2) -> new AccidentReport(
						Long.min(r1.getT1(), r2.getT1()),
						Long.max(r1.getT2(), r2.getT2()),
						r1.getVid(),
						r1.getXway(),
						r1.getSeg(),
						r1.getDir(),
						r1.getPos(),
						r1.getCounter() + r2.getCounter()
						))
				.filter(r -> r.getCounter() >= NUM_EVENTS_STOPPED)
				.map(r -> r.toAccidentFinesFormat())
				.setParallelism(10)
				.writeAsCsv(outputPath+"/"+OUTPUT_FILE_ACCIDENT, FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);




		// execute program
		try {
			env.execute("Real Time Traffic Analysis");
			System.out.print("Finished execution. You have the out files on the folder: /" + outputPath);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
