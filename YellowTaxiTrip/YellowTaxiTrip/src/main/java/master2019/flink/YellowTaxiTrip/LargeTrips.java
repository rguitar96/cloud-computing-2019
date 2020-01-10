package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * In this class the Large trips program has to be implemented
 */
public class LargeTrips {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Large Trips Execution...");

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String inputPath = "";
        String outputPath = "";
        try {
            inputPath = params.get("input");
            outputPath = params.get("output");
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Input file and output folder path must be provided.");
            return;
        }

        // If it is not an absolute path, make it absolute
        if (params.get("input").charAt(0) != '/') inputPath = System.getProperty("user.dir") + "/" + inputPath;
        if (params.get("output").charAt(0) != '/') outputPath = System.getProperty("user.dir") + "/" + outputPath;

        String outFilePathLargeTrips = outputPath + "/largeTrips.csv";
        DataStream<String> source = env.readTextFile(inputPath);

        //Splits the lines by commas, discards the lines with passengers under 2. Parses the String to a tuple of integers.
        SingleOutputStreamOperator<Tuple4<Integer, LocalDateTime, LocalDateTime, Integer>>
                taxiTrips = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                String[] s = value.split(",");
                int passengerCount = Integer.parseInt(s[3]);
                boolean is_jfk = Integer.parseInt(s[5]) == 2;
                return passengerCount >= 2 && is_jfk;
            }
        }).map(new MapFunction<String, Tuple4<Integer, LocalDateTime, LocalDateTime, Integer>>() {
            @Override
            public Tuple4<Integer, LocalDateTime, LocalDateTime, Integer> map(String value) {
                String[] s = value.split(",");
                //  Pattern: 2019-06-01 00:55:13
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return new Tuple4<Integer, LocalDateTime, LocalDateTime, Integer>(Integer.parseInt(s[0]), LocalDateTime.parse(s[1], formatter), LocalDateTime.parse(s[2], formatter), Integer.parseInt(s[3]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, LocalDateTime, LocalDateTime, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Integer, LocalDateTime, LocalDateTime, Integer> input) {
                return input.f1.atZone(ZoneId.of("America/New_York")).toInstant().toEpochMilli();
            }
        }).keyBy(0).window(TumblingEventTimeWindows.of(Time.hours(3))).reduce(new SummingReducer());

        // emit result
        taxiTrips.writeAsCsv(outFilePathLargeTrips, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute program
        env.execute("Large Trips");
    }

    private static class SummingReducer implements ReduceFunction<Tuple4<Integer, LocalDateTime, LocalDateTime, Integer>> {

        @Override
        public Tuple4<Integer, LocalDateTime, LocalDateTime, Integer> reduce(Tuple4<Integer, LocalDateTime, LocalDateTime, Integer> value1, Tuple4<Integer, LocalDateTime, LocalDateTime, Integer> value2) {
            return new Tuple4<Integer, LocalDateTime, LocalDateTime, Integer>(value1.f0, value1.f1, value2.f2, value1.f3 + value2.f3);
        }
    }
}
