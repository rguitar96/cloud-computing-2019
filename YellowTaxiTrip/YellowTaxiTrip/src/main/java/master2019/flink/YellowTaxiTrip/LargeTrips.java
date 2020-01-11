package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;


public class LargeTrips {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Large Trips Execution...");

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input file path and output folder path
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

        SingleOutputStreamOperator<Tuple5<Integer, String, Integer, String, String>>
                taxiTrips = source.filter(new FilterFunction<String>() {
            @Override
            // Filter trips that take less than 20 minutes.
            public boolean filter(String value) {
                String[] s = value.split(",");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                LocalDateTime start = LocalDateTime.parse(s[1], formatter);
                LocalDateTime end = LocalDateTime.parse(s[2], formatter);

                long distanceMillis = Duration.between(start, end).toMillis();
                long distanceMinutes = distanceMillis / (1000 * 60);

                return distanceMinutes >= 20;
            }
        }).map(new MapFunction<String, Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime>>() {
            @Override
            // Converts the String to a Tuple5 extracting the desired output data
            public Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime> map(String value) {
                String[] s = value.split(",");
                //  Pattern: 2019-06-01 00:55:13
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return new Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime>
                        (Integer.parseInt(s[0]), LocalDateTime.parse(s[1], formatter).toLocalDate(), 1,
                                LocalDateTime.parse(s[1], formatter), LocalDateTime.parse(s[2], formatter));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime>>() {
            @Override
            // Assigns a Timestamp in order to use time windows
            public long extractAscendingTimestamp(Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime> input) {
                return input.f3.atZone(ZoneId.of("America/New_York")).toInstant().toEpochMilli();
            }
        }).keyBy(0).window(TumblingEventTimeWindows.of(Time.hours(3))).reduce(new SummingReducer())
        .filter(new FilterFunction<Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime>>() {
            @Override
            // Once grouped, filters the registers with less than 5 trips in the 3 hour window
            public boolean filter(Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime> value) {
                int numberOfTrips = value.f2;
                return numberOfTrips >= 5;
            }
        }).map(new MapFunction<Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime>, Tuple5<Integer, String, Integer, String, String>>() {
            @Override
            // Just to format the dates to match the PDF output
            public Tuple5<Integer, String, Integer, String, String> map(Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime> value) {
                //  Pattern: 2019-06-01 00:55:13
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                return new Tuple5<Integer, String, Integer, String, String>
                        (value.f0, value.f1.format(formatterDate), value.f2, value.f3.format(formatter), value.f4.format(formatter));
            }
        });

        // emit result
        taxiTrips.writeAsCsv(outFilePathLargeTrips, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute program
        env.execute("Large Trips");
    }

    private static class SummingReducer implements ReduceFunction<Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime>> {
        @Override
        // Sums the number of trips (value1.f2 + value2.f2), gets first starting trip date (value1.f3) and gets last ending trip date (value2.f4)
        public Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime>
        reduce(Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime> value1, Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime> value2) {
            return new Tuple5<Integer, LocalDate, Integer, LocalDateTime, LocalDateTime>
                    (value1.f0, value1.f1, value1.f2 + value2.f2, value1.f3, value2.f4);
        }
    }
}
