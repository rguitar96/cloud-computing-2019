package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting JFK Execution...");

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
        String inputPath = "";
        String outputPath = "";
        try {
            inputPath = args[0];
            outputPath = args[1];
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Input file and output folder path must be provided.");
            return;
        }
 */
        String inputPath = "/home/rodrigo/flink/cloud-computing-2019/YellowTaxiTrip/YellowTaxiTrip/yellow_tripdata_2019_06.csv";
        String outputPath = "/home/rodrigo/flink/cloud-computing-2019/YellowTaxiTrip/YellowTaxiTrip/output/";
        String	outFilePathJFK = outputPath +  "/jfk.csv";
        DataStream<String> source = env.readTextFile(inputPath).setParallelism(1);

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
                return new Tuple4<Integer, LocalDateTime, LocalDateTime, Integer>(Integer.parseInt(s[0]), LocalDateTime.parse(s[1], formatter), LocalDateTime.parse(s[1], formatter), Integer.parseInt(s[3]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, LocalDateTime, LocalDateTime, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Integer, LocalDateTime, LocalDateTime, Integer> input) {
                return input.f1.atZone(ZoneId.of("America/New_York")).toInstant().toEpochMilli();
            }
        }).keyBy(1).timeWindow(Time.hours(1)).sum(3);


        // emit result
        taxiTrips.writeAsText(outFilePathJFK);

        // execute program
        env.execute("JFK");
    }
}
