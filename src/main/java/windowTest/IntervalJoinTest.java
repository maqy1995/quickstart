package windowTest;
/**
 * created by maqy .2018.09.20
 * 在flink1.6中才新加入的IntervalJoin，flink1.4版本没有这个功能
 */
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {

        String hostName1 = "localhost";
        Integer port1 = 9000;
        Integer port2 = 9999;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //第一个流
        DataStream<String> first = env.socketTextStream(hostName1, port1);
        //第二个流
        DataStream<String> second = env.socketTextStream(hostName1,port2);

        DataStream<Tuple2<String, Integer>> first1 = first.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] ss = value.split("\\W+");
                for (String s : ss) {
                    if (s.length() > 0) {
                        out.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        });
        DataStream<Tuple2<String, Integer>> second1 = second.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] ss = value.split("\\W+");
                for (String s : ss) {
                    if (s.length() > 0) {
                        out.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>,Tuple>  keyedStream1 = first1.keyBy(0);
        KeyedStream<Tuple2<String, Integer>,Tuple>  keyedStream2 = second1.keyBy(0);

//        DataStream resutlt=keyedStream1.intervalJoin(keyedStream2).between(Time.seconds(-3),Time.seconds(+3))
//                .process(new IntervalJoinF)
//        result.print();
        env.execute();
    }

}
