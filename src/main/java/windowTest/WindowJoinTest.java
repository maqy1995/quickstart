package windowTest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowJoinTest {
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

        JoinedStreams<Tuple2<String, Integer>,Tuple2<String, Integer>> joinedStreams=first1.join(second1);

        DataStream result=joinedStreams.where(new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5))) //必须得添加window，然后通过apply(JoinFunction())来转换为DataStream
                .apply(new JoinFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple4<String,Integer,Integer,Integer>>() {
                    @Override
                    public Tuple4<String, Integer, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return new Tuple4<>(first.f0,first.f1,second.f1,first.f1+second.f1);
                    }
                });
        result.print();
        env.execute();
    }
}
