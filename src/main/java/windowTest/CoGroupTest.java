package windowTest;
/**
 *  dataStream.coGroup(otherStream)
 *     .where(0).equalTo(1)
 *     .window(TumblingEventTimeWindows.of(Time.seconds(3)))
 *     .apply (new CoGroupFunction () {...});
 *
 *     和Join的不同在于，CoGroupFunction中是迭代器
 *     Join则是每个元素进行连接
 */

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CoGroupTest {
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

        DataStream<String> result= first1.coGroup(second1).where(new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> iterable, Iterable<Tuple2<String, Integer>> iterable1, Collector<String> collector) throws Exception {
                        String s="";
                        int sum=0;
                        for(Tuple2<String,Integer> t1: iterable){
                            s=s+"  "+t1.f0;
                            sum=sum+t1.f1;
                        }
                        s=s+"**************";
                        for(Tuple2<String,Integer> t2:iterable1){
                            s=s+" "+t2.f0;
                            sum=sum+t2.f1*100;
                        }
                        collector.collect(s+sum);
                    }
                });

        result.print();
        env.execute();
    }
}
