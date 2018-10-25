package windowTest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class SplitTest {
    public static void main(String[] args) throws Exception {
        String hostName1 = "localhost";
        Integer port1 = 9000;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //第一个流
        DataStream<String> first = env.socketTextStream(hostName1, port1);

        DataStream<Integer> first1 = first.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {
                String[] splited = value.split("\\W+");
                for(String s : splited){
                    out.collect(Integer.parseInt(s));
                }
            }
        });


        SplitStream<Integer> split = first1.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even");
                }
                else {
                    output.add("odd");
                }
                return output;
            }
        });

        DataStream<Integer> even =split.select("even");

        even.print();
        env.execute();
    }
}
