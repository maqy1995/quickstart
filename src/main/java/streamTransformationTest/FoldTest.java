package streamTransformationTest;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.KeySelector;

public class FoldTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> i=env.fromElements(1,2,3,4,5,5);

        env.setParallelism(1);
        KeyedStream<Integer,Integer> i1=i.keyBy(new org.apache.flink.api.java.functions.KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return 1;
            }
        });

        i1.print();

        DataStream<String> result = i1.fold("start", new FoldFunction<Integer, String>() {
            @Override
            public String fold(String accumulator, Integer value) throws Exception {
                return accumulator+"-"+value;
            }
        });
        result.print();

        env.execute();
    }
}
