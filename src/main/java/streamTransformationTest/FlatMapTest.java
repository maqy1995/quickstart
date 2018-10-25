package streamTransformationTest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see= StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<String> s=see.fromElements("I am a student","are you ok","I am fine");

        DataStream<String> flatMapResult=s.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out)
                    throws Exception{
                for(String word: value.split(" ")){
                    out.collect(word);
                }
            }
        });

        flatMapResult.print();

        see.execute();
    }

}
