package streamTransformationTest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();

        DataStreamSource<Integer> integer= see.fromElements(1);

        //DataStream<String> s=see.fromElements("I am a student","are you ok","I am fine");

        DataStream<Integer> MapResult=integer.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return 2*value;
            }
        });

        DataStream<String> s=see.fromElements("I am a student","are you ok","I am fine");

//        DataStream<String> flatMapResult=s.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                for(String word: s.split(" ")) {
//                    return word;
//                }
//            }
//        });


//        flatMapResult.print();
        DataStream<Integer> unionResult=integer.union(integer,integer,integer);
        //integer.print();
        unionResult.print();

        //MapResult.print();



        see.execute();

    }
}
