package streamTransformationTest;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<String> s=see.fromElements("abcde","ffff");

        DataStream<Integer> i=see.fromElements(1,2,3,4,5,5);

        ConnectedStreams<String,Integer> cs=s.connect(i);

        DataStream<Boolean> result=cs.map(new CoMapFunction<String, Integer, Boolean>() {
            @Override
            public Boolean map1(String vaule){
                return false;
            }

            @Override
            public Boolean map2(Integer vaule){
                return true;
            }
        });


        result.print();
        see.execute();
    }
}
