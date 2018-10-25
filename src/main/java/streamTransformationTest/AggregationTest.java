package streamTransformationTest;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment see =StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> t1= see.fromElements(1,3,5,7,9,18,1,1,18,18,18);

        //KeyedStream<Integer,Tuple> r1=t1.keyBy(t1.);

        //r1.sum(0).print();

        see.execute();
    }
}
