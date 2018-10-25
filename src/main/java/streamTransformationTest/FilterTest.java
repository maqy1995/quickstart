package streamTransformationTest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Integer> integerDataStream = see.fromElements(0, 1, 3, 2, 4, 8, 0,1,1,1,1,1,1,1);

        DataStream<Integer> filterResult = integerDataStream.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer != 1;
            }
        });

        System.out.println("-----------------------------------------------------");
        filterResult.print();

        see.execute();
    }
}
