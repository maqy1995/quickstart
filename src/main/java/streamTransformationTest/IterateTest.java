package streamTransformationTest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see=StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Integer> initialStream=see.fromElements(3,3,3,3,3,3);

        IterativeStream<Integer> iteration = initialStream.iterate();
        DataStream<Integer> iterationBody = iteration.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer-1;
            }
        });


        DataStream<Integer> feedback = iterationBody.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer>0;
            }
        }).setParallelism(1);

        iteration.closeWith(feedback);

        DataStream<Integer> output = iterationBody.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer<=0;
            }
        });

        feedback.print();

        see.execute();
    }





}
