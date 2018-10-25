package streamTransformationTest;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see=StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Integer> t1=see.fromElements(1,2,3,4,5,6,7,8);

        SplitStream<Integer> split=t1.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer integer) {
                List<String> output=new ArrayList<String>();
                if(integer%2==0){
                    output.add("even");
                }
                else{
                    output.add("odd");
                }
                return output;
            }
        });

        DataStream<Integer> even=split.select("even");
        DataStream<Integer> odd=split.select("odd");
        DataStream<Integer> all=split.select("even","odd");

        odd.print();
        //even.print();
        see.execute();
    }
}
