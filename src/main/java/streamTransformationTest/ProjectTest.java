package streamTransformationTest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple3;

public class ProjectTest {
    public static void main(String[] args ) throws Exception{
        StreamExecutionEnvironment see =StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Tuple3<Integer,Double,String>> t1=see.fromElements(new Tuple3(1,1.1,"FIRST"));

        DataStream<Tuple2<String,Integer>> tt1=t1.project(2,0);

        t1.print();

        see.execute();
    }
}
