import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class batchAPI {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment=ExecutionEnvironment.getExecutionEnvironment();
        //executionEnvironment.createInput()
        		DataSet<String> dataSet = executionEnvironment.fromElements(
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,"
				);
        		DataSet<Tuple3<Integer,String,String>> dataSet1=executionEnvironment.fromElements(new Tuple3<>(1,"a","b"),new Tuple3<>(2,"c","d"));
        		DataSet<Tuple2<Integer,String>> out=dataSet1.project(0,1);
        		out.print();
//        dataSet.map();
//        DataSet<Tuple2<String,Integer>> mapResult=dataSet.map(new MapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public Tuple2<String,Integer> map(String value) throws Exception {
//                String[] split=value.split("\\W+");
//
//            }
//        });
        executionEnvironment.execute();
//        dataSet.flatMap();
    }
}
