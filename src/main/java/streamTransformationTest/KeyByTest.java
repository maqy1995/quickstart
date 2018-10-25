package streamTransformationTest;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class KeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer,String>> t1=see.fromElements(Tuple2.of(1,"first"),Tuple2.of(2,"second"),Tuple2.of(1,"FIRST"));
        DataStream<Tuple2<Integer,Integer>> t2=see.fromElements(Tuple2.of(1,1),Tuple2.of(2,2),Tuple2.of(1,3));
        DataStream<String> t3=see.fromElements("a","aa","aaa");

        KeyedStream<Tuple2<Integer,String>,Tuple> r1=t1.keyBy(0);

        KeyedStream<Tuple2<Integer,Integer>,Tuple> r2=t2.keyBy(0);

        DataStream<Tuple2<Integer,Integer>> tt2=r2.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> t1) throws Exception {
                return Tuple2.of(t1.getField(0),t1.getField(1));
            }
        });
        //tt2.print();

        DataStream<String> tt1=r1.fold("start", new FoldFunction<Tuple2<Integer, String>, String>() {
            @Override
            public String fold(String s, Tuple2<Integer, String> o) throws Exception {
                return s+"---"+o.getField(1);
            }
        });

        //tt1.print();
        //r1.print();


        see.execute();
    }
}
