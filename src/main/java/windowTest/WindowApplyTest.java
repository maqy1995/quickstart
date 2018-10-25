package windowTest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowApplyTest {

    public static void main(String[] args) throws Exception {

        String hostName1 = "localhost";
        Integer port1 = 9000;

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // get input data
        //DataStream<String> text = env.socketTextStream(hostName1, port1);

        DataStream<String> a = env.socketTextStream(hostName1, port1);
        // split up the lines in pairs (2-tuples) containing: (word,1)

        DataStream<Tuple2<String, Integer>> b = a.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] ss = value.split("\\W+");
                for (String s : ss) {
                    if (s.length() > 0) {
                        out.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        });

        //AllWindowedStream<Tuple2<String ,Integer>, TimeWindow> allWindowedStream = b.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        /**
         *    WindowedStream<T, K, W extends Window>    K is key ，W is WindowAssigner
         */
        WindowedStream<Tuple2<String, Integer>,Tuple, TimeWindow> windowWindowedStream = b.union(b).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
//        DataStream<Tuple2<String, Integer>> result=allWindowedStream.apply (new AllWindowFunction<Tuple2<String,Integer>, Tuple2<String, Integer>, TimeWindow>() {
//            public void apply (TimeWindow window, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String s=null;
//                int sum = 0;
//                for (Tuple2<String,Integer> t: values) {
//                    sum += t.f1;
//                    s=t.f0;
//                }
//                out.collect (new Tuple2<String, Integer>(s,sum));
//            }
//        });
        DataStream<Tuple2<String,Integer>> result = windowWindowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("Tuple"+tuple.toString());
                String s = null;
                int sum = 0;
                for(Tuple2<String ,Integer> t: input){
                    sum +=t.f1;
                    s=t.f0;
                }
                out.collect(new Tuple2<String,Integer>(s,sum));
            }
        });
//        DataStream<Tuple2<String,Integer>> result = allWindowedStream.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
//            @Override
//            public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
//
//            }
//        });

        result.print();

        // execute program
        env.execute("Java WordCount from SocketTextStream Example");
        //System.out.println(env.getExecutionPlan());
    }

    //
    // 	User Functions
    //

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2&lt;String, Integer&gt;).
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // normalize and split the line
            //new Test().test();
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
