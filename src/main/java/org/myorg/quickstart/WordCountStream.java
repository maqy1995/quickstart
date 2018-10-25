package org.myorg.quickstart;

import org.apache.commons.math3.filter.ProcessModel;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

public class WordCountStream {
    //
    //	Program
    //

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        String input="C:\\Users\\Administrator\\Desktop\\output\\test.txt";
        String output="C:\\Users\\Administrator\\Desktop\\output\\out1";
        int parallism = 1;

        env.setParallelism(parallism);
        //这里实现了一直对文件的监视
        DataStream<Tuple2<String, Integer>> counts = env.readFile(new TextInputFormat(new Path("C:\\Users\\Administrator\\Desktop\\output\\test.txt")),"C:\\Users\\Administrator\\Desktop\\output\\test.txt",FileProcessingMode.PROCESS_CONTINUOUSLY,5000)
                // split up the lines in pairs (2-tuples) containing: (word,1)
                .flatMap(new LineSplitter())
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("value1:"+value1.getField(0)+"   "+"value2:"+value2.getField(0));
                        Tuple2<String, Integer> result =new Tuple2<>(value1.getField(0),(Integer)value1.getField(1)+(Integer)value2.getField(1));
                        return result;
                    }
                });

        // execute and print result
        //counts.writeAsText(output);
        counts.print();
        //执行程序
        env.execute("WordCountStream~~~~~~~~~~~~~");

    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2&lt;String, Integer&gt;).
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
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
