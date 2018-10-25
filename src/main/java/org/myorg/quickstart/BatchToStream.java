package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BatchToStream {
    public static void main(String[] args) throws Exception {
        if (args.length != 2){
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }
        //得到ip和端口号
        String hostName1 = args[0];
        Integer port1 = Integer.parseInt(args[1]);
        //创建流环境
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        //读取流数据
        //DataStream<String> streamtext = streamEnv.socketTextStream(hostName1, port1);

        DataStream<Tuple2<String, Integer>> streamcounts =streamEnv.socketTextStream(hostName1, port1).flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1);

        streamcounts.writeAsText("C:\\Users\\Administrator\\Desktop\\output\\streamout1");


        final ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        batchEnv.setParallelism(1);
        DataSet<String> batchtext = batchEnv.fromElements(
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,"
        );
        DataSet<Tuple2<String, Integer>> batchcounts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                batchtext.flatMap(new WordCount.LineSplitter())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // execute and print result
        batchcounts.writeAsText("C:\\Users\\Administrator\\Desktop\\output\\batchout1");


        //执行批
        //batchEnv.execute("Batch Running~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        //执行流
        streamEnv.execute("Stream Running~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    }




    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
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
