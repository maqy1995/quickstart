package guowenpeng;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class stream implements Runnable {
    private String input_path;
    private String output_path;
    public stream(String input_path,String output_path) {
        this.input_path=input_path;
        this.output_path=output_path;
    }

    @Override
    public void run() {
        try {
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Thread of Stream  Execute Start!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            StreamExecutionEnvironment streamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<String> stream=streamExecutionEnvironment.readTextFile(input_path);
            DataStream<Tuple2<String ,Integer>> result=stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    String[] array=s.split("\\W+");
                    for(String element:array)
                        collector.collect(new Tuple2<String, Integer>(element,1));
                }
            }).keyBy(0).sum(1);
            result.writeAsText(output_path).setParallelism(1);
            streamExecutionEnvironment.execute("stream job thread");
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Thread of Stream  Execute Over!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

