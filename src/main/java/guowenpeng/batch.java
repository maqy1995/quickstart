package guowenpeng;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class batch implements Runnable {
    private String input_path;
    private String output_path;

    public batch(String input_path,String output_path) {
        this.input_path=input_path;
        this.output_path=output_path;
    }

    @Override
    public void run() {
        try {
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Thread of Batch Begin Execute!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
            DataSet<String> batch=environment.readTextFile(input_path);
            DataSet<Tuple2<String,Integer>> result=batch.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    String[] array=s.split("\\W+");
                    for(String element:array){
                        collector.collect(new Tuple2<String, Integer>(element,1));
                    }
                }
            }).groupBy(0).sum(1);
            result.writeAsText(output_path).setParallelism(1);
            environment.execute("batch job thread");
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Thread of Batch Execute Over!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

