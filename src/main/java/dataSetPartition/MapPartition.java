package dataSetPartition;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

class MyPartition implements Partitioner {
    @Override
    public int partition(Object key, int numPartitions) {
        if(((String)key).equals("j")){
            return 1;
        }else
            return 0;
    }
}
class MyPartition1 implements Partitioner {
    @Override
    public int partition(Object key, int numPartitions) {
        if(((String)key).equals("a")||((String)key).equals("b")||((String)key).equals("c")||((String)key).equals("d")||((String)key).equals("e")) {
            return 0;
        }else{
            return 1;
        }
    }
}
public class MapPartition {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if(args.length != 3){
            System.out.println("the use is   input  output parallelism");
            return;
        }

        String input = args[0];
        String output = args[1];
        int parallelism = Integer.parseInt(args[2]);

        //设置并行度
        env.setParallelism(parallelism);
        DataSet<String> text = env.readTextFile(input).rebalance();

        //DataSet<String> text1=text.rebalance();
        DataSet<Tuple2<String,Integer>> words = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split("\\W+");
                for(String s:strings){
                    out.collect(new Tuple2<String, Integer>(s,1));
                }
            }
        });
        //DataSet<Tuple2<String,Integer>> RangeWords = words.partitionByHash(0);
        DataSet<Tuple2<String,Integer>> RangeWords = words.partitionByRange(0);
        //DataSet<Tuple2<String,Integer>> RangeWords = words.partitionCustom(new MyPartition1(),0);
        //words.print();
        //DataSet<Tuple2<String,Integer>> counts = RangeWords.groupBy(0).sum(1);
        RangeWords.writeAsText(output);
        env.execute("this is a custom partition(abcde) job!!!");
    }
}
