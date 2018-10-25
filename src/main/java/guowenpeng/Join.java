package guowenpeng;
/**
 *  created by guowenpeng at 2018.08
 *  先运行了一个批任务，将结果存储到ArrayList中，然后流任务通过该ArrayList来读取
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Join {
    public static void main(String args[]) throws Exception {
        //获取批处理环境
        ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
        //batch1为批文件1，batch1中的内容为<商品id，商品price>
        String batch1=args[0];
        //batch2为批文件2，batch2中的内容为<商品id，商品number>
        String batch2=args[1];
        //读取批文件1，并用DataSet<String> Batch1存放
        DataSet<String> Batch1=environment.readTextFile(batch1);
        //读取批文件2，并用DataStet<String> Batch2存放
        DataSet<String> Batch2=environment.readTextFile(batch2);
        //将batch1预处理成<商品id，商品price>的形式
        DataSet<Tuple2<String,Integer>> batch1_info=Batch1.flatMap(new Tokenizer());
        //将batch2预处理成<商品id，商品number>的形式
        DataSet<Tuple2<String,Integer>> batch2_info=Batch2.flatMap(new Tokenizer());
        //对商品价格表batch1和商品数量表batch2进行连接，结果存放在DataSet<Tuple3<Integer,Integer,Integer>> table中，分别对应商品id,商品price，商品number
        DataSet<Tuple3<String,Integer,Integer>> table=batch1_info.join(batch2_info).where(0).equalTo(0).projectFirst(0,1).projectSecond(1);
        //table_file为连接存放的路径
        String table_file=args[2];
        //将结果存放于table_file中
        table.writeAsText(table_file).setParallelism(1);
        //批环境执行
        environment.execute();

        //将table内容进行预处理【该处理针对不同数据可能会有不同的操作，先按照我们的预处理方式来处理，以后再想办法抽象成普遍的方式】
        //将table的内容处理成了Tuple3<String,Integer,Integer>,分别对应商品id,商品price，商品number
        final ArrayList<Tuple3<String,Integer,Integer>> arrayList=new ArrayList<Tuple3<String, Integer, Integer>>();
        try{
            BufferedReader table_content = new BufferedReader(new FileReader(table_file));
            String line = table_content.readLine();
            while(line != null)
            {

                String[] tokens = line.toLowerCase().split("\\W+");
                //读入数据放入　map　中
                String[] array=line.split(",");
                String commodity=array[0].substring(1,array[0].length());
                Integer price=Integer.parseInt(array[1]);
                Integer number=Integer.parseInt(array[2].substring(0,array[2].length()-1));
//                System.out.println(commodity+"  "+price+" "+number);
                arrayList.add(new Tuple3<String, Integer, Integer>(commodity,price,number));
                line=table_content.readLine();

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //创建流环境
        StreamExecutionEnvironment streamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment();
        //获取流数据源，以socket形式输入要查询的商品信息,记数据源为query_info
        DataStream<String> query_info=streamExecutionEnvironment.socketTextStream("hostname",9000);
        //将批的文件名，流环境，和流数据源传递给stream_to_join()函数，进行查询并打印
        query(query_info,arrayList,streamExecutionEnvironment);
        streamExecutionEnvironment.execute();





    }
    public static final class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] array=s.split(" ");
            String key=array[0];
            Integer value=Integer.parseInt(array[1]);
            collector.collect(new Tuple2<String, Integer>(key,value));
        }
    }
    public static void query(DataStream<String> stream, final ArrayList<Tuple3<String,Integer,Integer>> table, StreamExecutionEnvironment streamExecutionEnvironment) throws IOException {
        //以流的方式读取table
        // DataStream<String> table_info=streamExecutionEnvironment.readTextFile(table);

        /***********以流读文件的方式**************/
        //将table内容进行预处理【该处理针对不同数据可能会有不同的操作，先按照我们的预处理方式来处理，以后再想办法抽象成普遍的方式】
        //将table的内容处理成了Tuple3<String,Integer,Integer>,分别对应商品id,商品price，商品number
//        final DataStream<Tuple3<String,Integer,Integer>> table_content=table_info.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
//                String[] array=s.split(",");
//                String commodity=array[0].substring(1,array.length-1);
//                Integer price=Integer.parseInt(array[1]);
//                Integer number=Integer.parseInt(array[2]);
//                collector.collect(new Tuple3<String, Integer, Integer>(commodity,price,number));
//            }
//        });

        /***********以普通java读文件的方式**************/
//        final ArrayList<Tuple3<String,Integer,Integer>> arrayList=new ArrayList<Tuple3<String, Integer, Integer>>();
//        try{
//            BufferedReader table_content = new BufferedReader(new FileReader(table_file));
//            String line = table_content.readLine();
//            while(line != null)
//            {
//
//                String[] tokens = line.toLowerCase().split("\\W+");
//                //读入数据放入　map　中
//                String[] array=line.split(",");
//                String commodity=array[0].substring(1,array.length-1);
//                Integer price=Integer.parseInt(array[1]);
//                Integer number=Integer.parseInt(array[2]);
//                arrayList.add(new Tuple3<String, Integer, Integer>(commodity,price,number));
//            }
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        //对stream输入的元素进行查询，并且结果打印
        stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                for(Tuple3<String,Integer,Integer> tuple3:table){

//                    System.out.println(tuple3);
                    if(tuple3.f0.equals(s))
                        System.out.println(tuple3);
                }
                return s;
            }
        });

    }
}

