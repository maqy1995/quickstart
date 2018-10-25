package org.myorg.quickstart;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.logging.Logger;


public class UserScore {

    /**
     * Class to hold info about a game event.
     */

    static class GameActionInfo {
        String user;
        String team;
        Integer score;
        Long timestamp;

        public GameActionInfo() {}

        public GameActionInfo(String user, String team, Integer score, Long timestamp) {
            this.user = user;
            this.team = team;
            this.score = score;
            this.timestamp = timestamp;
        }

        public String getUser() {
            return this.user;
        }
        public String getTeam() {
            return this.team;
        }
        public Integer getScore() {
            return this.score;
        }
        public String getKey(String keyname) {
            if (keyname.equals("team")) {
                return this.team;
            } else {  // return username as default
                return this.user;
            }
        }
        public String toString(){
            return "user:"+user+"\t"+"team:"+team+"\t"+"score:"+score;
        }

        public Long getTimestamp() {
            return this.timestamp;
        }
    }

    /**
     * Parses the raw game event info into GameActionInfo objects. Each event line has the following
     * format: username,teamname,score,timestamp_in_ms,readable_time
     * e.g.:
     * user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
     * The human-readable time string is not used here.
     */
    //get GameActionInfo via text
    public static final class ParseEventFn implements MapFunction<String,GameActionInfo>{

        @Override
        public GameActionInfo map(String s) throws Exception {
            String[] components=s.split(",");
           // GameActionInfo gInfo=null;

            //try catch中都得return，处理数据的时候里面会有异常值，因此得用try catch
            try{
                String user = components[0].trim();
                String team = components[1].trim();
                Integer score = Integer.parseInt(components[2].trim());
                Long timstamp = Long.parseLong(components[3].trim());
                GameActionInfo gInfo = new GameActionInfo(user,team,score,timstamp);
                return gInfo;
            }catch (ArrayIndexOutOfBoundsException | NumberFormatException e){
                System.out.println("errormmmmmmmmmmmmm"+e.getMessage());
                //发现异常值之后，不能直接返回空的GameActionInfo，不然后面map之类的处理会出错。因此这里额外定义一个bad的Info。
                GameActionInfo gInfo = new GameActionInfo("bad","bad",1,0L);
                return gInfo;
            }
            //return gInfo;
        }
    }

    //get Tuple2<String,Integer> via GameActionInfo
    public static final class ExtractInfo implements MapFunction<GameActionInfo , Tuple2<String,Integer>> {

        private final String field;

        ExtractInfo(String field){this.field=field;}

        @Override
        public Tuple2<String, Integer> map(GameActionInfo gameActionInfo) throws Exception {
            return new Tuple2<String, Integer>(gameActionInfo.getKey(field),gameActionInfo.getScore());
        }


    }


    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
//        if (args.length != 3){
//            System.err.println("please input inputFilePath and OutputFilePath");
//            return;
//        }
        long startTime=System.currentTimeMillis();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // get input data
        String inputFilePath="/media/maqy/DATA/test/500w/";
        String outputFilePath="/media/maqy/DATA/test/result11223";
        int parallelism=1;
        if (args.length==3){
            inputFilePath=args[0];

            outputFilePath=args[1];

            parallelism=Integer.parseInt(args[2]);
        }
        env.setParallelism(parallelism);

//        DataSet<String> text = env.readTextFile("/home/maqy/文档/beam_samples/mobileGame/data/100w/gaming_data1_0_2.csv");
        DataSet<String> text = env.readTextFile(inputFilePath);

        DataSet<Tuple2<String,Integer>> counts = text.map(new ParseEventFn())
                .map(new ExtractInfo("user"))
                .groupBy(0)
                .sum(1);

//        counts.print();
//        counts.writeAsText("/home/maqy/文档/beam_samples/mobileGame/data/100w/result");
        counts.writeAsText(outputFilePath);
        //counts.writeAsText("/home/maqy/Documents/batch_result.txt");
        // execute program
        env.execute("Flink Batch Java API Skeleton");
        //System.out.println(env.getExecutionPlan());

        long endTime=System.currentTimeMillis();

        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
    }
}
