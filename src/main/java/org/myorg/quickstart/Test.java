package org.myorg.quickstart;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class Test {
    public static void test() throws Exception {
        final ExecutionEnvironment env1 = ExecutionEnvironment.getExecutionEnvironment();

        env1.setParallelism(1);
        // get input data
        DataSet<String> text1 = env1.fromElements(
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,"
        );

        DataSet<Tuple2<String, Integer>> counts1 =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text1.flatMap(new WordCount.LineSplitter())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // execute and print result
        counts1.print();
    }
}
