package org.myorg.quickstart;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestIterate {

    public static void main(String[] args) throws Exception{
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setBufferTimeout(100);


        //生成1-4的数，因为电脑是6核心的，所以会随机给4个核去跑，每个核跑一个数，每次减1,直到为0。
        DataStream<Long> someIntegers = env.generateSequence(0, 5);

        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1;
            }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });


        stillGreaterThanZero.print();
        lessThanZero.print();

        env.execute();
    }
}
