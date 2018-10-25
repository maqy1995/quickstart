package streamTransformationTest;

import javaStream.Student;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MaxByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> i = env.fromElements(
                new Student(20160001, "孔明", 5, 1, "土木工程", "cccc"),
                new Student(20160002, "伯约", 9, 2, "信息安全", "武汉大学"),
                new Student(20160003, "玄德", 6, 3, "经济管理", "武汉大学"),
                new Student(20160004, "云长", 8, 2, "信息安全", "武汉大学"),
                new Student(20161001, "翼德", 7, 2, "机械与自动化", "华中科技大学"),
                new Student(20161002, "元直", 10, 4, "土木工程", "华中科技大学"),
                new Student(20161003, "奉孝", 5, 4, "计算机科学", "华中科技大学"),
                new Student(20162001, "仲谋", 1, 3, "土木工程", "浙江大学"),
                new Student(20162002, "鲁肃", 10, 4, "计算机科学", "浙江大学"),
                new Student(20163001, "丁奉", 6, 5, "土木工程", "南京大学")
        );

        env.setParallelism(1);
        KeyedStream<Student, Integer> i1 = i.keyBy(new org.apache.flink.api.java.functions.KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return 1;
            }
        });

        //i1.print();
        //使用max的话，每次都是输出20160001, "孔明", 5, 1, "土木工程", "cccc"),只是age会变，用maxBy的话则可以得到正确的结果
        DataStream<Student> result = i1.max("age");

        result.print();

        env.execute();
    }
}

