package org.myorg.quickstart;

/**
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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class WordCount {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);
		// get input data
//		DataSet<String> text = env.fromElements(
//				"To be, or not to be,--that is the question:--",
//				"Whether 'tis nobler in the mind to suffer",
//				"The slings and arrows of outrageous fortune",
//				"Or to take arms against a sea of troubles,"
//				);
		//DataSet<String> text = env.readTextFile("D:\\批流融合大数据\\mobiledata\\gaming_data1.csv");
		//text.writeAsText("C:\\Users\\Administrator\\Desktop\\output\\out1");

		String input="/home/maqy/桌面/out/test";
		String output="/home/maqy/桌面/out/out1";
		int parallism = 1;
		if(args.length == 3){
			input = args[0];
			output = args[1];
			parallism = Integer.parseInt(args[2]);
		}

		env.setParallelism(parallism);

		DataSet<Tuple2<String, Integer>> counts = env.readTextFile(input)
				// split up the lines in pairs (2-tuples) containing: (word,1)
				.flatMap(new LineSplitter())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// execute and print result
		//counts.writeAsText(output);
		counts.print();
		//执行程序
		env.execute("batchJob~~~~~~~~~~~~~");

	}


	//
	// 	User Functions
	//

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
