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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * <p>You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/quickstart-0.1.jar
 * From the CLI you can then run
 * 		./bin/flink run -c org.myorg.quickstart.StreamingJob target/quickstart-0.1.jar
 *
 * <p>For more information on the CLI see:
 *
 * <p>http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {
	public static final class filterMa implements FilterFunction<Tuple2<String, Integer>>{
		@Override
		public boolean filter(Tuple2<String,Integer> stringIntegerTuple2) throws Exception{
			if(stringIntegerTuple2.getField(0).equals("ma")){
				return true;
			}
			return false;
		}
	}
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment



		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<String> text=env.socketTextStream("localhost",9000);

		DataStream<Tuple2<String,Integer>> countsFilter=text.flatMap(new SocketTextStreamWordCount.LineSplitter())
				.filter(new filterMa())
				.keyBy(0)
				.sum(1);

		countsFilter.print();


		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
