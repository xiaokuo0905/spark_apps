package tiger.spark.example;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

import java.util.regex.Pattern;

public final class JavaNetworkWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	public static final Pattern nginxLogRegex = Pattern
			.compile("^([\\d.]+) \\S+ (\\S+) \\S+ \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\" (\\S+)");

	public static void main(String[] args) {
		args = new String[2];
		args[0]="localhost";
		args[1]="7077";
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				new Duration(1000));

		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0],
				Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 5500281825521262609L;

					public Iterable<String> call(String x) {
						return Lists.newArrayList(SPACE.split(x));
					}
				});
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = -6474078087493197942L;

					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1654398362664179813L;

			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		wordCounts.print();
		wordCounts.saveAsHadoopFiles("aaa", "txt");
		ssc.start();
		ssc.awaitTermination();
	}
}
