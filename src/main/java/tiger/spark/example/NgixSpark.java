package tiger.spark.example;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public final class NgixSpark {
	@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
	public static void main(String[] args) throws Exception {
		args = new String[3];
		args[0] = "hdfs://localhost:19000/data/fw_cobra_access.log";
		args[1] = "/data/ngix/ngix-out/ips";
		args[2] = "/data/ngix/ngix-out/host";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
				.setMaster("spark://localhost:7077");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		NgixUtil.getFiled(lines);
		// 输出 ip
		JavaRDD text = lines.flatMap(new FlatMapFunction() {
			public Iterable<String> call(Object s) throws Exception {
				if (s != null && StringUtils.isNotEmpty(s.toString().trim())) {
					String txt = String.valueOf(s);
					return Arrays.asList(txt);
				}
				return null;
			}
		});
		JavaPairRDD ones = text.mapToPair(new PairFunction() {
			public Tuple2<String, Integer> call(Object s) throws Exception {
				String _s = String.valueOf(s);
				return new Tuple2(_s, Integer.valueOf(1));
			}
		});
		JavaPairRDD counts = ones.reduceByKey(new Function2() {
			public Integer call(Object one, Object two) {
				return Integer.valueOf(((Integer) one).intValue()
						+ ((Integer) two).intValue());
			}
		});
		JavaRDD result = counts.flatMap(new FlatMapFunction() {
			public Iterable<String> call(Object s) throws Exception {
				String txt = String.valueOf(s);
				txt = txt.replace("(", "");
				txt = txt.replace(")", "");
				txt = txt.replace(",", "|");
				return Arrays.asList(txt);
			}
		});
		result.saveAsTextFile(args[1]);
		ctx.stop();
	}
}