package tiger.spark.example;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public final class JavaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		args = new String[2];
		args[0] = "/data/ngix/fw_cobra_access.log";
		args[1] = "/data/ngix/ngix-out";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD lines = ctx.textFile(args[0], 1);
		JavaRDD ips = lines.flatMap(new FlatMapFunction() {
			public Iterable<String> call(Object s) throws Exception {
				if(s!=null && StringUtils.isNotEmpty(s.toString().trim())){
					String _key = String.valueOf(s);
					System.out.println(_key);
					return Arrays.asList(JavaWordCount.SPACE.split(_key));
				}
				return null;
			}
		});
		JavaPairRDD ones = ips.mapToPair(new PairFunction() {
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
		counts.collect();
		counts.saveAsTextFile(args[1]);
		ctx.stop();
	}
}