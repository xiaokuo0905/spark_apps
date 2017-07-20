package tiger.spark.example;

import java.util.Arrays;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.tiger.spark.ngix.util.LogFormatter;

import scala.Tuple2;

public class NgixUtil {

	//private static final Pattern SPACE = Pattern.compile(" ");
	
	@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
	public static JavaRDD contText(JavaRDD lines){
		JavaRDD text = lines.flatMap(new FlatMapFunction() {
			public Iterable<String> call(Object s) throws Exception {
				if (s != null && StringUtils.isNotEmpty(s.toString().trim())) {
					String txt = String.valueOf(s);
					Matcher m = LogFormatter.extractStats(txt);
					return Arrays.asList(m.group(2));
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
		return result;
	}
	
	
	@SuppressWarnings({ "unused", "rawtypes", "serial" })
	public static void getFiled(JavaRDD<String> lines){
		String[] sArr = null;
		FlatMapFunction f = new FlatMapFunction() {
			public Iterable<String> call(Object s) throws Exception {
				if (s != null && StringUtils.isNotEmpty(s.toString().trim())) {
					String txt = String.valueOf(s);
				}
				return null;
			}
		};
//		JavaRDD ips = lines.flatMap();
//		System.out.println(sArr[0]);
//		System.out.println(sArr[1]);
//		System.out.println(sArr[2]);
	}
}
