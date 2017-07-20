package com.tiger.spark.ngix.job;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.tiger.spark.ngix.load.data.GenerateMap;
import com.tiger.spark.ngix.model.PhysicsLocation;
import com.tiger.spark.ngix.model.Statics;
import com.tiger.spark.ngix.util.NgixCleanUtil;
import com.tiger.spark.ngix.util.LogFormatter;

import scala.Tuple2;
import tiger.spark.utils.Contains;
import tiger.spark.utils.DateUtil;

public final class SparkNgixHour {

	// 定义24个小时的RDD
	@SuppressWarnings("unchecked")
	private static JavaPairDStream<String, Integer>[] hourRdd = new JavaPairDStream[24];
	private static int i = 0;// 某小时【0-23】
	private static String time = null;// 日期

	private static Broadcast<HashMap<String, PhysicsLocation>> broadcastVar = null;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		// 参数
		args = new String[2];
		args[0] = Contains.HADOOP_URL + "/data/ngixlog";
		args[1] = Contains.HADOOP_URL + "/data/out/ngix-log/";

		SparkConf sparkConf = new SparkConf()
				.setAppName("ngix-log-user-report").setMaster(
						Contains.SPARK_LOCAL);
		final JavaStreamingContext sc = new JavaStreamingContext(sparkConf,
				new Duration(Contains.HALF_AN_MINUTE));

		// 加载广播变量
		if (broadcastVar == null) {
			getRemoteAddrBroadcast(sc);
		}
		// 目前采用文件读取的方式
		JavaDStream lines = sc.textFileStream(args[0]);
		// 获取行内容RDD
		JavaDStream<String> clienLine = lines
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 5500281825521262609L;

					public Iterable<String> call(String x) {
						try {
							if (x != null && StringUtils.isNotEmpty(x)) {
								String key = getNgixKey(x.trim(), broadcastVar);
								return Arrays.asList(key);
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}
						return null;
					}
				});
		clienLine.dstream().saveAsTextFiles(args[1] + "clean-line", null);
		// 分发给每个小时 把24个小时的数据填充起来
		// 把24个小时的数据填充起来
//		for (i = 0; i < 24; i++) {
			// 第一步：按时间过滤，得第i小时的全部数据
			JavaDStream<String> hourProvinceLine = clienLine
					.filter(new Function<String, Boolean>() {
						private static final long serialVersionUID = 3868982362600941115L;

						public Boolean call(String key) throws Exception {
							// 取得日期
							time = key.split(",")[0];
							return DateUtil.getHour(time) == i;
						}
					});
			JavaPairDStream<String, Integer> hourIpProvinceOnes = hourProvinceLine
					.mapToPair(new PairFunction<String, String, Integer>() {
						private static final long serialVersionUID = 4033523658268489688L;

						public Tuple2<String, Integer> call(String key)
								throws Exception {
							String province = key.split(",")[2];
							String ip = key.split(",")[1];
							String ip_pro = ip + "_" + province;
							return new Tuple2(ip_pro, 1);
						}

					});
			JavaPairDStream<String, Integer> hourProvince = hourIpProvinceOnes
					.reduceByKey(new Function2<Integer, Integer, Integer>() {
						private static final long serialVersionUID = -1812904207906115094L;

						public Integer call(Integer req0, Integer req1)
								throws Exception {
							return req0 + req1;
						}
					});
			// hourProvince (ip_pro,reqs)
			// 得到的时当前小时的完整数据(所谓完整，当前执行的job执行的新产生的file的结果和历史执行的file所catch的结果合并)
			if (hourRdd[i] == null) {
				hourRdd[i] = hourProvince;
			} else {
				hourRdd[i] = hourRdd[i].union(hourProvince);
				// 写这种代码的关键是返回值
				hourRdd[i] = hourRdd[i]
						.reduceByKey(new Function2<Integer, Integer, Integer>() {
							private static final long serialVersionUID = 4435669991032548104L;

							public Integer call(Integer req0, Integer req1)
									throws Exception {
								return req0 + req1;
							}
						});
			}// else
				// 先输出结果看一下
			hourRdd[i].dstream().saveAsTextFiles(args[1] + i + "-HOUR", null);
			// 缓存每小时的数据，必须有返回值。。用来做天得合并操作中用到。
			hourRdd[i].cache();

//		}

		// 刷小时结果
		NgixHourRdd.getNgixHourResult(hourRdd, args[1]);
		// 刷天得结果
		NgixHourRdd.getNgixDayResult(hourRdd, args[1]);

		sc.start();
		sc.awaitTermination();

	}// main

	@SuppressWarnings("deprecation")
	public static void getRemoteAddrBroadcast(JavaStreamingContext sc) {
		try {
			HashMap<String, PhysicsLocation> map = GenerateMap
					.getIpPointPhyLocationMap();
			broadcastVar = sc.sc().broadcast(map);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取要统计的Key
	 * 
	 * @param rowText
	 * @param broadcastVar
	 * @return
	 */
	public static String getNgixKey(String rowText,
			Broadcast<HashMap<String, PhysicsLocation>> broadcastVar) {
		Statics statics = LogFormatter.toNgixLog(rowText);
		HashMap<String, PhysicsLocation> map = broadcastVar.getValue();
		String time = DateUtil.converDateToStr(statics.getTimeLocal());
		String ip = statics.getRemoteAddr();
		String ipSeg = NgixCleanUtil.getIpSeg(statics.getRemoteAddr());
		PhysicsLocation phy = map.get(ipSeg);
		String province = "NULL";
		String city = "NULL";
		if (phy != null) {
			province = String.valueOf(phy.getPhyProvinceId());
			city = String.valueOf(phy.getPhyCityId());
		}
		String key = time + "," + ip + "," + province + "," + city;
		return key;
	}
}