package com.tiger.spark.ngix.job;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;
import scala.Tuple4;

public final class NgixHourRdd {

	// 定义24个小时的RDD
	@SuppressWarnings("unchecked")
	public static JavaPairDStream<String, Integer>[] hourRdd = new JavaPairDStream[24];

	private static int hour = 0;

	/**
	 * 刷 小时 结果
	 * 
	 * @param hourRddCathe
	 * @param path
	 */
	public static void getNgixHourResult(
			JavaPairDStream<String, Integer>[] hourRdd, String path) {

		if (hourRdd != null && hourRdd.length > 0) {
			for (int n = 0; n < hourRdd.length; n++) {
				hour = n;
				if (hourRdd[n] != null) {
					// (ip_province,reqs)==>(province,ip_reqs)
					JavaPairDStream<String, String> hourIpReqOnes = hourRdd[n]
							.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
								private static final long serialVersionUID = 2855208054537234571L;

								@SuppressWarnings({ "unchecked", "rawtypes" })
								public Tuple2<String, String> call(
										Tuple2<String, Integer> key)
										throws Exception {
									String ip = key._1.split("_")[0];
									String province = key._1.split("_")[1];
									return new Tuple2(province, ip + "_"
											+ key._2);
								}
							});
					// (province,ip_reqs)==>(province,ip_reqs%ip_reqs%ip_reqs)
					JavaPairDStream<String, String> hourIpReqs = hourIpReqOnes
							.reduceByKey(new Function2<String, String, String>() {
								private static final long serialVersionUID = 5123444964065556356L;

								public String call(String ip_req0,
										String ip_req1) throws Exception {
									return ip_req0 + "%" + ip_req1;
								}
							});
					// 统计 (province,ip_reqs%ip_reqs%ip_reqs)
					// 用户数：ip_reqs%ip_reqs%ip_reqs 以%分割后的长度
					// 请求数：ip_reqs%ip_reqs%ip_reqs 里的 ip_reqs ： reqs的相加数量。
					hourIpReqs
							.map(new Function<Tuple2<String, String>, Tuple4<String, String, Integer, Integer>>() {
								private static final long serialVersionUID = 2192954965242636261L;

								@SuppressWarnings({ "unchecked", "rawtypes" })
								public Tuple4<String, String, Integer, Integer> call(
										Tuple2<String, String> key)
										throws Exception {
									String pro = key._1;
									String[] ipReqArray = key._2.split("%");
									long userSum = ipReqArray.length;// 得到用户数
									long reqSum = 0;
									for (String ipReqs : ipReqArray) {
										int reqNum = Integer.parseInt(ipReqs
												.split("_")[1]);
										reqSum += reqNum;
									}
									return new Tuple4("第" + hour + "小时", pro
											+ "省", "用户数：" + userSum, "请求数："
											+ reqSum);
								}
							})
							.dstream()
							.saveAsTextFiles(path + hour + "-HOUR-result", null);
				}

			}
		}
	}

	/**
	 * 刷天结果
	 * 
	 * @param hourRdd
	 * @param path
	 */
	public static void getNgixDayResult(
			JavaPairDStream<String, Integer>[] hourRdd, String path) {

		if (hourRdd != null && hourRdd.length > 0) {
			// 所有集合整合
			JavaPairDStream<String, Integer> uninRdd = null;
			for (int n = 0; n < hourRdd.length; n++) {
				if (hourRdd[n] != null) {
					if (uninRdd == null) {
						uninRdd = hourRdd[n];
					} else {
						uninRdd = uninRdd.union(hourRdd[n]);
					}
				}
			}
			// 执行reduceby 合并
			JavaPairDStream<String, Integer> uninhourProvince = uninRdd
					.reduceByKey(new Function2<Integer, Integer, Integer>() {
						private static final long serialVersionUID = -1812904207906115094L;

						public Integer call(Integer req0, Integer req1)
								throws Exception {
							return req0 + req1;
						}
					});

			// (ip_province,reqs)==>(province,ip_reqs)
			JavaPairDStream<String, String> uninHourIpReqOnes = uninhourProvince
					.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
						private static final long serialVersionUID = 2855208054537234571L;

						@SuppressWarnings({ "unchecked", "rawtypes" })
						public Tuple2<String, String> call(
								Tuple2<String, Integer> key) throws Exception {
							String ip = key._1.split("_")[0];
							String province = key._1.split("_")[1];
							return new Tuple2(province, ip + "_" + key._2);
						}
					});
			// (province,ip_reqs)==>(province,ip_reqs%ip_reqs%ip_reqs)
			JavaPairDStream<String, String> uninHourIpReqs = uninHourIpReqOnes
					.reduceByKey(new Function2<String, String, String>() {
						private static final long serialVersionUID = 5123444964065556356L;

						public String call(String ip_req0, String ip_req1)
								throws Exception {
							return ip_req0 + "%" + ip_req1;
						}
					});
			// 统计 (province,ip_reqs%ip_reqs%ip_reqs)
			// 用户数：ip_reqs%ip_reqs%ip_reqs 以%分割后的长度
			// 请求数：ip_reqs%ip_reqs%ip_reqs 里的 ip_reqs ： reqs的相加数量。
			uninHourIpReqs
					.map(new Function<Tuple2<String, String>, Tuple4<String, String, Integer, Integer>>() {
						private static final long serialVersionUID = 2192954965242636261L;

						@SuppressWarnings({ "unchecked", "rawtypes" })
						public Tuple4<String, String, Integer, Integer> call(
								Tuple2<String, String> key) throws Exception {
							String pro = key._1;
							String[] ipReqArray = key._2.split("%");
							long userSum = ipReqArray.length;// 得到用户数
							long reqSum = 0;
							for (String ipReqs : ipReqArray) {
								int reqNum = Integer.parseInt(ipReqs.split("_")[1]);
								reqSum += reqNum;
							}
							return new Tuple4("汇总一天的 -- ", pro + "省", "用户数："
									+ userSum, "请求数：" + reqSum);
						}
					}).dstream().saveAsTextFiles(path + "DAY-result", null);

		}
	}
}