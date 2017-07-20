package com.tiger.spark.ngix.job;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiger.spark.ngix.model.PhysicsLocation;
import com.tiger.spark.ngix.model.Statics;
import com.tiger.spark.ngix.util.LogFormatter;
import com.tiger.spark.ngix.util.NgixCleanUtil;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import tiger.spark.utils.Contains;
import tiger.spark.utils.DateUtil;

public final class NgixTupleRdd implements Serializable {

	public static final Logger logger = LoggerFactory.getLogger(NgixTupleRdd.class);
	public static String ERROR_STR = "ERROR-LOG=>";
	public static String SUCCES_STR = "SUCCES-LOG=>";
	private static final long serialVersionUID = -6934073434427464726L;
	
	
	/**
	 * 获取全部日志
	 * @param lines
	 * @return
	 */
	public static JavaDStream<String> getAllNgixLogs(JavaDStream<String> lines) {
		JavaDStream<String> ngixLogs = lines
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<String> call(String key) throws Exception {
						String sLog = getNgixLog(key.trim());
						return Arrays.asList(sLog);
					}
				});
		return ngixLogs;
	}
	
	public static String getNgixLog(String rowText) {
		StringBuffer sb = new StringBuffer();
		Statics statics = null;
		try {
			if (rowText != null && StringUtils.isNotEmpty(rowText.trim())) {
				statics = LogFormatter.toNgixLog(rowText);
				if (statics != null) {
					String time = DateUtil.converDateToStr(statics
							.getTimeLocal());
					String ip = statics.getRemoteAddr();
					sb.append(SUCCES_STR).append(",").append(time).append(",")
							.append(ip);
					return sb.toString();
				}
			}
		} catch (Exception ex) {
			logger.error("format Exception :  "+ex.getMessage()
					+"\n" + ex.getStackTrace());
			// ex.printStackTrace();
		}
		sb.setLength(0);
		sb.append(ERROR_STR).append(rowText);
		return sb.toString();
	}

	/**
	 * 获取错误解析不出来的日志
	 * @param ngixLogs
	 * @return
	 */
	public static JavaDStream<String> getErrorNgixLog(
			JavaDStream<String> ngixLogs) {
		// 过滤错误的log日志
		JavaDStream<String> errorLogs = ngixLogs
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String log) throws Exception {
						return log.startsWith(ERROR_STR);
					}
				});
		return errorLogs;
	}

	/**
	 * 获取正确能解析日志
	 * @param ngixLogs
	 * @return
	 */
	public static JavaDStream<String> getSuccesNgixLog(
			JavaDStream<String> ngixLogs) {
		// 过滤错误的log日志
		JavaDStream<String> successLogs = ngixLogs
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String log) throws Exception {
						return log.startsWith(SUCCES_STR);
					}
				});
		return successLogs;
	}

	
	/**
	 * 获取当前时间片正确日志 批量操作的结果
	 * 
	 * @param sucLogs
	 * @param broadcastVar
	 * @return
	 */
	public static JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> getCurrentBatchResult(
			JavaDStream<String> sucLogs,
			final Broadcast<HashMap<String, PhysicsLocation>> broadcastVar) {
		JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> dayHourProviceIpReqs = sucLogs
				.mapToPair(
						new PairFunction<String, Tuple4<String, Integer, Integer, String>, Integer>() {
							private static final long serialVersionUID = 8743854638933034644L;

							@SuppressWarnings({ "unchecked", "rawtypes" })
							@Override
							public Tuple2<Tuple4<String, Integer, Integer, String>, Integer> call(
									String sLog) throws Exception {
								String[] arr = sLog.split(",");
								String day = DateUtil.getDay(arr[1]);
								int hour = DateUtil.getHour(arr[1]);
								Integer province = null;
								if (StringUtils.isNotEmpty(arr[2])) {
									String ipSeg = NgixCleanUtil.getIpSeg(arr[2]);
									HashMap<String, PhysicsLocation> map = broadcastVar
											.getValue();
									PhysicsLocation phy = map.get(ipSeg);
									if (phy != null) {
										province = phy.getPhyProvinceId();
									}
								}
								Tuple4<String, Integer, Integer, String> tuple4 = new Tuple4<String, Integer, Integer, String>(
										day, hour, province, arr[2]);
								return new Tuple2(tuple4, 1);
							}
						}).reduceByKey(
						new Function2<Integer, Integer, Integer>() {
							private static final long serialVersionUID = 2019781068818141430L;

							public Integer call(Integer req0, Integer req1)
									throws Exception {
								return req0 + req1;
							}
						});
		return dayHourProviceIpReqs;
	}
	
	

	/**
	 * 获取历史数据
	 * 
	 * @param dayHourProviceIpReqs
	 * @return
	 */
	public static JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> getNgixHistoryData(
			JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> dayHourProviceIpReqs) {

		JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> historyCount = dayHourProviceIpReqs
				.reduceByKeyAndWindow(
						new Function2<Integer, Integer, Integer>() {
							private static final long serialVersionUID = -6615930246774347329L;

							public Integer call(Integer req0, Integer req1)
									throws Exception {
								return req0 + req1;
							}
						}, new Function2<Integer, Integer, Integer>() {
							private static final long serialVersionUID = 4161367714661213822L;

							public Integer call(Integer req0, Integer req1)
									throws Exception {
								return req0 - req1;
							}
						}, new Duration(Contains.ONE_DAY), new Duration(
								Contains.HALF_AN_MINUTE));
		return historyCount;
	}

	/**
	 * 刷小时结果
	 * 
	 * @param historyData
	 * @param path
	 */
	public static void getHourData(
			JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> historyData,
			String path) {

		JavaPairDStream<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> dayHourProvice = historyData
				.mapToPair(new PairFunction<Tuple2<Tuple4<String, Integer, Integer, String>, Integer>, Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1338119554567086456L;

					@SuppressWarnings({ "rawtypes", "unchecked" })
					public Tuple2<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> call(
							Tuple2<Tuple4<String, Integer, Integer, String>, Integer> t)
							throws Exception {
						Tuple3 t3 = new Tuple3(t._1._1(), t._1._2(), t._1._3());
						Tuple2 t2 = new Tuple2(t._1._4(), t._2);
						return new Tuple2(t3, t2);
					}
				});
		JavaPairDStream<Tuple3<String, Integer, Integer>, Iterable<Tuple2<String, Integer>>> hourData = dayHourProvice
				.groupByKey();
		
		// 第一个结果 day，hour，province，reqs，users
		JavaDStream<Tuple5<String, Integer, Integer, Long, Long>> hourProvinceResult = hourData
				.map(new Function<Tuple2<Tuple3<String, Integer, Integer>, Iterable<Tuple2<String, Integer>>>, Tuple5<String, Integer, Integer, Long, Long>>() {
					private static final long serialVersionUID = -8087761832734929569L;

					@SuppressWarnings({ "unchecked", "rawtypes" })
					@Override
					public Tuple5<String, Integer, Integer, Long, Long> call(
							Tuple2<Tuple3<String, Integer, Integer>, Iterable<Tuple2<String, Integer>>> tupleHour)
							throws Exception {
						
						Tuple3<String, Integer, Integer> key = tupleHour._1;
						Iterator<Tuple2<String, Integer>> list = tupleHour._2
								.iterator();
						long userSum = 0;
						long reqSum = 0;
						Tuple2<String, Integer> ipReq = null;
						while (list.hasNext()) {
							userSum++;// 每一个ip_reqs记录代表一个用户
							ipReq = list.next();
							reqSum += ipReq._2;
						}
						return new Tuple5(key._1(), key._2(), key._3(), reqSum,
								userSum);
					}
				});
		hourProvinceResult.dstream().saveAsTextFiles(path + "EveryHourProRes",
				null);

		// 第二个结果 day，hour，reqs，users
		JavaDStream<Tuple4<String, Integer, Long, Long>> hourResult = hourProvinceResult
				.mapToPair(
						new PairFunction<Tuple5<String, Integer, Integer, Long, Long>, Tuple2<String, Integer>, Tuple3<Integer, Long, Long>>() {
							private static final long serialVersionUID = -518414000789757532L;

							@SuppressWarnings({ "unchecked", "rawtypes" })
							public Tuple2<Tuple2<String, Integer>, Tuple3<Integer, Long, Long>> call(
									Tuple5<String, Integer, Integer, Long, Long> res)
									throws Exception {

								Tuple2<String, Integer> t2 = new Tuple2(res
										._1(), res._2());
								Tuple3<Integer, Long, Long> t3 = new Tuple3(res
										._3(), res._4(), res._5());
								return new Tuple2(t2, t3);
							}
						})
				.groupByKey()
				.map(new Function<Tuple2<Tuple2<String, Integer>, Iterable<Tuple3<Integer, Long, Long>>>, Tuple4<String, Integer, Long, Long>>() {
					private static final long serialVersionUID = -2542271990772540755L;

					@SuppressWarnings({ "unchecked", "rawtypes" })
					@Override
					public Tuple4<String, Integer, Long, Long> call(
							Tuple2<Tuple2<String, Integer>, Iterable<Tuple3<Integer, Long, Long>>> tupleHour)
							throws Exception {

						Tuple2<String, Integer> key = tupleHour._1;
						Iterator<Tuple3<Integer, Long, Long>> list = tupleHour._2
								.iterator();
						long userSum = 0;
						long reqSum = 0;
						Tuple3<Integer, Long, Long> ipReq = null;
						while (list.hasNext()) {
							ipReq = list.next();
							reqSum += ipReq._2();
							userSum += ipReq._3();
						}
						return new Tuple4(key._1(), key._2(), reqSum, userSum);
					}
				});

		hourResult.dstream().saveAsTextFiles(path + "EveryHourRes", null);
	}

	/**
	 * 刷天结果
	 * 
	 * @param historyData
	 * @param path
	 */
	public static void getDayData(
			JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> historyData,
			String path) {
		JavaDStream<Tuple4<String, Integer, Long, Long>> dayProRes = historyData
				.mapToPair(
						// Tuple4<day, hour, pro, ip>, reqs> => Tuple3<day,pro, ip>, reqs>
						new PairFunction<Tuple2<Tuple4<String, Integer, Integer, String>, Integer>, Tuple3<String, Integer, String>, Integer>() {
							private static final long serialVersionUID = -1707718434139731749L;

							@SuppressWarnings({ "unchecked", "rawtypes" })
							@Override
							public Tuple2<Tuple3<String, Integer, String>, Integer> call(
									Tuple2<Tuple4<String, Integer, Integer, String>, Integer> t)
									throws Exception {
								Tuple3<String, Integer, String> t3 = new Tuple3(t._1._1(),t._1._3(),t._1._4());
								Integer reqs = t._2();
								return new Tuple2(t3,reqs);
							}
						})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					// Tuple3<day, pro, ip>, reqs>
					private static final long serialVersionUID = -8342944927130449694L;

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
				})
				.mapToPair(
						new PairFunction<Tuple2<Tuple3<String, Integer, String>, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
							private static final long serialVersionUID = -6790979127642177399L;

							// Tuple3<day, pro, ip>, reqs> ==> Tuple2<Tuple2<day,pro>,Tuple2<ip,reqs>>
							@SuppressWarnings({ "unchecked", "rawtypes" })
							@Override
							public Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> call(
									Tuple2<Tuple3<String, Integer, String>, Integer> t)
									throws Exception {
								Tuple2<String, Integer> dayPro = new Tuple2(
										t._1._1(), t._1._2());
								Tuple2<String, Integer> ipReqs = new Tuple2(
										t._1._3(), t._2());
								return new Tuple2(dayPro, ipReqs);
							}
						})
				.groupByKey()
				.map(new Function<Tuple2<Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>>, Tuple4<String, Integer, Long, Long>>() {
					private static final long serialVersionUID = -4335079814433918715L;

					@SuppressWarnings({ "unchecked", "rawtypes" })
					@Override
					public Tuple4<String, Integer, Long, Long> call(
							Tuple2<Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>> dayRes)
							throws Exception {

						Tuple2<String, Integer> key = dayRes._1;
						Iterator<Tuple2<String, Integer>> list = dayRes._2
								.iterator();
						long userSum = 0;
						long reqSum = 0;
						Tuple2<String, Integer> ipReq = null;
						while (list.hasNext()) {
							userSum++;// 每一个ip_reqs记录代表一个用户
							ipReq = list.next();
							reqSum += ipReq._2;
						}
						return new Tuple4(key._1(), key._2(), reqSum, userSum);
					}
				});
		dayProRes.dstream().saveAsTextFiles(path + "EveryDayProRes", null);

		// 第二个结果 day，pro，reqs，users
		JavaDStream<Tuple3<String, Long, Long>> dayRes = dayProRes
				.mapToPair(
				// day pro,req,user
						new PairFunction<Tuple4<String, Integer, Long, Long>, String, Tuple3<Integer, Long, Long>>() {
							private static final long serialVersionUID = 59480056683164430L;

							@SuppressWarnings({ "unchecked", "rawtypes" })
							public Tuple2<String, Tuple3<Integer, Long, Long>> call(
									Tuple4<String, Integer, Long, Long> tuple)
									throws Exception {

								String key = tuple._1();
								Tuple3<Integer, Long, Long> t3 = new Tuple3(
										tuple._2(), tuple._3(), tuple._4());
								return new Tuple2(key, t3);
							}
						})
				.groupByKey()
				.map(new Function<Tuple2<String, Iterable<Tuple3<Integer, Long, Long>>>, Tuple3<String, Long, Long>>() {
					private static final long serialVersionUID = -7721175886615631784L;

					@SuppressWarnings({ "unchecked", "rawtypes" })
					@Override
					public Tuple3<String, Long, Long> call(
							Tuple2<String, Iterable<Tuple3<Integer, Long, Long>>> dayRes)
							throws Exception {

						String key = dayRes._1();
						Iterator<Tuple3<Integer, Long, Long>> list = dayRes._2
								.iterator();
						long userSum = 0;
						long reqSum = 0;
						Tuple3<Integer, Long, Long> ipReq = null;
						while (list.hasNext()) {
							ipReq = list.next();
							reqSum += ipReq._2();
							userSum += ipReq._3();
						}
						return new Tuple3(key, reqSum, userSum);
					}
				});
		dayRes.dstream().saveAsTextFiles(path + "EveryDayRes", null);
	}

}