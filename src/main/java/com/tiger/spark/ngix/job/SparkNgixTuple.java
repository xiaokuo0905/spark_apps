package com.tiger.spark.ngix.job;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.tiger.spark.ngix.load.data.GenerateMap;
import com.tiger.spark.ngix.model.PhysicsLocation;

import scala.Tuple4;
import tiger.spark.utils.Contains;

public final class SparkNgixTuple {

	public static Broadcast<HashMap<String, PhysicsLocation>> broadcastVar = null;

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		// 参数
		// args = new String[2];
		// args[0] = Contains.HADOOP_URL + "/data/ngixlog";
		// args[1] = Contains.HADOOP_URL + "/data/out/ngix-log/";

		SparkConf sparkConf = new SparkConf()
				.setAppName("ngix-log-user-report");// .setMaster("local");
		final JavaStreamingContext sc = new JavaStreamingContext(sparkConf,
				new Duration(Contains.HALF_AN_MINUTE));

		sc.checkpoint(args[1] + "checkpoint");
		// 加载广播变量
		if (broadcastVar == null) {
			HashMap<String, PhysicsLocation> map = GenerateMap
					.getIpPointPhyLocationMap();
			broadcastVar = sc.sc().broadcast(map);
		}
		// 目前采用文件读取的方式
		JavaDStream<String> lines = sc.textFileStream(args[0]);
		// 获取所有的NgixLog
		JavaDStream<String> ngixLogs = NgixTupleRdd.getAllNgixLogs(lines);
		// 过滤错误的log日志
		JavaDStream<String> errorLogs = NgixTupleRdd.getErrorNgixLog(ngixLogs);
		errorLogs.dstream().saveAsTextFiles(args[1] + "ngix-error-logs", null);
		// 过滤正确的log日志
		JavaDStream<String> sucLogs = NgixTupleRdd.getSuccesNgixLog(ngixLogs);

		// 获取当前时间片执行的结果
		JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> dayHourProviceIpReqs = NgixTupleRdd
				.getCurrentBatchResult(sucLogs, broadcastVar);
		// 当前时间片得到的中间结果和历史时间片得到的结果整合
		JavaPairDStream<Tuple4<String, Integer, Integer, String>, Integer> historyData = NgixTupleRdd
				.getNgixHistoryData(dayHourProviceIpReqs);
		// 输出整合结果到HDFS验证中间结果
		// historyData.dstream().saveAsTextFiles(args[1] + "historyCount",
		// null);

		// 根据历史数据刷小时结果
		NgixTupleRdd.getHourData(historyData, args[1]);

		// 根据历史数据刷天结果
		NgixTupleRdd.getDayData(historyData, args[1]);

		sc.start();
		sc.awaitTermination();

	}// main

	@SuppressWarnings("deprecation")
	public static Broadcast<HashMap<String, PhysicsLocation>> getRemoteAddrBroadcast(
			JavaStreamingContext sc) {
		try {
			HashMap<String, PhysicsLocation> map = GenerateMap
					.getIpPointPhyLocationMap();
			broadcastVar = sc.sc().broadcast(map);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return broadcastVar;
	}
}