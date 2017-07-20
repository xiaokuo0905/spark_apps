package com.tiger.spark.ngix.job;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.tiger.spark.ngix.load.data.GenerateMap;
import com.tiger.spark.ngix.model.PhysicsLocation;
import com.tiger.spark.ngix.model.Statics;
import com.tiger.spark.ngix.util.NgixCleanUtil;
import com.tiger.spark.ngix.util.LogFormatter;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import tiger.spark.utils.Contains;
import tiger.spark.utils.DateUtil;

public final class SparkNgix {
	private static Broadcast<HashMap<String, PhysicsLocation>> broadcastVar = null;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		// args = new String[3];
		// args[0] = Contains.HADOOP_URL + "/data/ngixlog";
		// args[1] = Contains.HADOOP_URL + "/data/out/ngix-log/";

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
//潜在危险，文件读取可能有大量空Dstream存在。。
		JavaDStream lines = sc.textFileStream(args[0]);
		// 获取行内容RDD
		JavaDStream<String> clienLine = lines
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 5500281825521262609L;

					public Iterable<String> call(String x) {
						try {
							if (StringUtils.isNotEmpty(x)) {
								String key = getNgixKey(x, broadcastVar);
								return Arrays.asList(key);
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}
						return null;
					}
				});
		clienLine.dstream().saveAsTextFiles(args[1] + "clean-line", "txt");
		
		//words(ip,time,province,city)
//----Tuple－n－－版----
//----1----把words转化为(ip_pro_day_hour,1)
		clienLine.mapToPair(new PairFunction<String,Tuple4<String,String,String,Integer>,Integer>(){
			public Tuple2<Tuple4<String, String, String, Integer>, Integer> call(
					String key) throws Exception {
				String time = key.split(",")[0];
				int hour = DateUtil.getHour(time);
				String day = DateUtil.strToDate(time);
				String pro = key.split(",")[2];
				String ip = key.split(",")[1];
				return new Tuple2(new Tuple4(ip,pro,day,hour),1);
			}
			
		})
//----2----把(ip_pro_day_hour,1)转化为(ip_pro_day_hour,reqs)
//		----这里是关键，做多大范围的归并(按时间分片统计24个小时的)，多长时间归并一次（按时间分片1个小时刷一次结果）。
//		----这里做的是增量归并。。
		.reduceByKeyAndWindow(new Function2<Integer,Integer,Integer>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = -1338072842936303636L;

			public Integer call(Integer req0, Integer req1) throws Exception {
				return req0 + req1;
			}
			
		}, new Function2<Integer,Integer,Integer>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = -3611623711251061460L;

			public Integer call(Integer req0, Integer req1) throws Exception {
				return req0 - req1;
			}
			
		}, new Duration(24 * 60 * 60 * 1000), new Duration(1 * 60 * 60 * 1000))
//	把刷出的结果做整理，然后统计输出。。下面的操作是完成，统计工作。	
//----3----把(ip_pro_day_hour,reqs)转化为(pro_day_hour,ip_reqs)
		.mapToPair(new PairFunction<Tuple2<Tuple4<String,String,String,Integer>,Integer>,Tuple3<String,String,Integer>,Tuple2<String,Integer>>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = -6562234525930687087L;

			public Tuple2<Tuple3<String, String, Integer>, Tuple2<String, Integer>> call(
					Tuple2<Tuple4<String, String, String, Integer>, Integer> item)
					throws Exception {
//				String ip = item._1.split("_")[0];
				String ip = item._1._1();
//				String pro_day_hour = item._1.substring(item._1.indexOf("_") + 1);
				String pro = item._1._2();
				String day = item._1._3();
				int hour = item._1._4();
				int reqs = item._2();
				return new Tuple2(new Tuple3(pro,day,hour),new Tuple2(ip,reqs));
			}
			
		})
//----4----把(pro_day_hour,ip_reqs)转化为(pro_day_hour,ip_reqs%List)。===Seq(ip_reqs)
//		得到按省份，每天每小时的。
		.groupByKey()
		.map(new Function<Tuple2<Tuple3<String,String,Integer>,Iterable<Tuple2<String,Integer>>>,Tuple5<String,Integer,String,Integer,Integer>>(){
			public Tuple5<String,Integer,String,Integer,Integer> call(
					Tuple2<Tuple3<String, String, Integer>, Iterable<Tuple2<String, Integer>>> item)
					throws Exception {
				String pro = item._1._1();
				String day = item._1._2();
				int   hour = item._1._3();
				Iterator<Tuple2<String, Integer>> ip_reqsList = item._2.iterator();
				int userSum = 0;
				int reqSum = 0;
				Tuple2<String, Integer> ip_reqs = null;
				while(ip_reqsList.hasNext()){
					userSum ++;//每一个ip_reqs记录代表一个用户
					ip_reqs = ip_reqsList.next();
					reqSum += ip_reqs._2;
				}
				
				
				return new Tuple5(day,hour,pro,userSum,reqSum);
			}
			
		})
//每小时刷一次结果。把结果(day,hour,pro,userSum,reqSum)以增量变化的结果输出保存到文件。
		.dstream().saveAsTextFiles(args[1] + "EveryHourOfDay", "txt");
		//而每天的结果可以在数据库中，汇集小时的结果得到，没有必要在这里计算。。
		
		
		
//-----------从这里开始处理------原版改进的--------两个版本都可用（String版和Tuple版）-----------		
//--------郭流芳----每小时的------------		
		//新版第一步：把words(ip,time,province,city)转化为(ip_pro_day_hour,1)
		clienLine.mapToPair(new PairFunction<String, String, Integer>() {
					// step one:(time,ip,province,city)==>(ip_pro_day_hour,1)
					private static final long serialVersionUID = -5602411385486750514L;

					public Tuple2<String, Integer> call(String key)
							throws Exception {
						String time = key.split(",")[0];
						int hour = DateUtil.getHour(time);
						String day = DateUtil.getDay(time);
						String pro = key.split(",")[2];
						String ip = key.split(",")[1];
						String ip_pro_day_hour = ip + "_" + pro + "_" + day
								+ "_" + hour;
						return new Tuple2(ip_pro_day_hour, 1);
					}

				})

		//新版第二步：把(ip_pro_day_hour,1)转化为(ip_pro_day_hour,reqs)
//------这里是改进的地方。。开始动态计算。。每天每小时每省的结果。
		.reduceByKeyAndWindow(new Function2<Integer,Integer,Integer>(){
			public Integer call(Integer req0, Integer req1) throws Exception {
				return req0 + req1;
			}
			
		}, new Function2<Integer,Integer,Integer>(){
			public Integer call(Integer req0, Integer req1) throws Exception {
				return req0 - req1;
			}
			
		}, new Duration(24 * 60 * 60 * 1000), new Duration(1 * 60 * 60 * 1000))
		//新版第三步：把(ip_pro_day_hour,reqs)转化为(pro_day_hour,ip_reqs)
		.mapToPair(new PairFunction<Tuple2<String,Integer>,String,String>(){
			public Tuple2<String, String> call(Tuple2<String, Integer> item)
					throws Exception {
				String ip = item._1.split("_")[0];
				String pro_day_hour = item._1.substring(item._1.indexOf("_") + 1);
				int reqs = item._2;
				return new Tuple2(pro_day_hour,ip + "_" + reqs);
			}
			
		})
		//新版第四步：把(pro_day_hour,ip_reqs)转化为(pro_day_hour,ip_reqs%List)。
		//相当于Tuple中的groupByKey()		
	.reduceByKey(new Function2<String,String,String>(){
		public String call(String ip_reqs0, String ip_reqs1) throws Exception {
			return ip_reqs0 + "%" + ip_reqs1;
		}
		
	})
		//新版第五步：把(pro_day_hour,ip_reqs%List)做统计并返回最终结果
		.map(new Function<Tuple2<String,String>,Tuple4<String,String,Integer,Integer>>(){
			public Tuple4<String, String, Integer, Integer> call(
					Tuple2<String, String> item) throws Exception {
				String day_hour = item._1.substring(item._1.indexOf("_") + 1);
				String pro = item._1.split("_")[0];
				String[] lists = item._2.split("%");
				int userSum = 0;
				int reqSum = 0;
				userSum += lists.length;//每一个ip_reqs记录代表一个用户
				for(String rec : lists){
					reqSum += Integer.parseInt(rec.split("_")[1]);
				}
				
				return new Tuple4(day_hour,pro,userSum,reqSum);
			}
			
		})
		//新版第六步：把结果存储到文件，每小时刷一次结果。以后要连接数据库，采取update的操作。
		.dstream().saveAsTextFiles(args[1] + "EveryHourOfDay", "txt");
		//而每天的结果可以在数据库中，汇集小时的结果得到，没有必要在这里计算。。

		// -----------------程序结束--------------
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
		//String city = "NULL";
		if (phy != null) {
			province = String.valueOf(phy.getPhyProvinceId());
			//city = String.valueOf(phy.getPhyCityId());
		}
		String key = time + "," + ip + "," + province;// + "," + city;
		return key;
	}
}