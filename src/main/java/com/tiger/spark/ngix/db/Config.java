package com.tiger.spark.ngix.db;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xingkaihu 用于读取配置文件中得数据库配置参数
 */
public class Config {
	protected static transient Logger log = LoggerFactory
			.getLogger(Config.class);
	private static Properties prop = new Properties();
	static {
		try {
			// 加载dbconfig.properties配置文件
			prop.load(Config.class.getClassLoader().getResourceAsStream(
					"ngix.db.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	// 设置常量
	public static final String CLASS_NAME = prop.getProperty("CLASS_NAME");
	public static final String DATABASE_URL = prop.getProperty("DATABASE_URL");
	public static final String SERVER_IP = prop.getProperty("SERVER_IP");
	public static final String SERVER_PORT = prop.getProperty("SERVER_PORT");
	public static final String DATABASE_SID = prop.getProperty("DATABASE_SID");
	public static final String USERNAME = prop.getProperty("USERNAME");
	public static final String PASSWORD = prop.getProperty("PASSWORD");

}