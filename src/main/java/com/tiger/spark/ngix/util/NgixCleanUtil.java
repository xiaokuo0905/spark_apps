package com.tiger.spark.ngix.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class NgixCleanUtil {

	/**
	 * 获取IP号段(只取C段)
	 * 
	 * @param ip
	 * @return
	 */
	public static String getIpSeg(String ip) {
		if (StringUtils.isNotEmpty(ip)) {
			int index = ip.lastIndexOf(".");
			if (index != -1) {
				return ip.substring(0, index);
			}
		}
		return "";
	}
	
	public static Integer objToInteger(Object obj) {
		Integer num = 0;
		if (obj != null) {
			try {
				num = Integer.parseInt(String.valueOf(obj));
			} catch (Exception ex) {
			}
		}
		return num;
	}
	
	/**
	 * 验证是否为ip
	 * 
	 * @param ipaddr
	 * @return
	 */
	public static boolean isIPAddress(String ipaddr) {
		boolean flag = false;
		Pattern pattern = Pattern
				.compile("\\b((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\b");
		Matcher m = pattern.matcher(ipaddr);
		flag = m.matches();
		// 17620830
		return flag;
	}
	
}
