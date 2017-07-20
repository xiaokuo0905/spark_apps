package com.tiger.spark.ngix.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tiger.spark.ngix.model.Statics;


public class LogFormatter {
	public static final String exampleApacheLogs = "14.29.81.70 - 223.203.226.125 - [04/Jan/2015:20:22:36 +0800]  \"POST /bbs/thread/7126131 HTTP/1.0\" 200 387 \"-\" \"MAUI_WAP_Browser\" 0.374";

	public static final Pattern apacheLogRegex = Pattern
			.compile("^([\\d.]+) \\S+ (\\S+) \\S+ \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\" (\\S+)");

	public static final Pattern apacheLogRegex2 = Pattern.compile("^(\\S+) \\S+ (\\S+) \\S+ \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\]  \"(.+?)\" (\\d+) (\\d+) \"([^\"]+)\" \"([^\"]+)\" (\\S+)");
	
	public static void main(String[] args) throws Exception {
//		String text = "ddddddssssddsfasdfasd";
//		text = text.replace("d", "1");
//		System.out.println(text);
		extractStats(exampleApacheLogs);

	}

	public static Matcher extractStats(String line) {
		Matcher m = apacheLogRegex2.matcher(line);
		System.out.println(m.find());
		System.out.println(m.groupCount());
		System.out.println(m.group(0));
		System.out.println(m.group(1));
		System.out.println(m.group(2));
		System.out.println(m.group(3));
		System.out.println(m.group(4));
		System.out.println(m.group(5));
		System.out.println(m.group(6));
		System.out.println(m.group(7));
		System.out.println(m.group(8));
		System.out.println(m.group(9));
		
		Statics statics = new Statics();
		statics.setRemoteAddr(String.valueOf(m.group(1)));
		statics.setHttpHost(String.valueOf(m.group(2)));
		//statics.setRemoteUser(String.valueOf(m.group(3)));
		statics.setTimeLocal(String.valueOf(m.group(3)));
		statics.setRequest(String.valueOf(m.group(4)));
		statics.setStatus(String.valueOf(m.group(5)));
		statics.setBodyBytesSent(String.valueOf(m.group(6)));
		statics.setHttpReferer(String.valueOf(m.group(7)));
		statics.setHttpUserAgent(String.valueOf(m.group(8)));
		statics.setRequestTime(String.valueOf(m.group(9)));
		
		System.out.println("remoteAddr : "+statics.getRemoteAddr());
		System.out.println("httpHost : "+statics.getHttpHost());
		System.out.println("timeLocal : "+statics.getTimeLocal());
		System.out.println("request : "+statics.getRequest());
		System.out.println("status : "+statics.getStatus());
		System.out.println("bodyBytesSent : "+statics.getBodyBytesSent());
		System.out.println("httpReferer : "+statics.getHttpReferer());
		System.out.println("httpUserAgent : "+statics.getHttpUserAgent());
		System.out.println("requestTime : "+statics.getRequestTime());
		
		return m;
	}
	
	public static Statics toNgixLog(String txt){
		Matcher m = apacheLogRegex2.matcher(txt);
		if(m.find()){
			Statics statics = new Statics();
			m.groupCount();
			statics.setRemoteAddr(String.valueOf(m.group(1)));
			statics.setHttpHost(String.valueOf(m.group(2)));
			//statics.setRemoteUser(String.valueOf(m.group(3)));
			statics.setTimeLocal(String.valueOf(m.group(3)));
			statics.setRequest(String.valueOf(m.group(4)));
			statics.setStatus(String.valueOf(m.group(5)));
			statics.setBodyBytesSent(String.valueOf(m.group(6)));
			statics.setHttpReferer(String.valueOf(m.group(7)));
			statics.setHttpUserAgent(String.valueOf(m.group(8)));
			statics.setRequestTime(String.valueOf(m.group(9)));
			return statics;
		}
		return null;
	}
}
