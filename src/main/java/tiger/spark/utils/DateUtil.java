package tiger.spark.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;

public class DateUtil {

	private static final String DATE_STR = "yyyy-MM-dd";
	private static final String DATE_TIME_STR = "yyyy-MM-dd HH:mm:ss";

	/**
	 * str to datetime
	 * 
	 * @param sText
	 * @return
	 */
	public static Date strToDateTime(String sDay) {
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_STR);
		try {
			return sdf.parse(sDay);
		} catch (ParseException e) {

		}
		return null;
	}

	/**
	 * str to datetime
	 * 
	 * @param sText
	 * @return
	 */
	public static String strToDate(String sDay) {
		if (StringUtils.isNotEmpty(sDay)) {
			SimpleDateFormat sdf = new SimpleDateFormat(DATE_STR);
			try {
				return sdf.parse(sDay).toString();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * str to datetime
	 * 
	 * @param sText
	 * @return
	 */
	public static String getDay(String sDay) {
		if (StringUtils.isNotEmpty(sDay)) {
			SimpleDateFormat sdf = new SimpleDateFormat(DATE_STR);
			try {
				return sdf.format(sdf.parse(sDay));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return "";
	}

	/** 获取当前日期时间,格式:<code>yyyy-MM-dd HH:mm:ss</code> */
	public static final String getDateTime() {
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_STR);
		String str = sdf.format(new Date());
		return str;
	}

	public static String convertFromTimestamp(Timestamp ts) {
		if (ts == null) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_STR);
		return sdf.format(ts);
	}

	public static String convertToTimestamp(Object value) {
		java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
		nf.setGroupingUsed(false);
		Long timeMillis = Long.parseLong(String.valueOf(nf.format(value)));
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_STR);
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeMillis);
		return sdf.format(calendar.getTime());
	}

	public static String convertStrToTimestamp(String value) {
		Long timeMillis = Long.parseLong(value);
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_STR);
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeMillis);
		return sdf.format(calendar.getTime());
	}

	@SuppressWarnings("deprecation")
	public static String converFormDate(String time) {
		// "Tue Oct 30 2012 09:35:09 GMT+0800";
		Date date = new Date(time);
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_STR);
		return sdf.format(date);
	}

	public static String converDateToStr(String sDate) {
		// String sDate = "26/Jun/2010:18:41:25 +0800";
		if (sDate != null && !StringUtils.isEmpty(sDate.trim())) {
			SimpleDateFormat f = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",
					Locale.US);
			try {
				Date d = f.parse(sDate);
				f = new SimpleDateFormat(DATE_TIME_STR);
				return f.format(d);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return "";
	}

	public static int getHour(String time) {
		int hour = 0;
		try{
			if(StringUtils.isNotEmpty(time)){
				Date date = strToDateTime(time);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(date);
				hour = calendar.get(Calendar.HOUR_OF_DAY);
				// return String.valueOf(date.getHours());
			}
		}catch(Exception ex){
			
		}
		return hour;

	}

	public static void main(String[] args) {
		String time = "2014-12-12 10:12:23";
		String s = getDay(time);
		System.out.print(s);
	}

}
