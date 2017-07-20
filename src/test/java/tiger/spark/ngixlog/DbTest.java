package tiger.spark.ngixlog;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.tiger.spark.ngix.db.DBConnection;
import com.tiger.spark.ngix.load.data.GenerateMap;
import com.tiger.spark.ngix.model.PhysicsLocation;

import tiger.spark.utils.DateUtil;

public class DbTest {
	DBConnection connection = null;
	String createTime = null;
	String updateTime = null;

	@Before
	public void init() {
		connection = new DBConnection();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		createTime = sdf.format(new Date().getTime());
		updateTime = sdf.format(new Date().getTime());
	}

//	@SuppressWarnings("static-access")
//	@Test
//	public void getSmsPointLocationMap() throws Exception {
//		try {
//			GenerateMap gdb = new GenerateMap();
//			HashMap<String, PhysicsLocation> map = gdb.getIpPointPhyLocationMap();
//			System.out.println(map.size());
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//
//	}
//	
//	@Test
//	public void converTime() throws Exception {
//		try {
//			//"EEE MMM dd yyyy hh:mm aaa"
//			String time = "04/Jan/2015:20:22:36 +0800";
//			String str = DateUtil.converDateToStr(time);
//			System.out.println(str);
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//
//	}
	
}
