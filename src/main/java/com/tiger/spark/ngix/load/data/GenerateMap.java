package com.tiger.spark.ngix.load.data;

import java.util.HashMap;
import java.util.List;

import com.tiger.spark.ngix.db.DBConnection;
import com.tiger.spark.ngix.model.PhysicsLocation;
import com.tiger.spark.ngix.util.NgixCleanUtil;



/**
 * 获取清洗数据的映射MAP集合类
 * 
 * @author huangshengbo
 * 
 */
public class GenerateMap {

	//private static final Logger logger = LoggerFactory.getLogger("clean");
	private static DBConnection connection = new DBConnection();
	private static  List<Object[]> queryList = null;


	/**
	 * 获取IP物理位置列表
	 * 
	 * @return
	 */
	public static HashMap<String, PhysicsLocation> getIpPointPhyLocationMap()
			throws Exception {
		HashMap<String, PhysicsLocation> map = new HashMap<String, PhysicsLocation>();
		try {
			String sql = "select inet_ntoa(a.ip_start),a.province_id from  ip_addresses a  order by a.ip_start";
			PhysicsLocation local = null;
			String key = "";
			int provinceId = 0;
			queryList = connection.getAll(sql);
			for (Object[] obj : queryList) {
				key = obj[0] == null ? "" : String.valueOf(obj[0]);
				key = NgixCleanUtil.getIpSeg(key);
				provinceId = NgixCleanUtil.objToInteger(obj[1]);
				local = new PhysicsLocation();
				local.setPhyProvinceId(provinceId);
				map.put(key, local);
				local = null;
			}
			queryList.clear();
			queryList = null;
			return map;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new Exception(ex);
		} finally {
			connection.closeConn();
		}
	}

}