package com.tiger.spark.ngix.db;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xingkaihu 用于获取数据库的链接Connection对象
 */
public class DBConnection {

	protected static transient Logger log = LoggerFactory.getLogger(DBConnection.class);
	private static Connection conn = null;
	private static PreparedStatement pstmt = null;
	private static ResultSet rs = null;

	/**
	 * 创建数据库的连接
	 * 
	 * @return 数据库连接
	 */
	public Connection getConntion() {
		try {
			Class.forName(Config.CLASS_NAME);
			String url = Config.DATABASE_URL
					+ "://"
					+ Config.SERVER_IP
					+ ":"
					+ Config.SERVER_PORT
					+ "/"
					+ Config.DATABASE_SID
					+ "?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false";
			conn = DriverManager.getConnection(url, Config.USERNAME,
					Config.PASSWORD);
		} catch (Exception e) {
			log.debug("加载数据库配置信息异常，原因是：" + e.getMessage());
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * 关闭数据库资源
	 */
	public void closeConn() {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (pstmt != null) {
			try {
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 根据传递的预编译sql保存数据
	 * 
	 * @param sql
	 *            预编译插入脚本
	 * @param params
	 *            预编译脚本填充的参数
	 * @return 保存成功后生成的id值
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Long save(String sql, Object... params) {
		Long id = null;
		Connection conn = getConntion();
		QueryRunner qr = new QueryRunner();
		try {
			qr.update(conn, sql, params);
			// 获取新增记录的自增主键
			BigInteger bId = (BigInteger) qr.query(conn,
					"SELECT LAST_INSERT_ID()", new ScalarHandler(1));
			id = Long.valueOf(String.valueOf(bId));
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConn();
		}
		return id;
	}

	public List<Object[]> getAll(String sql, Object... params) {
		Connection conn = getConntion();
		QueryRunner qr = new QueryRunner();
		try {
			List<Object[]> results = qr.query(conn, sql,
					new ArrayListHandler(), params);
			return results;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConn();
		}
		return null;
	}
	
	/**
	 * 根据传递的预编译SQL查询数据
	 * @param Connection
	 * @param QueryRunner
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public  List<Object[]> getListByLimit(Connection conn,QueryRunner qr,String sql, Object... params) throws SQLException {
		List<Object[]> results = qr.query(conn, sql,
				new ArrayListHandler());
		qr = null;
		return results;
	}
	

	/**
	 * 根据传递的预编译sql更新数据
	 * 
	 * @param sql
	 *            预编译更新脚本
	 * @param params
	 *            预编译脚本填充的参数
	 */
	public void update(String sql, Object... params) {
		Connection conn = getConntion();
		QueryRunner qr = new QueryRunner();
		try {
			qr.update(conn, sql, params);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConn();
		}
	}

	/**
	 * 根据传递的预编译sql查询数据(加载1条)
	 * 
	 * @param sql
	 *            预编译查询脚本
	 * @param params
	 *            预编译脚本填充的参数
	 * @return 查询的结果
	 */
	public Map<String, Object> loadFirst(String sql, Object... params) {
		Connection conn = getConntion();
		QueryRunner qr = new QueryRunner();
		try {
			Map<String, Object> map = qr.query(conn, sql, new MapHandler(),
					params);
			return map;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConn();
		}
		return null;
	}

	/**
	 * 根据传递的预编译sql查询数据(加载多条)
	 * 
	 * @param sql
	 * @param params
	 * @return
	 */
	public List<Map<String, Object>> loadMany(String sql, Object... params) {
		Connection conn = getConntion();
		QueryRunner qr = new QueryRunner();
		try {
			List<Map<String, Object>> results = qr.query(conn, sql,
					new MapListHandler(), params);
			return results;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConn();
		}
		return null;
	}
	
	
	/**
	 * 查询条数
	 * @param sql
	 * @param params
	 * @return
	 */
	public long count(Connection conn,QueryRunner qr,String sql, Object... params) {
		try {
			Map<String, Object> map = qr.query(conn, sql, new MapHandler(),
					params);
			long num = Long.valueOf(String.valueOf(map.get("count")));
			return num;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}

}
