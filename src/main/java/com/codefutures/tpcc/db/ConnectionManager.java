package com.codefutures.tpcc.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.alibaba.druid.pool.DruidDataSource;

public class ConnectionManager {
	private static DruidDataSource dataSource = null;
	private static boolean isInited = false;
	
	public static void init(String user, String password, String jdbcUrl) {
		if(!isInited) {
			dataSource = new DruidDataSource();
			dataSource.setDriverClassName("com.mysql.jdbc.Driver");
			dataSource.setUsername(user);
			dataSource.setPassword(password);
			dataSource.setUrl(jdbcUrl);
			dataSource.setInitialSize(30);
			dataSource.setMinIdle(30);
			dataSource.setMaxActive(100);
			
			// 启用监控统计功能
			dataSource.setPoolPreparedStatements(false);
			
			dataSource.setTestWhileIdle(true);
			dataSource.setValidationQuery("select 1");
			dataSource.setTimeBetweenEvictionRunsMillis(60000);
			dataSource.setMinEvictableIdleTimeMillis(300000);
			isInited = true;
		}
	}
	
	public static void init(String user, String password, String jdbcUrl,int initSize) {
		if(!isInited) {
			dataSource = new DruidDataSource();
			dataSource.setDriverClassName("com.mysql.jdbc.Driver");
			dataSource.setUsername(user);
			dataSource.setPassword(password);
			dataSource.setUrl(jdbcUrl);
			dataSource.setInitialSize(initSize);
			dataSource.setMinIdle(initSize);
			dataSource.setMaxActive(100);
			
			// 启用监控统计功能
			dataSource.setPoolPreparedStatements(false);
			
			dataSource.setTestWhileIdle(true);
			dataSource.setValidationQuery("select 1");
			dataSource.setTimeBetweenEvictionRunsMillis(60000);
			dataSource.setMinEvictableIdleTimeMillis(300000);
			isInited = true;
		}
	}
	
	public static Connection getConnection() throws SQLException {
		return dataSource.getConnection();
	}
	
	public static Connection getTransactionConnection() throws SQLException {
		Connection con = dataSource.getConnection();
		con.setAutoCommit(false);
		return con;
	}
	
	public static void close(Connection con,Statement st,ResultSet rs) {
		if(rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
			}
		}
		
		if(st != null) {
			try {
				st.close();
			} catch (Exception e) {
			}
		}
		
		if(con != null) {
			try {
				if(!con.isClosed())
					con.close();
			} catch (Exception e) {
			}
		}
	}
	
	public static void close(Connection con,Statement st,ResultSet rs, boolean isRollback) {
		if(rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
			}
		}
		
		if(st != null) {
			try {
				st.close();
			} catch (Exception e) {
			}
		}
		
		try {
			if(con.isClosed()) {
				return;
			} else {
				if(isRollback) {
					try {
						con.rollback();
						con.setAutoCommit(true);
					} catch (Exception e) {
					}
				} else {
					try {
						con.commit();
						con.setAutoCommit(true);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
				}
					try {
						con.close();
					} catch (Exception e) {
					}
				}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

	}

	
	public static void destroy() {
		dataSource.close();
	}
	
	
}
