package com.codefutures.tpcc.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.jolbox.bonecp.BoneCPDataSource;

public class ConnectionManager {
//	private static DruidDataSource dataSource = null;
	private static BoneCPDataSource dataSource = null;
	private static boolean isInited = false;
	
	public static void init(String user, String password, String jdbcUrl) {
		if(!isInited) {
			synchronized(ConnectionManager.class) {
				if(isInited)
					return;
				try {
					//加载数据库驱动  
				    Class.forName("com.mysql.jdbc.Driver");  
				    //创建一个DataSource对象  
				    dataSource = new BoneCPDataSource();  
				    //设置JDBC URL  
				    dataSource.setJdbcUrl(jdbcUrl);  
				    //设置用户名  
				    dataSource.setUsername(user);  
				    //设置密码  
				    dataSource.setPassword(password);  
				    //下面的代码是设置其它可选属性  以下参数多按照retail_mps中的dal-db-config.properties
				    dataSource.setPartitionCount(1);
				    dataSource.setMinConnectionsPerPartition(5);//相当于最小连接数
				    dataSource.setMaxConnectionsPerPartition(1000);//相当于最大连接数
				    dataSource.setAcquireIncrement(2);
				    
				    dataSource.setConnectionTimeoutInMs(30000);
				    dataSource.setMaxConnectionAgeInSeconds(172800);
				    dataSource.setConnectionTestStatement("select 1");
				    dataSource.setIdleMaxAgeInSeconds(3600);
				    dataSource.setIdleConnectionTestPeriodInSeconds(1200);
				    dataSource.setAcquireRetryAttempts(5);
				    dataSource.setAcquireRetryDelayInMs(1000);
				    dataSource.setLazyInit(false);
//				    dataSource.setStatementCacheSize(50);
				    dataSource.setLogStatementsEnabled(false);
				    dataSource.setPoolAvailabilityThreshold(20);
				              
				    Connection connection;  
				    connection = dataSource.getConnection();  
				              
				    //这里操作数据库  
				    //...  
				    //关闭数据库连接  
				    connection.close();  
				    isInited = true;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}   
	}
	
	public static void init(String user, String password, String jdbcUrl,int initSize) {
		if(!isInited) {
			synchronized(ConnectionManager.class) {
				if(isInited)
					return;
				try {
					//加载数据库驱动  
				    Class.forName("com.mysql.jdbc.Driver");  
				    //创建一个DataSource对象  
				    dataSource = new BoneCPDataSource();  
				    //设置JDBC URL  
				    dataSource.setJdbcUrl(jdbcUrl);  
				    //设置用户名  
				    dataSource.setUsername(user);  
				    //设置密码  
				    dataSource.setPassword(password);  
				    //下面的代码是设置其它可选属性  以下参数多按照retail_mps中的dal-db-config.properties
				    dataSource.setPartitionCount(3);
				    dataSource.setMinConnectionsPerPartition(2);
				    dataSource.setMaxConnectionsPerPartition(20);
				    dataSource.setAcquireIncrement(2);
				    
				    dataSource.setConnectionTimeoutInMs(30000);
				    dataSource.setMaxConnectionAgeInSeconds(172800);
				    dataSource.setConnectionTestStatement("select 1");
				    dataSource.setIdleMaxAgeInSeconds(3600);
				    dataSource.setIdleConnectionTestPeriodInSeconds(1200);
				    dataSource.setAcquireRetryAttempts(5);
				    dataSource.setAcquireRetryDelayInMs(1000);
				    dataSource.setLazyInit(false);
//				    dataSource.setStatementCacheSize(50);
				    dataSource.setLogStatementsEnabled(false);
				    dataSource.setPoolAvailabilityThreshold(20);
				    dataSource.setDisableJMX(false);
				              
				    Connection connection;  
				    connection = dataSource.getConnection();  
				              
				    //这里操作数据库  
				    //...  
				    //关闭数据库连接  
				    connection.close();  
//				    dataSource.close();
				    isInited = true;
				} catch (Exception e) {
					e.printStackTrace();
				}
				isInited = true;
			}
			
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
