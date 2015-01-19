package com.codefutures.tpcc.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codefutures.tpcc.db.ConnectionManager;

public class CountTable {
	private static final Logger logger = LoggerFactory.getLogger(CountTable.class);
	
	private Properties properties;
    private InputStream inputStream;

    private static final String PROPERTIESFILE = "tpcc.properties";
    
    private static String dbUser = null;
    private static String dbPassword = null;
    private static String jdbcUrl = null;
    private static String database = null;
    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";
    private static final String JDBCURL = "JDBCURL";
	
	private void init(String[] argv) {

        logger.info("Loading properties from: " + PROPERTIESFILE);

        try {
        	if (argv.length == 0) {
        		properties = new Properties();
                inputStream = new FileInputStream(PROPERTIESFILE);
                properties.load(inputStream);
                
                dbUser = properties.getProperty(USER);
                dbPassword = properties.getProperty(PASSWORD);
                jdbcUrl = properties.getProperty(JDBCURL);
        	} else {
        		if ((argv.length % 2) == 0) {
                    System.out.println("Using the command line arguments for the CountTable configuration.");
                    for (int i = 0; i < argv.length; i = i + 2) {
                        if (argv[i].equals("-u")) {
                            dbUser = argv[i + 1];
                        } else if (argv[i].equals("-p")) {
                            dbPassword = argv[i + 1];
                        } else if (argv[i].equals("-l")) {
                            jdbcUrl = argv[i + 1];
                        } 
                        else {
                            System.out.println("Incorrect Argument: " + argv[i]);
                            System.out.println("The possible arguments are as follows: ");
                            System.out.println("-u [database username]");
                            System.out.println("-p [database password]");
                            System.out.println("-l [jdbc url]");
                            
                            System.exit(-1);

                        }
                    }
                   
                } else {
                	logger.info("Using the  configuration from properties.");
                	 dbUser = properties.getProperty(USER);
                     dbPassword = properties.getProperty(PASSWORD);
                     
                     jdbcUrl = properties.getProperty(JDBCURL);

                }
        	}
            
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties file", e);
        }

    }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CountTable count = new CountTable();
		count.init(args);
		ConnectionManager.init(dbUser, dbPassword, jdbcUrl);
		countTable();

	}
	
	public static void countTable() {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			con = ConnectionManager.getConnection();
			List<String> tables = getTables(con);
			printRecordNumber(con, tables);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			ConnectionManager.close(con, st, rs);
		}
	}
	
	private static List<String> getTables(Connection connection) throws Exception{
    	List<String> tables = new ArrayList<String>();
    	
    	String[] types = {"TABLE"};
    	ResultSet rs = null;
    	
    	try {
    		rs = connection.getMetaData().getTables(null, connection.getSchema(), null, types);
        	
        	while(rs.next()) {
        		String table = rs.getString("TABLE_NAME");
//        		System.out.println(table);
        		tables.add(table);
        	}
		} catch (Exception e) {
			logger.error("getTables", e);
		} finally {
			ConnectionManager.close(null, null, rs);
		}
    	return tables;
    }
    
    private static void printRecordNumber(Connection connection,List<String> tables) {
    	Statement st = null;
    	ResultSet rs = null;
    	try {
	    	for(String table : tables) {
	    		String sqlCount = "select count(1) from " + table ;
	        	
	        	int count = 0;
	        	
	        		st = connection.createStatement();
	        		rs = st.executeQuery(sqlCount);
	        		if(rs.next()) {
	        			count = rs.getInt(1);
	        			logger.info(table + ":" + count);
	        		}
	    	}
    	} catch (Exception e) {
    		logger.error("printRecordNumber", e);
		} finally {
			ConnectionManager.close(null, st, rs);
		}
    	
    }

}
