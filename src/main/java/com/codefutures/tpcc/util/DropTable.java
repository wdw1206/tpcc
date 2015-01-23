package com.codefutures.tpcc.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codefutures.tpcc.Tpcc;
import com.codefutures.tpcc.Util;
import com.codefutures.tpcc.db.ConnectionManager;

public class DropTable {



    private String dbUser = null;
    private String dbPassword = null;
    private String jdbcUrl = null;
    private String javaDriver = null;


    /* Global SQL Variables */
    static int fd = 0;
    static int seed = 0;


    int particle_flg = 0; /* "1" means particle mode */
    int part_no = 0; /* 1:items 2:warehouse 3:customer 4:orders */
    long min_ware = 1;
    long max_ware;

    /* Global Variables */
    static int i = 0;
    //    static int is_local = 1;           /* "1" mean local */
//    static int DB_STRING_MAX = 51;
    static boolean option_debug = false;	/* 1 if generating debug output    */

    private static final Logger logger = LoggerFactory.getLogger(Tpcc.class);


    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";
    private static final String JDBCURL = "JDBCURL";

    private Properties properties;
    private InputStream inputStream;

    private static final String PROPERTIESFILE = "tpcc.properties";

    public DropTable() {
        // Empty.
    }

    private void init() {

        logger.info("Loading properties from: " + PROPERTIESFILE);

        try {
            properties = new Properties();
            inputStream = new FileInputStream(PROPERTIESFILE);
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties file", e);
        }

    }

/**
 * TpccLoad -m JDBC -o output -u root -p 123456 -w 1 -s 1 -i 1
 * @param overridePropertiesFile
 * @param argv
 * @return
 */
    private int runDropTable(boolean overridePropertiesFile, String[] argv) {

        if (overridePropertiesFile) {
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
            dbUser = properties.getProperty(USER);
            dbPassword = properties.getProperty(PASSWORD);
            
            jdbcUrl = properties.getProperty(JDBCURL); 
            
        }

        System.out.printf("*************************************\n");
        System.out.printf("*** Java TPC-C Data dropTable version " + Tpcc.VERSION + " ***\n");
        System.out.printf("*************************************\n");

        final long start = System.currentTimeMillis();
        System.out.println("Execution time start: " + start);


        javaDriver = "com.mysql.jdbc.Driver";
        if (javaDriver == null) {
            throw new RuntimeException("Java Driver is null.");
        }
        if (jdbcUrl == null) {
            throw new RuntimeException("JDBC Url is null.");
        }

        //TODO: Pass the seed in as a variable.
        Util.setSeed(seed);

        

		/* EXEC SQL WHENEVER SQLERROR GOTO Error_SqlCall; */
      
        try {
            Class.forName(javaDriver);
        } catch (ClassNotFoundException e1) {
            throw new RuntimeException("Class for mysql error", e1);
        }

        Connection conn = null;
        Statement stmt = null;
        boolean isRollBack = false;

        try {
            //TODO: load from config
            Properties jdbcConnectProp = new Properties();
            jdbcConnectProp.setProperty("user", dbUser);
            jdbcConnectProp.setProperty("password", dbPassword);
            jdbcConnectProp.setProperty("useServerPrepStmts", "false");
            jdbcConnectProp.setProperty("cachePrepStmts", "true");
            //************************************************************
            jdbcConnectProp.setProperty("cachePrepStmts", "true");
            
            //************************************************************
            
            ConnectionManager.init(dbUser,dbPassword,jdbcUrl,5);

//                conn = DriverManager.getConnection(jdbcUrl, jdbcConnectProp);
            conn = ConnectionManager.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            
            try {
                stmt.execute("/*#mycat: sql = select count(*) from orders for update */SET UNIQUE_CHECKS=0");
                stmt.execute("/*!mycat: sql = select count(*) from orders for update */SET FOREIGN_KEY_CHECKS=0");
                
//                stmt.execute("SET UNIQUE_CHECKS=0");
//                stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            } catch (SQLException e) {
                throw new RuntimeException("Could not set unique checks error", e);
            }
            
            System.out.printf("TPCC drop Table Started...\n");

            try {
                String [] tables = {"customer","district","history","item","new_orders","order_line","orders","stock","warehouse"};
                
                for(String table : tables) {
                	String sql = "drop table " + table;
                	logger.info(sql);
                	stmt.execute(sql);
                }
                
            	 stmt.execute("/*#mycat: sql = select count(*) from orders for update */SET FOREIGN_KEY_CHECKS=1");

                System.out.printf("\n...TPCC drop Table  COMPLETED SUCCESSFULLY.\n");
            } catch (Exception e) {
                System.out.println("Error drop Table");
                throw new RuntimeException("Connection error", e);
                
            } 

        } catch (Throwable e) {
        	isRollBack = true;
        	logger.error("", e);
            throw new RuntimeException("Connection error", e);
        }  finally {
        	ConnectionManager.close(conn, stmt, null,isRollBack);
        }
        

        return 0;
    }

    public static void main(String[] argv) {

        // dump information about the environment we are running in
        String sysProp[] = {
                "os.name",
                "os.arch",
                "os.version",
                "java.runtime.name",
                "java.vm.version",
                "java.library.path"
        };

        for (String s : sysProp) {
            logger.info("System Property: " + s + " = " + System.getProperty(s));
        }

        DecimalFormat df = new DecimalFormat("#,##0.0");
        System.out.println("maxMemory = " + df.format(Runtime.getRuntime().totalMemory() / (1024.0 * 1024.0)) + " MB");
        DropTable truncate = new DropTable();

        int ret = 0;
        if (argv.length == 0) {
            System.out.println("Using the tpcc.properties file for the load configuration.");
            truncate.init();
            ret = truncate.runDropTable(false, argv);
        } else {

            if ((argv.length % 2) == 0) {
                System.out.println("Using the command line arguments for the load configuration.");
                ret = truncate.runDropTable(true, argv);
                
            } else {
                System.out.println("Invalid number of arguments.");
                System.out.println("Incorrect Argument: " + argv[i]);
                System.out.println("The possible arguments are as follows: ");
                System.out.println("-h [database host]");
                System.out.println("-d [database name]");
                System.out.println("-u [database username]");
                System.out.println("-p [database password]");
                System.out.println("-w [number of warehouses]");
                System.out.println("-j [java driver]");
                System.out.println("-l [jdbc url]");
                System.out.println("-s [shard count]");
                System.out.println("-i [shard id]");
                System.exit(-1);

            }
        }
        
        CountTable count = new CountTable();
        count.countTable();

        System.exit(ret);
    }



}
