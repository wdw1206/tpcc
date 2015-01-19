package com.codefutures.tpcc.util;

import com.codefutures.tpcc.db.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestTransaction {
    private static final Logger logger = LoggerFactory.getLogger(TestTransaction.class);

    private Properties properties;
    private InputStream inputStream;

    private static final String PROPERTIESFILE = "tpcc.properties";

    private static String dbUser = null;
    private static String dbPassword = null;
    private static String jdbcUrl = null;
    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";
    private static final String JDBCURL = "JDBCURL";

    private void init() {
        logger.info("Loading properties from: " + PROPERTIESFILE);
        try {
            properties = new Properties();
            inputStream = new FileInputStream(PROPERTIESFILE);
            properties.load(inputStream);

            dbUser = properties.getProperty(USER);
            dbPassword = properties.getProperty(PASSWORD);
            jdbcUrl = properties.getProperty(JDBCURL);
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties file", e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        TestTransaction count = new TestTransaction();
        count.init();
        ConnectionManager.init(dbUser, dbPassword, jdbcUrl);
        concurrentlyUpdate(50);
    }

    private static void update() {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            con = ConnectionManager.getConnection();
            con.setAutoCommit(false);
            
            for(int i = 0; i < 1000; i++) {
//        		String select = "/*!mycat: sql = update district set d_next_o_id = d_next_o_id WHERE d_id = 1 AND d_w_id = 4 */SELECT d_next_o_id FROM district WHERE d_id = 1 AND d_w_id = 4 FOR UPDATE";
                String select = "/*!mycat: sql = update district set d_name = 'lqz' WHERE d_id = 1 AND d_w_id = 4 */SELECT d_next_o_id FROM district WHERE d_id = 1 AND d_w_id = 4 FOR UPDATE";

//        		String select = "SELECT d_next_o_id FROM district WHERE d_id = 2 AND d_w_id = 2 ";
                st = con.createStatement();
                rs = st.executeQuery(select);
                int oid = 0;
                if (rs.next()) {
                    oid = rs.getInt("d_next_o_id");
//                    System.out.println("original id:" + oid);
//                    System.out.println();
                }
//              st.execute("update item set i_data='lqz' where i_id=1");

                String extraSql = "SELECT d_next_o_id FROM district WHERE d_id = 2 AND d_w_id = 2 ";
                ResultSet extraResult = st.executeQuery(extraSql);
                int otherId = 0;
                if (extraResult.next()) {
                    otherId = extraResult.getInt("d_next_o_id");
//                    System.out.println(String.format("get extra id value as %d\n\n", otherId));
                }

                String update = "UPDATE district SET d_next_o_id = " + (oid + 1) + " WHERE d_id = 1 AND d_w_id = 4";
//        		String update = "UPDATE district SET d_next_o_id = d_next_o_id + 1, d_name='updated' WHERE d_id = 1 AND d_w_id = 4";
                st.executeUpdate(update);
//                System.out.println("updated to new id!");
            }

        } catch (Exception e) {
            logger.error("update", e);
            ConnectionManager.close(con, st, rs, true);
        } finally {
            ConnectionManager.close(con, st, rs, false);
        }

    }
    
    private static int getNextOId() {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        int nextOId = -1;
        try {
            con = ConnectionManager.getConnection();

    		String select = "SELECT d_next_o_id FROM district WHERE d_id = 1 AND d_w_id = 4 ";
            st = con.createStatement();
            rs = st.executeQuery(select);
            if (rs.next()) {
            	nextOId = rs.getInt("d_next_o_id");
                System.out.println("nextOId:" + nextOId);
                System.out.println();
            }

        } catch (Exception e) {
            logger.error("update", e);
        } finally {
            ConnectionManager.close(con, st, rs);
        }
        return nextOId;

    }

    private static void concurrentlyUpdate(int concurrency) throws InterruptedException {
    	int originNextOId = getNextOId();
    	System.out.println("before:");
        ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(concurrency);
        int counter = 0;
        while ((counter++) < concurrency) {
            executorService.submit(new MyCatUpdateThread(latch));
        }
        latch.await();
        
        int latestNextOId = getNextOId();
        
        
        System.out.println("before add NextOId is:" + originNextOId);
        System.out.println("add 1 for " + concurrency*1000 + " times" );
        System.out.println("after add NextOId is :" + latestNextOId);
        System.out.println(" NextOId should be :" + (originNextOId + concurrency * 1000));
        
        long end = System.currentTimeMillis();
        long time = (end - start) / 1000;
        System.out.println("it takes time " + time + " seconds.");
        executorService.shutdown();
    }

    private static class MyCatUpdateThread extends Thread {
        private final CountDownLatch waiter;

        public MyCatUpdateThread(CountDownLatch waiter) {
            this.waiter = waiter;
        }

        @Override
        public void run() {
            try {
                update();
            } finally {
                waiter.countDown();
            }
        }
    }
}

