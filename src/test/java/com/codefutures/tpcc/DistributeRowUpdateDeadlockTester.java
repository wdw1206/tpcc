package com.codefutures.tpcc;

import com.codefutures.tpcc.db.ConnectionManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 *
 * This class uses <code></code>CountDownLatch<code></code> to send concurrent
 * SQL update to mycat to check if dead lock fatal error due to distribute
 * coordinated lock acquirings. It updates the same row concurrently into global
 * table with coordinated method. The dead lock scenairo can be depicted as follows,
 *
 * Suppose that we have two data nodes for global table, namely dn1, dn2 and
 * we have two threads T1&T2,
 *
 * 1. T1 updated into dn1 to acquire dn1 row lock;
 * 2. T2 updated into dn2 to acquire dn2 row lock;
 * 3. T1 tries to update dn2 row but blocked due to lock on dn2 was hold by T2;
 * 4. At the same time, T2 tries to update dn1 row but block due to lock on dn1 was hold by T1.
 * 5. As as result, this causes distributed dead lock.
 */
public class DistributeRowUpdateDeadlockTester {

    private static class TpccUpdaterThread extends Thread {
        private final CountDownLatch waiter;
        public TpccUpdaterThread(CountDownLatch waiter) {
            this.waiter = waiter;
        }

        @Override
        public void run() {
            Connection conn = null;
            Statement statement = null;
            
            boolean isRollBack = false;
            try {
                System.out.println("begin to execute SQL in thread " + getName());
                conn = ConnectionManager.getTransactionConnection();
                statement = conn.createStatement();
                String newData = "update-" + getName() + "-" + new Date();
//                statement.execute("/* !mycat: sql = select count(*) from orders for update */SET innodb_lock_wait_timeout=300"); //default is 50
                String sql = "update item set i_data='" + newData + "' where i_id=1";
               //String sql = "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = 1 AND d_w_id = 4";
               statement.execute(sql);
                System.out.println("done with SQL update in thread " + getName());
            } catch (SQLException e) {
            	isRollBack = true;
               
                System.err.println("SQL error:" + e);
            } finally {
                waiter.countDown();
                ConnectionManager.close(conn, statement, null,isRollBack);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        final boolean USE_MYCAT = true;
        if (USE_MYCAT) {
        	ConnectionManager.init("tpcc", "tpcc","jdbc:mysql://localhost:8066/tpcc");
        } else {
            ConnectionManager.init("root", "123456","jdbc:mysql://localhost:3306/demo");
        }
        System.out.println("Connect to mycat:" + USE_MYCAT);
        final int UPDATERS = 300;
        assert (ConnectionManager.getConnection() != null);
        ExecutorService executorService = Executors.newFixedThreadPool(UPDATERS);
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(UPDATERS);
        int counter = 0;
         while ((counter++) < UPDATERS) {
             executorService.submit(new TpccUpdaterThread(latch));
        }
        latch.await();
        long end = System.currentTimeMillis();
        long time = (end - start) / 1000;
        System.out.println("it takes time " + time + " seconds.");
        executorService.shutdown();
        ConnectionManager.destroy();
        System.exit(0);
    }

}
