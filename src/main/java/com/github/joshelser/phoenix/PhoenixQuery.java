package com.github.joshelser.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixQuery {

	private static final String TABLE_A = "tableA";
	private static final String TABLE_B = "tableB";
	private static final String QUERY = "SELECT /*+USE_SORT_MERGE_JOIN*/ R.sheetid, R.msgid, R.recpid, R.msgperm, Q.dlr, Q.gmttimestamp, Q.bbid, "
			+ "Q.asizn, Q.bsizn, Q.ask, Q.bid, Q.aysp, Q.bysp, Q.derived, Q.aytw, Q.bytw, Q.ybnb FROM " + TABLE_A
			+ " AS Q INNER JOIN (" + "SELECT sheetid, msgid, recpid, msgperm FROM " + TABLE_B
			+ " WHERE gmttimestamp >= to_date('2018-06-21 03:40:00') AND gmttimestamp < to_date('2018-06-21 03:50:00')"
			+ "AND recpType = 3 AND recpId = 13674 ) AS R"
			+ " ON Q.sheetId = R.sheetId WHERE Q.gmttimestamp >= to_date('2018-06-21 03:40:00') AND Q.gmttimestamp < to_date('2018-06-21 03:50:00')";

	// using logger for easy logging of thread
	public static final Logger LOG = LoggerFactory.getLogger(PhoenixQuery.class);

	private static long executeQuery(Connection conn, String query) throws SQLException {
		long rows = 0l;
		try (Statement stmt = conn.createStatement()) {
			long start = System.currentTimeMillis();
			try (ResultSet results = stmt.executeQuery(query)) {
				ResultSetMetaData rsmd = results.getMetaData();
				int columnsNumber = rsmd.getColumnCount();
				while (results.next()) {
					rows++;
					for (int i = 1; i <= columnsNumber; i++) {
						if (i > 1)
							System.out.print(",  ");
						String columnValue = results.getString(i);
						if (LOG.isDebugEnabled()) {
							LOG.debug(columnValue + " " + rsmd.getColumnName(i));
						}
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug("");
					}
				}
			}
			LOG.info("Execution time: " + (System.currentTimeMillis() - start) + "ms");
			return rows;
		}
	}

	public static void main(String[] args) throws Exception {
		String jdbcUrl = args[0];
		int noOfThreads = 1;
		int noOfExecutionPerThread = 5;
		if (args.length > 1) {
			noOfThreads = Integer.parseInt(args[1]);
			noOfExecutionPerThread = Integer.parseInt(args[2]);
		}
		final int finalNoOfExecutionPerThread = noOfExecutionPerThread;
		LOG.info("Query will be executed:"+ QUERY);
		ExecutorService executorService = new ThreadPoolExecutor(noOfThreads, noOfThreads, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>());
		Callable<Long> callableTask = () -> {
			long rows = 0l;
			try (Connection conn = DriverManager.getConnection("jdbc:phoenix:"+jdbcUrl, "",
					"")) {
				LOG.info("Explain Query");
				executeQuery(conn, "EXPLAIN " + QUERY);
				for (int i = 0; i < finalNoOfExecutionPerThread; i++) {
					LOG.info("Data Query");
					rows += executeQuery(conn, QUERY);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return rows;
		};

		List<Callable<Long>> callableTasks = new ArrayList<>();
		for (int i = 0; i < noOfThreads; i++) {
			callableTasks.add(callableTask);
		}
		long start = System.currentTimeMillis();
		List<Future<Long>> futures = executorService.invokeAll(callableTasks);
		for (Future<Long> future : futures) {
			Long rows = 0l;
			try {
				rows = future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			LOG.info("Thread completes with rows:" + rows);
		}
		LOG.info("All threads are completed now");
		LOG.info("Execution time: " + (System.currentTimeMillis() - start) + "ms");
		executorService.shutdown();
		try {
			if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
				executorService.shutdownNow();
			}
		} catch (InterruptedException e) {
			executorService.shutdownNow();
		}

	}

}
