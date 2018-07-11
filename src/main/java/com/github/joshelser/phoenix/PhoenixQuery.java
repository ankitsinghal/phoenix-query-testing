package com.github.joshelser.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixQuery {

  private static final String TABLE_A = "tableA";
  private static final String TABLE_B = "tableB";

  private static void executeQuery(Connection conn, String query) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      long start = System.currentTimeMillis();
      try (ResultSet results = stmt.executeQuery(query)) {
        ResultSetMetaData rsmd = results.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (results.next()) {
          for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print(",  ");
            String columnValue = results.getString(i);
            System.out.print(columnValue + " " + rsmd.getColumnName(i));
          }
          System.out.println("");
        }
      }
      System.out.println("Execution time: " + (System.currentTimeMillis() - start) + "ms");
    } 
  }

  public static void main(String[] args) throws Exception {
    final String QUERY = "SELECT /*+USE_SORT_MERGE_JOIN*/ R.sheetid, R.msgid, R.recpid, R.msgperm, Q.dlr, Q.gmttimestamp, Q.bbid, " +
        "Q.asizn, Q.bsizn, Q.ask, Q.bid, Q.aysp, Q.bysp, Q.derived, Q.aytw, Q.bytw, Q.ybnb FROM " + TABLE_A + " AS Q INNER JOIN (" +
        "SELECT sheetid, msgid, recpid, msgperm FROM " + TABLE_B + " WHERE gmttimestamp >= to_date('2018-06-21 03:40:00') AND gmttimestamp < to_date('2018-06-21 03:50:00')" +
          "AND recpType = 3 AND recpId = 13674 ) AS R" +
        " ON Q.sheetId = R.sheetId WHERE Q.gmttimestamp >= to_date('2018-06-21 03:40:00') AND Q.gmttimestamp < to_date('2018-06-21 03:50:00')";
    try (Connection conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181:/hbase-1.1.2.2.6.5.0", "", "")) {
      System.out.println("Explain Query");
      executeQuery(conn, "EXPLAIN " + QUERY);
      for (int i = 0; i < 5; i++) {
        System.out.println("Data Query");
        executeQuery(conn, QUERY);
      }
    }
  }
}
