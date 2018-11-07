package com.github.joshelser.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Random;
import java.util.TimeZone;

public class PhoenixTest {
  private static final String ALPHA_NUMERIC = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final Random RAND = new Random();
  private static final Calendar CAL = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

  private static final short[] RECEIPT_TYPES = new short[] {1, 2, 3};
  private static final int[] RECEIPT_IDS = new int[] {13674, 1, 2, 3, 4};
  private static final int[] SHEET_IDS = new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};

  private static final String TABLE_A = "tableA";
  private static final String TABLE_B = "tableB";
  private static final String TABLE_A_UPSERT = "UPSERT INTO " + TABLE_A + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
      + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  private static final String TABLE_B_UPSERT = "UPSERT INTO " + TABLE_B + " VALUES(?, ?, ?, ?, ?, ?, ?)";

  private static String getRandomString(int length) {
    if (length < 0) {
      throw new IllegalArgumentException("String length must be non-negative");
    }
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(ALPHA_NUMERIC.charAt(RAND.nextInt(ALPHA_NUMERIC.length())));
    }
    return sb.toString();
  }

  private static int getRandomInteger() {
    return RAND.nextInt();
  }
 
  private static double getRandomDouble() {
    return RAND.nextDouble();
  }

  private static long getRandomLong() {
    return RAND.nextLong();
  }

  private static short getRandomShort() {
    // Get the full range of Short
    return (short) (RAND.nextInt(Short.MAX_VALUE * 2) - Short.MAX_VALUE);
  }

  private synchronized static java.sql.Date getRandomDate() {
    CAL.set(2018, 5, 21, 3, 40 + RAND.nextInt(10));
    return new java.sql.Date(CAL.getTime().getTime());
  }

  private static int chooseFromSet(int[] values) {
    return values[RAND.nextInt(values.length)];
  }

  private static short chooseShortFromSet(short[] values) {
    return values[RAND.nextInt(values.length)];
  }

  private static void prepareTableA(PreparedStatement pstmt) throws SQLException {
    int col = 1;
    pstmt.setDate(col++, getRandomDate());
    pstmt.setInt(col++, getRandomInteger());
    pstmt.setString(col++, getRandomString(20));
    pstmt.setLong(col++, getRandomLong());
    pstmt.setString(col++, getRandomString(1));
    pstmt.setString(col++, getRandomString(1));
    pstmt.setString(col++, getRandomString(30));
    pstmt.setString(col++, getRandomString(30));
    pstmt.setDate(col++, getRandomDate());
    pstmt.setString(col++, getRandomString(20));
    pstmt.setString(col++, getRandomString(100));
    pstmt.setString(col++, getRandomString(20));
    pstmt.setString(col++, getRandomString(200));
    pstmt.setDate(col++, getRandomDate());
    pstmt.setString(col++, getRandomString(1));
    pstmt.setString(col++, getRandomString(1));
    pstmt.setString(col++, getRandomString(100));
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setString(col++, getRandomString(30));
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setInt(col++, getRandomInteger());
    pstmt.setString(col++, getRandomString(40));
    pstmt.setInt(col++, getRandomInteger());
    pstmt.setString(col++, getRandomString(40));
    pstmt.setInt(col++, getRandomInteger());
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setString(col++, getRandomString(20));
    pstmt.setString(col++, getRandomString(100));
    pstmt.setString(col++, getRandomString(1));
    pstmt.setString(col++, getRandomString(10));
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setDouble(col++, getRandomDouble());
    pstmt.setDouble(col++, getRandomDouble());
  }

  private static void prepareTableB(PreparedStatement pstmt) throws SQLException {
    int col = 1;
    pstmt.setDate(col++, getRandomDate());
    //recptType
    pstmt.setShort(col++, chooseShortFromSet(RECEIPT_TYPES));
    //recpId
    pstmt.setInt(col++, chooseFromSet(RECEIPT_IDS));
    //sheetId
    pstmt.setInt(col++, chooseFromSet(SHEET_IDS));
    pstmt.setShort(col++, getRandomShort());
    pstmt.setString(col++, getRandomString(30));
    pstmt.setInt(col++, getRandomInteger());
  }

  public static void main(String[] args) throws Exception {
	 
    if (args.length < 3) {
      throw new IllegalArgumentException("Usage: <zookeeper_url> <num_rows_in_A> <num_rows_in_B>");
    }
		String zookeeper_url = args[0];
    
    try (Connection conn = DriverManager.getConnection("jdbc:phoenix:"+zookeeper_url, "", "")) {
      conn.setAutoCommit(false);
      try (PreparedStatement pstmt = conn.prepareStatement(TABLE_A_UPSERT)) {
        System.out.println("Writing to " + TABLE_A);          
        for (int i = 0; i < Integer.parseInt(args[1]); i++) {
          prepareTableA(pstmt);
          pstmt.executeUpdate();
          if (i % 1000 == 0) {
            conn.commit();
          }
        }
        conn.commit();
      }
      try (PreparedStatement pstmt = conn.prepareStatement(TABLE_B_UPSERT)) {
        System.out.println("Writing to " + TABLE_B);
        for (int i = 0; i < Integer.parseInt(args[2]); i++) {
          prepareTableB(pstmt);
          pstmt.executeUpdate();
          if (i % 1000 == 0) {
            conn.commit();
          }
        }
        conn.commit();
      }

      try (Statement stmt = conn.createStatement()) {
        try (ResultSet results = stmt.executeQuery("SELECT COUNT(1) from " + TABLE_A)) {
          if (results.next()) {
            System.out.println("Num rows in " + TABLE_A + ": " + results.getInt(1));
          } else {
            System.out.println("Could not get results from " + TABLE_A);
          }
        }
        try (ResultSet results = stmt.executeQuery("SELECT COUNT(1) from " + TABLE_B)) {
          if (results.next()) {
            System.out.println("Num rows in " + TABLE_B + ": " + results.getInt(1));
          } else {
            System.out.println("Could not get results from " + TABLE_B);
          }
        }
      }
    }
  }
}
