package com.qubole.spark.datasources.hiveacid;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.StringWriter;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.*;

public class TestHiveClient {
	/*
	* Before running this docker container with HS2 / HMS / Hadoop running
	*/
	private static String driverName = "com.qubole.shaded.hive.jdbc.HiveDriver";
	private static Connection con = null;
	private static Statement stmt = null;
	private Boolean verbose = false;
	private static Logger logger = Logger.getLogger(TestHiveClient.class.getName());

	TestHiveClient(Boolean verbose) {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {
			this.verbose = verbose;
			con = DriverManager.getConnection("jdbc:hive2://0.0.0.0:10001?allowMultiQueries=true", "root", "root");
			stmt = con.createStatement();
		}
		catch (Exception e) {
			System.out.println("Failed to create statement "+ e);
		}
	}

	public String executeQuery(String cmd) throws Exception {
		if (verbose) logger.log(Level.INFO, "\n\nHive> " + cmd + "\n");
		// Start Hive txn
		ResultSet rs = null;
		String resStr = null;
		try {
			rs = stmt.executeQuery(cmd);
			resStr = resultStr(rs);
			// close hive txn
			rs.close();
			rs = null;

		} catch (Exception e) {
			System.out.println("Failed execute statement "+ e);
			if (rs != null ) {
				rs.close();
			}
		}
		return resStr;
	}

	public void execute(String cmd) throws SQLException {
		if (verbose) logger.log(Level.INFO, "\n\nHive> " + cmd + "\n");
		stmt.execute(cmd);
	}

	public String resultStr(ResultSet rs) throws SQLException {
		StringWriter outputWriter = new StringWriter();
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		int rowNumber = 0;
		while (rs.next()) {
			if (rowNumber != 0) {
				outputWriter.append("\n");
			}
			rowNumber++;
			for (int i = 1; i <= columnsNumber; i++) {
				if (i > 1) outputWriter.append(",");
				String columnValue = rs.getString(i);
				// outputWriter.append(rsmd.getColumnName(i)+ "=" + columnValue);
				outputWriter.append(columnValue);
			}
		}
		return outputWriter.toString();
	}

	public void teardown() throws SQLException {
		if (stmt != null) {
			stmt.close();
		}
		if (con != null) {
			con.close();
		}
		stmt = null;
		con = null;
	}
}
