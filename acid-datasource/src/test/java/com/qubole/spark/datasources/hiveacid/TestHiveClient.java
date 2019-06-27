package com.qubole.spark.datasources.hiveacid;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.StringWriter;

public class TestHiveClient {
	/*
	* Before running this docker container with HS2 / HMS / Hadoop running
	*/
	private static String driverName = "com.qubole.shaded.hive.jdbc.HiveDriver";
	private static Connection con = null;
	private static Statement stmt = null;

	TestHiveClient() {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {
			con = DriverManager.getConnection("jdbc:hive2://0.0.0.0:10001", "root", "root");
			stmt = con.createStatement();
		}
		catch (Exception e) {
			System.out.println("Failed to create statement "+ e);
		}
	}

	public ResultSet executeQuery(String cmd) throws SQLException {
		return stmt.executeQuery(cmd);
	}

	public Boolean execute(String cmd) throws SQLException {
		return stmt.execute(cmd);
	}

	public String resultStr(ResultSet rs) throws SQLException {
		StringWriter outputWriter = new StringWriter();
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		while (rs.next()) {
			for (int i = 1; i <= columnsNumber; i++) {
				if (i > 1) outputWriter.append(",");
				String columnValue = rs.getString(i);
				outputWriter.append(rsmd.getColumnName(i)+ "=" + columnValue);
			}
			outputWriter.append("\n");
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
