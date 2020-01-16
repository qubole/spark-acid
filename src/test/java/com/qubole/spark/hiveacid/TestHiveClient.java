/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qubole.spark.hiveacid;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.StringWriter;

public class TestHiveClient {
	private static Connection con = null;
	private static Statement stmt = null;

	TestHiveClient() {
		try {
			// Before running this docker container with HS2 / HMS / Hadoop running
			String driverName = "com.qubole.shaded.hive.jdbc.HiveDriver";
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {
			con = DriverManager.getConnection("jdbc:hive2://0.0.0.0:10001?allowMultiQueries=true", "root", "root");
			stmt = con.createStatement();
		}
		catch (Exception e) {
			System.out.println("Failed to create statement "+ e);
		}
	}

	public String executeQuery(String cmd) throws Exception {
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
			System.out.println("Failed execute query statement \""+ cmd +"\" Error:"+ e);
			if (rs != null ) {
				rs.close();
			}
		}
		return resStr;
	}

	public void execute(String cmd) throws SQLException {
		try {
			stmt.execute(cmd);
		} catch (Exception e) {
			System.out.println("Failed execute statement \""+ cmd +"\" Error:"+ e);
		}
	}

	private String resultStr(ResultSet rs) throws SQLException {
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
			stmt = null;
		}
		if (con != null) {
			con.close();
			con = null;
		}
	}
}
