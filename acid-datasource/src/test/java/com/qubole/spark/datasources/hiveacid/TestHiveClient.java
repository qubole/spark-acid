package com.qubole.spark.datasources.hiveacid;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/*
// Create table
ResultSet res = stmt.executeQuery("create table empdata(id int, name string, dept string)");

// Show table
res = stmt.executeQuery("show tables empdata");
if (res.next()) {
System.out.println(res.getString(1));
}

// load data into table
// NOTE: filepath has to be local to the hive server
// NOTE: /home/user/input.txt is a ctrl-A separated file with three fields per line
String filepath = "/home/user/input.txt";
sql = "load data local inpath '" + filepath + "' into table empdata";
res = stmt.executeQuery(sql);

// Select
sql = "select * from empdata where id='1'";
res = stmt.executeQuery(sql);

}
*/
public class TestHiveClient {
	/*
	* Before Running this example we should start thrift server. To Start
	* Thrift server we should run below command in terminal
	* hive --service hiveserver &
	*/
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static Connection con = null;
	private static Statement stmt = null;

	public void init() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		if (con == null) {
			con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
		}
		if (stmt == null) {
			stmt = con.createStatement();
		}
	}

	public void execute(String cmd) throws SQLException {
		ResultSet res = stmt.executeQuery(cmd);
		res.close();
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
