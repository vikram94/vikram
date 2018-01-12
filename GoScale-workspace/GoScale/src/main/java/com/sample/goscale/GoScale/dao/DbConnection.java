package com.sample.goscale.GoScale.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.sample.goscale.GoScale.utils.Constants;


public class DbConnection {
	public Connection getConnection() throws Exception
	{
		try
		{
			//String connectionURL = "jdbc:mysql://localhost:3306/campushaat_tables";
			String connectionURL = "jdbc:mysql://mysql-mumbai-campushaat.cohdp50bvpva.ap-south-1.rds.amazonaws.com:3306/"+Constants.mysqlTableName;
			Connection connection = null;
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			//connection = DriverManager.getConnection(connectionURL, "root", "");
			connection = DriverManager.getConnection(connectionURL, "admin", "9251640269Guddu");
			return connection;
		}
		catch (SQLException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw e;
		}
	}
}
