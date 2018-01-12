package com.goscale.project.goscaletest.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.goscale.project.goscaletest.utils.Constants;

public class DbConnection {

	public Connection getConnection() throws Exception
	{
		try
		{
			String connectionURL = "jdbc:mysql://localhost:3306/goscale_db";
			//String connectionURL = "jdbc:mysql://mysql-mumbai-campushaat.cohdp50bvpva.ap-south-1.rds.amazonaws.com:3306/"+Constants.mysqlTableName;
			Connection connection = null;
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection(connectionURL, "root", "admin");
			//connection = DriverManager.getConnection(connectionURL, "admin", "9251640269Guddu");
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
