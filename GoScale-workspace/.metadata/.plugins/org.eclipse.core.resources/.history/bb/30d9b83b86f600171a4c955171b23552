package com.sampe.goscale.GoScale.services;

import java.sql.Connection;

import com.sampe.goscale.GoScale.models.UserResponse;
import com.sampe.goscale.GoScale.models.Users;
import com.sample.goscale.GoScale.dao.DbConnection;
import com.sample.goscale.GoScale.dao.UsersDao;

public class UsersService {
	
	public UserResponse checkLogin(String email, String password) throws Exception{
		Connection connection = getConnection();
		UsersDao usersDao = new UsersDao();
		return usersDao.checkLogin(email, password, connection);
	}
	private Connection getConnection() throws Exception {
		DbConnection dbConnection=new DbConnection();
		Connection connection = dbConnection.getConnection();
		return connection;
	}
	public UserResponse verifyOTP(String email, String otp) throws Exception{
		Connection connection = getConnection();
		UsersDao usersDao = new UsersDao();
		return usersDao.verifyOTP(email, otp, connection);
	}
	public UserResponse sendOTPonMail(String email) throws Exception{
		Connection connection = getConnection();
		UsersDao usersDao = new UsersDao();
		return usersDao.sendOTPonMail(email, connection);
	}
	public UserResponse registerUser(Users user) throws Exception{
		Connection connection = getConnection();
		UsersDao usersDao = new UsersDao();
		return usersDao.registerUser(user, connection);
	}
}
