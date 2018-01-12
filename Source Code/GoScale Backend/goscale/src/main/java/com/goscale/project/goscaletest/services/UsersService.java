package com.goscale.project.goscaletest.services;

import java.sql.Connection;

import com.goscale.project.goscaletest.dao.DbConnection;
import com.goscale.project.goscaletest.dao.UsersDao;
import com.goscale.project.goscaletest.models.UserResponse;
import com.goscale.project.goscaletest.models.Users;

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
	public UserResponse userProfile(int userId) throws Exception{
		Connection connection = getConnection();
		UsersDao usersDao = new UsersDao();
		return usersDao.userProfile(userId, connection);
	}
}
