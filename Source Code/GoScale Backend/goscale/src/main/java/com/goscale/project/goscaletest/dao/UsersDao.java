package com.goscale.project.goscaletest.dao;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.goscale.project.goscaletest.models.BaseResponse;
import com.goscale.project.goscaletest.models.UserResponse;
import com.goscale.project.goscaletest.models.Users;
import com.goscale.project.goscaletest.utils.Constants;
import com.mysql.jdbc.Statement;

public class UsersDao {
	public UserResponse checkLogin(String email, String password, Connection conn) throws Exception
	{
		Boolean loginStatus=false;
		BaseResponse baseResponse = new BaseResponse();
		UserResponse response = new UserResponse();
		Users user = new Users();
		int isActive=1;
		String userPassword="";
		try{
			try{
				PreparedStatement checkEmail = conn.prepareStatement("select * from user where "
						+ "emailId='"+email+"'");
				//System.out.println(checkEmail.toString());
				ResultSet emailResult = checkEmail.executeQuery();
				if(!emailResult.next()){
					baseResponse.setStatusCode(Constants.CH400);
					baseResponse.setMessage(Constants.EMAIL_DOESNT_EXIST);
					response.setBaseResponse(baseResponse);
					return response;
				}
				else {
					emailResult.beforeFirst();
				}
				while(emailResult.next()){
					isActive=emailResult.getInt("isActive");
					if(isActive==0){
						baseResponse.setStatusCode(Constants.CH401);
						baseResponse.setMessage(Constants.EMAIL_NEEDS_ACTIVATION);
						response.setBaseResponse(baseResponse);
						return response;
					}
					else {
						userPassword=emailResult.getString("password");
						if(userPassword.equals(generateHash(password)) || userPassword.equals(password)){
							loginStatus=true;
							makeUser(conn, user, emailResult);
						}
						else {
							baseResponse.setStatusCode(Constants.CH400);
							baseResponse.setMessage(Constants.WRONG_PASSWORD);
							response.setBaseResponse(baseResponse);
							return response;
						}
					}
				}
			}catch (SQLException sqle) {
		            //sqle.printStackTrace();
		            throw sqle;
		        }
			}catch (Exception e) {
	            if (conn != null) {
	                conn.close();
	            }
	            throw e;
	        }finally {
	            if (conn != null) {
	                conn.close();
	            }
	        }
		if(loginStatus){
			baseResponse.setStatusCode(Constants.CH200);
			baseResponse.setMessage(Constants.LOGIN_SUCCESS);
			response.setBaseResponse(baseResponse);
			response.setUser(user);
		}
		else {
			baseResponse.setStatusCode(Constants.CH500);
			baseResponse.setMessage(Constants.SERVER_ERROR);
			response.setBaseResponse(baseResponse);
		}
		return response;
	}


	public UserResponse verifyOTP(String email, String otp, Connection conn) throws Exception
	{
		Boolean loginStatus=false;
		BaseResponse baseResponse = new BaseResponse();
		UserResponse response = new UserResponse();
		Users user = new Users();
		int isActive=0;
		String userOTP="";
		try{
			try{
				PreparedStatement checkEmail = conn.prepareStatement("select * from user where "
						+ "emailId='"+email+"'");
				//System.out.println(checkEmail.toString());
				ResultSet emailResult = checkEmail.executeQuery();
				if(!emailResult.next()){
					baseResponse.setStatusCode(Constants.CH400);
					baseResponse.setMessage(Constants.EMAIL_DOESNT_EXIST);
					response.setBaseResponse(baseResponse);
					return response;
				}
				else {
					emailResult.beforeFirst();
				}
				while(emailResult.next()){
					userOTP=emailResult.getString("otp");
					if(userOTP.equals(otp)){
						loginStatus=true;
						isActive=emailResult.getInt("isActive");
						if(isActive==0){
							PreparedStatement activateQuery = conn.prepareStatement("update user set isActive='"+1+
									"' where emailId='"+email+"'");
							//System.out.print(insertOTPQuery.toString());
							int result = activateQuery.executeUpdate();
							if(result>0){
								loginStatus=true;
							}else {
								loginStatus=false;
							}
						}
						if(loginStatus){
							makeUser(conn, user, emailResult);
						}
					}
					else {
						baseResponse.setStatusCode(Constants.CH400);
						baseResponse.setMessage(Constants.WRONG_OTP);
						response.setBaseResponse(baseResponse);
						return response;
					}
				}
			}catch (SQLException sqle) {
		            //sqle.printStackTrace();
		            throw sqle;
		        }
			}catch (Exception e) {
	            if (conn != null) {
	                conn.close();
	            }
	            throw e;
	        }finally {
	            if (conn != null) {
	                conn.close();
	            }
	        }
		if(loginStatus){
			baseResponse.setStatusCode(Constants.CH200);
			baseResponse.setMessage(Constants.LOGIN_SUCCESS);
			response.setBaseResponse(baseResponse);
			response.setUser(user);
		}
		else {
			baseResponse.setStatusCode(Constants.CH500);
			baseResponse.setMessage(Constants.SERVER_ERROR);
			response.setBaseResponse(baseResponse);
		}
		return response;
	}

	public UserResponse registerUser(Users user, Connection conn) throws Exception
	{
		Boolean insertStatus=false;
		BaseResponse baseResponse = new BaseResponse();
		UserResponse response = new UserResponse();
		try{
			try{
				String emailId = user.getEmail();
				String mobile = user.getPhone();
				PreparedStatement checkEmail;
				if(mobile!=null && !mobile.equals("")){
					checkEmail = conn.prepareStatement("select * from user where "
						+ " (emailId='"+emailId+"' OR mobileNumber='"+mobile+"')"+"and isActive='1'");
				}
				else {
					checkEmail = conn.prepareStatement("select * from user where "
							+ " (emailId='"+emailId+"')"+"and isActive='1'");		
				}
				ResultSet emailResult = checkEmail.executeQuery();
				while(emailResult.next()){
					baseResponse.setStatusCode(Constants.CH400);
					baseResponse.setMessage(Constants.EMAIL_MOBILE_REGISTERED);
					response.setBaseResponse(baseResponse);
					return response;
				}
				if(!emailId.equals("")){
					checkEmail = conn.prepareStatement("select * from user where "
							+ "(emailId='"+ emailId + "')"+"and isActive='0'");
					emailResult = checkEmail.executeQuery();
					//System.out.println(checkEmail.toString());
					while(emailResult.next()){
						baseResponse.setStatusCode(Constants.CH401);
						baseResponse.setMessage(Constants.EMAIL_NEEDS_ACTIVATION);
						response.setBaseResponse(baseResponse);
						return response;
					}
				}
				// Generate OTP to send on email
				Random random = new Random();
				int number = random.nextInt(9999-1000+1)+1000;
				String otp = Integer.toString(number);
				PreparedStatement insertQuery = conn.prepareStatement("INSERT into"
						+ " user(name, emailId, mobileNumber,dob, password,fbProfileLink, userSex, otp,isActive, createdDate) values"
						+ "('"+ user.getName()+ "','" + emailId
						+ "','" + mobile +  "','" + user.getDob() + "','" + generateHash(user.getPassword())+ "','" + user.getFbProfileLink()+ "','" + user.getUsersex()+"','" + otp + "','" + "1" +"',now())", Statement.RETURN_GENERATED_KEYS);
			
				int result = insertQuery.executeUpdate();
				if(result>0){
					insertStatus=true;
					ResultSet rs = insertQuery.getGeneratedKeys();
					while (rs.next()) {
						user.setUserId(rs.getString(1));
					}
					// if registered using email send otp on email else on mobile					
					sendOTPMail(emailId, otp);		
				}
			}catch (SQLException sqle) {
		            //sqle.printStackTrace();
		            throw sqle;
		        }
			}catch (Exception e) {
	            if (conn != null) {
	                conn.close();
	            }
	            throw e;
	        }finally {
	            if (conn != null) {
	                conn.close();
	            }
	        }
		if(insertStatus){
			baseResponse.setStatusCode(Constants.CH200);
			baseResponse.setMessage(Constants.REGISTRATION_SUCCESS);
			response.setBaseResponse(baseResponse);
			response.setUser(user);
		}
		else {
			baseResponse.setStatusCode(Constants.CH500);
			baseResponse.setMessage(Constants.SERVER_ERROR);
			response.setBaseResponse(baseResponse);
		}
		return response;
	}


	
	public UserResponse userProfile(int userId, Connection conn) throws Exception
	{
		Boolean profileStatus=false;
		BaseResponse baseResponse = new BaseResponse();
		UserResponse response = new UserResponse();
		Users user = new Users();
		try{
			try{
				PreparedStatement checkUserId = conn.prepareStatement("select * from user where "
						+ "userId='"+userId+"'");
				//System.out.println(checkEmail.toString());
				ResultSet profileResult = checkUserId.executeQuery();
				if(!profileResult.next()){
					baseResponse.setStatusCode(Constants.CH400);
					baseResponse.setMessage(Constants.USER_DOESNT_EXIST);
					response.setBaseResponse(baseResponse);
					return response;
				}
				else {
					profileResult.beforeFirst();
				}
				while(profileResult.next()){
					makeUser(conn, user, profileResult);
					profileStatus=true;
				}
			}catch (SQLException sqle) {
		            //sqle.printStackTrace();
		            throw sqle;
		        }
			}catch (Exception e) {
	            if (conn != null) {
	                conn.close();
	            }
	            throw e;
	        }finally {
	            if (conn != null) {
	                conn.close();
	            }
	        }
		if(profileStatus){
			baseResponse.setStatusCode(Constants.CH200);
			baseResponse.setMessage(Constants.USER_PROFILE);
			response.setBaseResponse(baseResponse);
			response.setUser(user);
		}
		else {
			baseResponse.setStatusCode(Constants.CH500);
			baseResponse.setMessage(Constants.SERVER_ERROR);
			response.setBaseResponse(baseResponse);
		}
		return response;
	}

	public void makeUser(Connection conn, Users user, ResultSet profileResult) throws SQLException {
		//user.setUserId(profileResult.getString("userId"));
		user.setName(profileResult.getString("name"));
		user.setEmail(profileResult.getString("emailId"));
		String mobile = profileResult.getString("mobileNumber");
		if(mobile==null) mobile="";
		user.setPhone(mobile);
	}

	public UserResponse sendOTPonMail(String email, Connection conn) throws Exception
	{
		Boolean insertStatus=false;
		BaseResponse baseResponse = new BaseResponse();
		UserResponse response = new UserResponse();
		try{
			try{
				PreparedStatement checkEmail = conn.prepareStatement("select * from user where emailId='"+email+"'");
				//System.out.println(checkEmail.toString());
				ResultSet emailResult = checkEmail.executeQuery();
				if(!emailResult.next()){
					baseResponse.setStatusCode(Constants.CH400);
					baseResponse.setMessage(Constants.EMAIL_DOESNT_EXIST);
					response.setBaseResponse(baseResponse);
					return response;
				}
				else {
					emailResult.beforeFirst();
				}
				while(emailResult.next()){
					Random random = new Random();
					int number = random.nextInt(9999-1000+1)+1000;
					String otp = Integer.toString(number);
					PreparedStatement insertOTPQuery = conn.prepareStatement("update user set otp='"+otp+
							"' where emailId='"+email+"'");
				
					//System.out.print(insertOTPQuery.toString());
					int result = insertOTPQuery.executeUpdate();
					if(result>0){
						insertStatus=true;
						sendOTPMail(email,otp);
					}else {
						insertStatus=false;
					}
				}
				
			}catch (SQLException sqle) {
		            //sqle.printStackTrace();
		            throw sqle;
		        }
			}catch (Exception e) {
	            if (conn != null) {
	                conn.close();
	            }
	            throw e;
	        }finally {
	            if (conn != null) {
	                conn.close();
	            }
	        }
		if(insertStatus){
			baseResponse.setStatusCode(Constants.CH200);
			baseResponse.setMessage(Constants.OTP_MAIL_SENT);
			response.setBaseResponse(baseResponse);
		}
		else {
			baseResponse.setStatusCode(Constants.CH500);
			baseResponse.setMessage(Constants.SERVER_ERROR);
			response.setBaseResponse(baseResponse);
		}
		return response;
	}


	public String generateHash(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte [] digest = md.digest(input.getBytes());
			BigInteger num = new BigInteger(1,digest);
			return num.toString(16).substring(0,16);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
			// TODO: handle exception
		}
	}
	
	public void sendMail(String email,String hash) {
	    String host = "smtp.gmail.com";//or IP address  
	    String port = "465";
		String to = email;
		final String from = "singh.vikramjeet92@gmail.com";
	    final String password = "**vikram92**";
		Properties properties = System.getProperties();  
	    properties.put("mail.smtp.host", host);  
	    properties.put("mail.smtp.socketFactory.port", port);  
	    properties.put("mail.smtp.socketFactory.class",  
	              "javax.net.ssl.SSLSocketFactory");  
	    properties.put("mail.smtp.auth", "true");  
	    properties.put("mail.smtp.port", port);  
	     
	    Session session = Session.getInstance(properties,  
	     new javax.mail.Authenticator() {  
	     protected PasswordAuthentication getPasswordAuthentication() {  
	     return new PasswordAuthentication(from,password);//change accordingly  
	     }  
	    });  
		try {
		  Message msg = new MimeMessage(session);
		  msg.setFrom(new InternetAddress(from, "goscale"));
		  msg.addRecipient(Message.RecipientType.TO,
		                   new InternetAddress(to, "Mr. User"));
		  msg.setSubject("Congratulations! Your account has been activated on CampusHaat.");
		  //msg.setText("Checking mail");
		  String verificationLink = "<a href=http://ec2-35-154-15-217.ap-south-1.compute.amazonaws.com/verify.php?email="+email+"&hash="+hash+"> verification Link </a>";
		  msg.setContent(
	              verificationLink,
	             "text/html");
		  Transport transport = session.getTransport("smtp");
	      transport.connect(host, from, password);
	      msg.saveChanges();
	      transport.sendMessage(msg, msg.getAllRecipients());
	      transport.close();
		  //System.out.println("Verification Email sent!");
		} catch (AddressException e) {
			System.err.println(e.toString());
		} catch (MessagingException e) {
			System.err.println(e.toString());
		} catch (UnsupportedEncodingException e) {
			System.err.println(e.toString());
		} catch (Exception e) {
			System.err.println(e.toString());
		} 
	}
	
	public void sendOTPMail(String email,String otp) {
	    String host = "smtp.gmail.com";//or IP address  
	    String port = "465";
		String to = email;
		if (email.equals("")) return;
		final String from = "revantprakash@gmail.com";
	    final String password = "9251640269guddu@";
		Properties properties = System.getProperties();  
	    properties.put("mail.smtp.host", host);  
	    properties.put("mail.smtp.socketFactory.port", port);  
	    properties.put("mail.smtp.socketFactory.class",  
	              "javax.net.ssl.SSLSocketFactory");  
	    properties.put("mail.smtp.auth", "true");  
	    properties.put("mail.smtp.port", port);  
	     
	    Session session = Session.getInstance(properties,  
	     new javax.mail.Authenticator() {  
	     protected PasswordAuthentication getPasswordAuthentication() {  
	     return new PasswordAuthentication(from,password);//change accordingly  
	     }  
	    });  
		try {
		  Message msg = new MimeMessage(session);
		  msg.setFrom(new InternetAddress(from, "CampusHaat"));
		  msg.addRecipient(Message.RecipientType.TO, new InternetAddress(to, "Mr. User"));
		  msg.setSubject("CampusHaat : verification OTP");
		  //msg.setText("Checking mail");
		  msg.setContent(
	              "OTP to verify your email is : "+otp,
	              "text/html");
		  Transport transport = session.getTransport("smtp");
	      transport.connect(host, from, password);
	      msg.saveChanges();
	      transport.sendMessage(msg, msg.getAllRecipients());
	      transport.close();
		  //System.out.println("OTP send on email!");
		} catch (AddressException e) {
			System.err.println(e.toString());
		} catch (MessagingException e) {
			System.err.println(e.toString());
		} catch (UnsupportedEncodingException e) {
			System.err.println(e.toString());
		} catch (Exception e) {
			System.err.println(e.toString());
		} 
	}

	

}
