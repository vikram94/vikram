package com.goscale.project.goscaletest.resources;


import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.goscale.project.goscaletest.models.UserResponse;
import com.goscale.project.goscaletest.models.Users;
import com.goscale.project.goscaletest.services.UsersService;

@Path("/users")
@Consumes(MediaType.APPLICATION_JSON)
public class UsersResource {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public UserResponse checkLogin(@QueryParam("email") String email, 
						@QueryParam("password") String password) throws Exception{
		UsersService usersService = new UsersService();
		return usersService.checkLogin(email, password);
	}
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("verifyOTPonMail")
	public UserResponse verifyOTP(@QueryParam("email") String email, 
						@QueryParam("otp") String otp) throws Exception{
		UsersService usersService = new UsersService();
		return usersService.verifyOTP(email, otp);
	}
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/sendOTPonMail")
	public UserResponse sendOTPonMail(@QueryParam("email") String email) throws Exception{
		UsersService usersService = new UsersService();
		return usersService.sendOTPonMail(email);
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public UserResponse registerUser(Users user) throws Exception{
		UsersService usersService = new UsersService();
		return usersService.registerUser(user);
	}
	@GET
	@Path("userProfile")
	@Produces(MediaType.APPLICATION_JSON)
	public UserResponse userProfile(@QueryParam("userId") int userId) throws Exception{
		UsersService usersService = new UsersService();
		return usersService.userProfile(userId);
	}
}
