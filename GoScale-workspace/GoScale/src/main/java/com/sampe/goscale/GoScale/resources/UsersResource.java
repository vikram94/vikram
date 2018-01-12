package com.sampe.goscale.GoScale.resources;


import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.sampe.goscale.GoScale.models.UserResponse;
import com.sampe.goscale.GoScale.models.Users;
import com.sampe.goscale.GoScale.services.UsersService;

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
}