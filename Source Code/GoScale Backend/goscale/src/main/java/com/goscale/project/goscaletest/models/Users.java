package com.goscale.project.goscaletest.models;

public class Users {

	private String userId;
	private String name;
	private String email;
	private String phone;
	private String Dob;
	private String password;
	private String fbProfileLink;
	private String usersex;
	public String getUsersex() {
		return usersex;
	}
	public void setUsersex(String usersex) {
		this.usersex = usersex;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getDob() {
		return Dob;
	}
	public void setDob(String dob) {
		Dob = dob;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getFbProfileLink() {
		return fbProfileLink;
	}
	public void setFbProfileLink(String fbProfileLink) {
		this.fbProfileLink = fbProfileLink;
	}
	
}
