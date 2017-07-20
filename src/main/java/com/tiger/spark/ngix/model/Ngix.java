package com.tiger.spark.ngix.model;

import java.io.Serializable;

public class Ngix implements Serializable{

	private static final long serialVersionUID = -3744641231605193264L;
	
	private String time = "" ;
	private String ip = "" ;
	private int province = 0;
	private int city = 0;
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getProvince() {
		return province;
	}
	public void setProvince(int province) {
		this.province = province;
	}
	public int getCity() {
		return city;
	}
	public void setCity(int city) {
		this.city = city;
	}
	
	
}
