package com.tiger.spark.ngix.model;


import java.io.Serializable;

public class IpLocation implements Serializable {

	/* 序列化ID */
	private static final long serialVersionUID = 6617096394160445255L;

	private long ipStart;
	private long ipEnd;
	private int physicalCityId;
	private int physicalProvinceId;

	public long getIpStart() {
		return ipStart;
	}

	public void setIpStart(long ipStart) {
		this.ipStart = ipStart;
	}

	public long getIpEnd() {
		return ipEnd;
	}

	public void setIpEnd(long ipEnd) {
		this.ipEnd = ipEnd;
	}

	public int getPhysicalCityId() {
		return physicalCityId;
	}

	public void setPhysicalCityId(int physicalCityId) {
		this.physicalCityId = physicalCityId;
	}

	public int getPhysicalProvinceId() {
		return physicalProvinceId;
	}

	public void setPhysicalProvinceId(int physicalProvinceId) {
		this.physicalProvinceId = physicalProvinceId;
	}

}
