package com.tiger.spark.ngix.model;

import java.io.Serializable;

public class PhysicsLocation implements Serializable {

	private static final long serialVersionUID = -8965285161892034970L;

	private int phyProvinceId = 0;
	private int phyCityId = 0;

	public int getPhyProvinceId() {
		return phyProvinceId;
	}

	public void setPhyProvinceId(int phyProvinceId) {
		this.phyProvinceId = phyProvinceId;
	}

	public int getPhyCityId() {
		return phyCityId;
	}

	public void setPhyCityId(int phyCityId) {
		this.phyCityId = phyCityId;
	}

}