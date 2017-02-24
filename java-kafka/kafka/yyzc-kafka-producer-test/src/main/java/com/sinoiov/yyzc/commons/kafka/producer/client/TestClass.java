package com.sinoiov.yyzc.commons.kafka.producer.client;

public class TestClass {
	
	private String operType;
	
	private String objectName;
	
	private SubClass dataObject;
	
	private String systemSign;

	public String getOperType() {
		return operType;
	}

	public void setOperType(String operType) {
		this.operType = operType;
	}

	public String getObjectName() {
		return objectName;
	}

	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}

	public SubClass getDataObject() {
		return dataObject;
	}

	public void setDataObject(SubClass dataObject) {
		this.dataObject = dataObject;
	}

	public String getSystemSign() {
		return systemSign;
	}

	public void setSystemSign(String systemSign) {
		this.systemSign = systemSign;
	}
}
