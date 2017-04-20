package com.ctfo.file.bean;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;



@Entity(value="excel_info", noClassnameStored=true)
public class ExcelBean implements java.io.Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3965295333103334135L;
	//ID
	@Id
	private String id;
	//类型
	@Property("type")
	private String type;
	//文件名
	@Property("fileName")
	private String fileName;
	//存放mongDB返回的文件url
	@Property("url")
	private String url;
	//提交时间
	@Property("time")
	private String time;
	//提交人
	@Property("submiter")
	private String submiter;
	//状态
	@Property("state")
	private String state;
	//导入或者导出
	@Property("ie_type")
	private String ie_type;

	public void setType(String type) {
		this.type = type;
	}
	public String getType() {
		return type;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getFileName() {
		return fileName;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getUrl() {
		return url;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getTime() {
		return time;
	}
	public void setSubmiter(String submiter) {
		this.submiter = submiter;
	}
	public String getSubmiter() {
		return submiter;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getState() {
		return state;
	}
	public void setIe_type(String ie_type) {
		this.ie_type = ie_type;
	}
	public String getIe_type() {
		return ie_type;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getId() {
		return id;
	}
}
