package com.ctfo.file.property;

import java.util.List;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class MongoProperties implements java.io.Serializable{

	private static final long serialVersionUID = -3445670775427110216L;
	/**
	 * 文件访问URL
	 */
	private String domainName = "file.4000966666.com";
	/**
	 * mongodb数据库名
	 */
	private String dbName = "chpt2";
	/**
	 * 图片存储默认表名
	 */
	private String picAddress = "photo";
	/**
	 * 文档存储默认表名
	 */
	private String docAddress = "file";
	/**
	 * 文件上传可支持文件类型
	 */
	private String fileExt = "jpg,jpeg,png,gif,bmp,doc,docx,xls,xlsx,ppt,pptx,txt,pdf,zip,rar,apk";
	/**
	 * 可支持文档类型
	 */
	private String docExt = "doc,docx,xls,xlsx,ppt,pptx,txt,pdf,zip,rar,apk";
	/**
	 * 可支持图片类型
	 */
	private String picExt = "jpg,jpeg,png,gif,bmp";
	/**
	 * mongodb连接地址：IP:PORT
	 */
	private List<ServerAddress> addresses;
	
	private int connectionsPerHost = 100;  

	private int threadsAllowedToBlockForConnectionMultiplier = 10;
    
    private int maxWaitTime = 5000;  
    
    private int socketTimeout = 2000;  
    
    private int connectTimeout = 1500;
    
    // 修改时间：2015-10-23 sunchuanfu
    private String userName;// 用户名
    private String userPwd;// 用户密码
    
	public MongoClientOptions getMongoOption(){
    	MongoClientOptions options = MongoClientOptions.builder()
    			.socketKeepAlive(true)
    			.connectionsPerHost(this.connectionsPerHost)
    			.maxWaitTime(this.maxWaitTime)
    			.socketTimeout(this.socketTimeout)
    			.connectTimeout(this.connectTimeout)
    			.threadsAllowedToBlockForConnectionMultiplier(this.threadsAllowedToBlockForConnectionMultiplier)
    			
    			.build();
/*    	
		//options.autoConnectRetry = true;
		options.socketKeepAlive=true;
		//options.maxAutoConnectRetryTime = 0;
		options.connectionsPerHost = this.connectionsPerHost;  
		options.maxWaitTime = this.maxWaitTime;  
		options.socketTimeout = this.socketTimeout;  
		options.connectTimeout = this.connectTimeout;
		options.threadsAllowedToBlockForConnectionMultiplier 
			= this.threadsAllowedToBlockForConnectionMultiplier;*/
		return options;
    }

	public String getDomainName() {
		return domainName;
	}

	public void setDomainName(String domainName) {
		this.domainName = domainName;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDocAddress() {
		return docAddress;
	}

	public void setDocAddress(String docAddress) {
		this.docAddress = docAddress;
	}

	public String getFileExt() {
		return fileExt;
	}

	public void setFileExt(String fileExt) {
		this.fileExt = fileExt;
	}

	public String getDocExt() {
		return docExt;
	}

	public void setDocExt(String docExt) {
		this.docExt = docExt;
	}

	public String getPicExt() {
		return picExt;
	}

	public void setPicExt(String picExt) {
		this.picExt = picExt;
	}

	public int getConnectionsPerHost() {
		return connectionsPerHost;
	}

	public void setConnectionsPerHost(int connectionsPerHost) {
		this.connectionsPerHost = connectionsPerHost;
	}

	public int getMaxWaitTime() {
		return maxWaitTime;
	}

	public void setMaxWaitTime(int maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public int getThreadsAllowedToBlockForConnectionMultiplier() {
		return threadsAllowedToBlockForConnectionMultiplier;
	}

	public void setThreadsAllowedToBlockForConnectionMultiplier(
			int threadsAllowedToBlockForConnectionMultiplier) {
		this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
	}

	public List<ServerAddress> getAddresses() {
		return addresses;
	}

	public void setAddresses(List<ServerAddress> addresses) {
		this.addresses = addresses;
	}

	public String getPicAddress() {
		return picAddress;
	}

	public void setPicAddress(String picAddress) {
		this.picAddress = picAddress;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserPwd() {
		return userPwd;
	}

	public void setUserPwd(String userPwd) {
		this.userPwd = userPwd;
	}

}
