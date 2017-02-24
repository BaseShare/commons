package com.ctfo.file.bean;

import java.io.File;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.NotSaved;
import org.mongodb.morphia.annotations.Property;



@Entity(value="attachment_info", noClassnameStored=true)
public class AttachmentBean implements java.io.Serializable {

	private static final long serialVersionUID = 6461396478663441651L;
	//ID
	@Id
	private String id;
	//类型
	@NotSaved
	private String type;
	//文件名
	@Property("fileName")
	private String fileName;
	//存储路径(相对)
	@NotSaved
	private String path;
	//描述
	@Property("description")
	private String description;
	//上传时间
	private String time;
	//调用端临时存储路径
	@NotSaved
	private String clientTempPath;
	//调用者
	@NotSaved
	private String client;
	//附件
	@NotSaved
	private File file;
	//文件类型(MIME Type)
	private String contentType;
	//存放mongDB返回的文件url
	@Property("url")
	private String url;
	
	public AttachmentBean(){
		
	}
	
	public AttachmentBean(File file, String fileName){
		this.setFile(file);
		this.setFileName(fileName);
	}

	// Property accessors
	public String getId() {
		return this.id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFileName() {
		return this.fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getPath() {
		return this.path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the clientTempPath
	 */
	public String getClientTempPath() {
		return clientTempPath;
	}

	/**
	 * @param clientTempPath the clientTempPath to set
	 */
	public void setClientTempPath(String clientTempPath) {
		this.clientTempPath = clientTempPath;
	}

	/**
	 * @return the client
	 */
	public String getClient() {
		return client;
	}

	/**
	 * @param client the client to set
	 */
	public void setClient(String client) {
		this.client = client;
	}
	
	/**
	 * @return the file
	 */
	public File getFile() {
		return file;
	}

	/**
	 * @param file the file to set
	 */
	public void setFile(File file) {
		this.file = file;
		this.clientTempPath = this.file.getPath();
	}

	/**
	 * @return the contentType
	 */
	public String getContentType() {
		return contentType;
	}

	/**
	 * @param fileContentType the fileContentType to set
	 */
	public void setFileContentType(String fileContentType) {
		this.contentType = fileContentType;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	
	public void setFileFileName(String fileFileName) {
		this.fileName = fileFileName;
	}
}