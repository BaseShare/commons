package com.ctfo.file.dao.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.ctfo.file.dao.IFileDao;
import com.ctfo.util.FileValidate;
import com.ctfo.util.MongoDatasource;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class FileDaoImpl implements IFileDao {
	
	private String dbName;

	private String datasource;

	public String saveFile(File file, String fileName, String tableName) throws IOException {
		return this.saveFile(file, null, fileName, tableName);
	}
	
	public String saveFile(File file, String contentType, String fileName, String tableName) throws IOException {
		if(file == null || fileName == null){
			return "";
		}
		GridFSInputFile gfsFile = new MongoDatasource(getDatasource())
				.getGridFS(getDbName(), tableName).createFile(file);
		gfsFile.setFilename(fileName);
		if(!StringUtils.isBlank(contentType))gfsFile.setContentType(contentType);
		gfsFile.save();
		return fileName; //文件名
	}
	
	public String saveFile(InputStream in, String fileName){
		if(in == null || fileName == null){
			return "";
		}
		GridFSInputFile gfsFile = new MongoDatasource(getDatasource())
				.getGridFS(getDbName(),
						FileValidate.getInstance(datasource).getTableNameByFileName(fileName))
				.createFile(in);
		gfsFile.setFilename(fileName);
		gfsFile.save();
		return fileName; //文件名
	}
	
	public GridFSDBFile getFileByFileName(String fileName) {
		String tableName = FileValidate.getInstance(datasource).getTableNameByFileName(fileName);
		DBObject query = new BasicDBObject("filename", fileName);
		GridFSDBFile gridFSDBFile = new MongoDatasource(getDatasource()).getGridFS(getDbName(), tableName)
				.findOne(query);
		return gridFSDBFile;
	}

	public List<GridFSDBFile> findFile(String fileName) throws IOException {
		return new MongoDatasource(getDatasource())
				.getGridFS(getDbName(),
						FileValidate.getInstance(datasource).getTableNameByFileName(fileName))
				.find(fileName);
	}
	
	public void removeFile(String fileName){
		GridFSDBFile file = getFileByFileName(fileName);
		if(file!=null){
			new MongoDatasource(getDatasource())
			.getGridFS(getDbName(),
					FileValidate.getInstance(datasource).getTableNameByFileName(fileName))
			.remove(file);
		}
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDatasource() {
		return datasource;
	}

	public void setDatasource(String datasource) {
		this.datasource = datasource;
	}
}
