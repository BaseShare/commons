package com.ctfo.file.dao;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.mongodb.gridfs.GridFSDBFile;

public interface IFileDao{
	
	/**
	 * 保存文件
	 * @param file 文件对象
	 * @param newName 文件名
	 * @return 文件nginx-gridfs相对路径
	 * @throws IOException
	 */
	public String saveFile(File file, String newName, String tableName) throws IOException;
	
	/**
	 * 
	 * @param file
	 * @param contentType
	 * @param fileName
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public String saveFile(File file, String contentType, String fileName, String tableName) throws IOException;
	
	/**
	 * 保存文件
	 * @param in 文件流
	 * @param newName 文件名
	 * @return 文件nginx-gridfs相对路径
	 * @throws IOException
	 */
	public String saveFile(InputStream in, String fileName);
	
	/**
	 * 删除文件
	 * @param fileName 文件名
	 */
	public void removeFile(String fileName);
	
	/**
	 * 根据文件名查询文件
	 * @param fileName
	 * @return
	 */
	public GridFSDBFile getFileByFileName(String fileName);
	
	/**
	 * 根据文件名查找文件List
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public List<GridFSDBFile> findFile(String fileName) throws IOException;
}
