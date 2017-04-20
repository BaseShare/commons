package com.ctfo.file.boss;

import com.ctfo.file.bean.AttachmentBean;
import com.ctfo.file.bean.ImageSizeBean;
import com.mongodb.gridfs.GridFSDBFile;

public interface IFileService {
	
	public static final String IMAGE_MAX = "max_";
	
	public static final String IMAGE_MID = "mid_";
	
	public static final String IMAGE_MIN = "min_";

	/**
	 * 上传文件至文件服务器 文件要求：
	 * 1、文件类型："jpg","jpeg","png","gif","bmp","doc","docx","xls",
	 * "xlsx","ppt","pptx","txt","pdf" 
	 * 2、文件大小：小于等于4M
	 * @param file
	 * @return 返回文件路径URL
	 * @throws Exception
	 */
	public String addFile(AttachmentBean attachmentBean) throws Exception;
	
	/**
	 * 上传文件至文件服务器 文件要求：
	 * 1、文件类型："jpg","jpeg","png","gif","bmp","doc","docx","xls",
	 * "xlsx","ppt","pptx","txt","pdf" 
	 * 2、文件大小：小于等于4M
	 * @param attachmentBean
	 * @param waterMark 是否加水印
	 * @param multiImage 是否生成多个图片文件，仅在文件类型为图片时有效
	 * @return 返回文件路径URL
	 * @throws Exception
	 */
	public String addFile(AttachmentBean attachmentBean, ImageSizeBean size) throws Exception;
	
	/**
	  * 上传文件至文件服务器 文件要求：
	 * 1、文件类型："jpg","jpeg","png","gif","bmp","doc","docx","xls",
	 * "xlsx","ppt","pptx","txt","pdf" 
	 * 2、文件大小：小于等于4M
	 * @param attachmentBean
	 * @param size 图片文件的规格，包括大小，是否加水印等
	 * @param fileName 为生成的文件指定文件名，而不是使用随机生成的文件名；文件名相同则覆盖原文件
	 * @return 返回文件路径URL
	 * @throws Exception
	 */
	public String addFile(AttachmentBean attachmentBean, ImageSizeBean size, String fileName) throws Exception;
	
	/**
	 * 删除文件
	 * @param fileName 文件名
	 */
	public void removeFile(String fileName) throws Exception;
	
	/**
	 * 根据业务模块uuid获取附件对象
	 * @param uuid
	 * @return
	 * @throws Exception
	 */
	public AttachmentBean findAttachByID(String uuid) throws Exception;
	
	/**
	 * 根据文件名查找文件对象
	 * @param fileName 文件名，例如：134510476148433.jpg
	 * @return
	 */
	public GridFSDBFile getFileByFileName(String fileName);
	
	/**
	 * 根据文件名获取文件RUL
	 * @param fileName 文件名，如：13472532646557118.jpg
	 * @return
	 */
	public String getFileURL(String fileName);
	
	/**
	 * 根据文件名获取文件RUL
	 * @param fileName 文件名，如：13472532646557118.jpg
	 * @param imageSize 图片大小，如IFileService.IMAGE_MAX为max_13472532646557118.jpg
	 * @return
	 */
	public String getFileURL(String fileName, String imageSize);
	
	public String addFileByUrl(String url) throws Exception ;
	
	/**
	 * 设置Mongodb数据源名称
	 * @param datasource 数据源名称，对应mongodb.properties中的DATA_SOURCES=BUSINESS,LOGS中的一个。
	 */
	public void setDatasource(String datasource);
	
	/**
	 * 获取Mongodb数据源名称
	 * @return
	 */
	public String getDatasource();
}