package com.ctfo.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.lang.StringUtils;

import com.ctfo.file.bean.AttachmentBean;

public class FileValidate {
	
	
	private static String[] fileExts;
	
	private static String[] imageExts;
	
	private static EnvironmentUtil env = EnvironmentUtil.getInstance("mongodb.properties");
	
	private String fileExt;
	
	private String imageExt;
	
	private String datasourceName;
	
	private int limitSize;
	
	private FileValidate(){
		init(env.getPropertyValue("DATA_SOURCES").split(",")[0]);
	}
	
	private FileValidate(String datasourceName){
		init(datasourceName);
	}
	
	private void init(String datasourceName) {
		this.datasourceName = datasourceName;
		fileExt = env.getPropertyValue(datasourceName + ".FILE_EXT");
		fileExts = fileExt.split(",");
		imageExt = env.getPropertyValue(datasourceName + ".PIC_EXT");
		imageExts = imageExt.split(",");
	}
	
	public static FileValidate getInstance(){
		return getInstance(env.getPropertyValue("DATA_SOURCES").split(",")[0]);
	}
	
	public static FileValidate getInstance(String datasourceName){
		if(StringUtils.isEmpty(datasourceName)) return getInstance();
		FileValidate fileValidate = new FileValidate(datasourceName);
		return fileValidate;
	}
	
	public String validateFile(AttachmentBean attachmentBean) throws Exception{
		sizeLimit(attachmentBean.getFile());
		return extLimit(attachmentBean.getFileName());
	}
	
	public String getTableNameByFileName(String fileName){
		String ext = getExtensionName(fileName);
		if(StringUtils.containsIgnoreCase(imageExt, ext)){
			return env.getPropertyValue(datasourceName + ".MONGO_DB_PIC_ADDRESS");
		}else{
			return env.getPropertyValue(datasourceName + ".MONGO_DB_DOC_ADDRESS");
		}
		
	}
	
	/**
	 * 验证文件大小，要求小于、等于4M
	 * @param file
	 * @return
	 * @throws Exception
	 */
	public boolean sizeLimit(File file) throws Exception {
		boolean flag = false;
		FileInputStream fis = null;
        try {
			fis = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}  
        try {
        	limitSize = StringUtils.isBlank(env.getPropertyValue(datasourceName + ".FILE_LIMIT_SIZE")) ?
        			64 : Integer.parseInt(env.getPropertyValue(datasourceName + ".FILE_LIMIT_SIZE"));
			if(Integer.valueOf(String.valueOf(fis.available()/1024)) > limitSize * 1024){
			    flag = true;
			    throw new Exception("The file size must be less than " + limitSize + " Mega.");
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return flag;
	}
	
	/**
	 * 验证文件类型
	 * @param arr
	 * @param str
	 * @return
	 * @throws Exception
	 */
	public String extLimit(String fileName) throws Exception{
		String ext = getExtensionName(fileName);
		for (int i = 0; i < fileExts.length; i++) {
			if (fileExts[i].equalsIgnoreCase(ext)) {
				return ext;
			}
		}
		throw new Exception("The file type \"" + ext + "\" is not allowed.");
	}
	
	public boolean isImage(String fileName){
		String ext = getExtensionName(fileName);
		for (int i = 0; i < imageExts.length; i++) {
			if (imageExts[i].equalsIgnoreCase(ext)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 获取文件扩展名
	 * @param filename
	 * @return
	 */
	public String getExtensionName(String filename) {   
        if ((filename != null) && (filename.length() > 0)) {   
            int dot = filename.lastIndexOf('.');   
            if ((dot >-1) && (dot < (filename.length() - 1))) {   
                return filename.substring(dot + 1);   
            }   
        }   
        return filename;   
    }
	
	public String getNewFileName(String fileExt){
		String name = "";
		Calendar cal = Calendar.getInstance();
		name = String.valueOf(cal.getTimeInMillis());
		name += SnoGerUtil.getRandomNumber(4) + "." + fileExt;// [15 numaber].xxx
		return name;
	}

}
