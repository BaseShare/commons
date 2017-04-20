package com.ctfo.file.boss.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import com.ctfo.csm.annotations.AnnotationName;
import com.ctfo.file.bean.AttachmentBean;
import com.ctfo.file.bean.ExcelBean;
import com.ctfo.file.bean.ImageSizeBean;
import com.ctfo.file.boss.IFileService;
import com.ctfo.file.dao.IFileDao;
import com.ctfo.file.dao.IMongoDao;
import com.ctfo.file.property.MongoProperties;
import com.ctfo.util.DateUtil;
import com.ctfo.util.FileValidate;
import com.ctfo.util.ImageUtil;
import com.ctfo.util.MongoDatasource;
import com.mongodb.gridfs.GridFSDBFile;

@AnnotationName(name = "【MongoDB】文件管理")
public class FileServiceImpl<T> implements IFileService{
	
	private IFileDao fileDao;
	
	private IMongoDao<AttachmentBean> mongoDao;
	
	private IMongoDao<ExcelBean> mongoExcelDao;
	/**
	 * 上传文件的扩展名
	 */
	private String fileExt;
	
	private String datasource;
	
	//////////////////////////////////////文件操作//////////////////////////////////////////////
	@Override
	public String addFile(AttachmentBean attachmentBean) throws Exception{
		return addFile(attachmentBean, null);
	}
	@Override
	public String addFile(AttachmentBean attachmentBean, ImageSizeBean size) throws Exception{
		return addFile(attachmentBean, size, null);
	}
	@Override
	public String addFile(AttachmentBean attachmentBean, ImageSizeBean size, String fileName) throws Exception{
		if(attachmentBean==null || attachmentBean.getFile()==null || !attachmentBean.getFile().exists()){
			return "";
		}
		if(StringUtils.isEmpty(attachmentBean.getFileName())){
			return "";
		}
		//保存文件
		fileExt = FileValidate.getInstance(getDatasource()).validateFile(attachmentBean);//验证文件大小、类型，返回文件扩展名
		//生成新的文件名
		String tableName = StringUtils.isEmpty(attachmentBean.getClient()) 
				?FileValidate.getInstance(getDatasource()).getTableNameByFileName(attachmentBean.getFileName())
						:attachmentBean.getClient();
		String newFileName = null;
		//生成随机文件名
		String randomName = FileValidate.getInstance(getDatasource()).getNewFileName(fileExt);
		if(StringUtils.isEmpty(fileName)){//如未传入指定文件名，则使用随机文件名
			newFileName = randomName;
		}else{//指定文件名保存文件，覆盖原同名文件
			newFileName = fileName;
			GridFSDBFile dbFile = fileDao.getFileByFileName(newFileName);
			if(dbFile != null){
				fileDao.removeFile(newFileName);
			}
		}
		fileDao.saveFile(attachmentBean.getFile(),attachmentBean.getContentType(), newFileName, tableName);
		
		//保存文件信息
		attachmentBean.setId(newFileName);
		attachmentBean.setTime(DateUtil.getDateStr(DateUtil.DEFAULT_FORMATSTR, new Date()));
		attachmentBean.setUrl(newFileName);
		attachmentBean.setContentType(fileExt);
		mongoDao.save(attachmentBean);
		
		if(size != null && FileValidate.getInstance(getDatasource()).isImage(newFileName)){//加水印，并且文件类型为图片
			//图片加水印、缩放
			dealWithImage(attachmentBean.getFile(), size, newFileName, tableName);
		}
		return newFileName;
	}
	
	/**
	 * 删除文件
	 * @param fileName 文件名
	 * @throws Exception 
	 */
	@Override
	public void removeFile(String fileName) throws Exception{
		if(StringUtils.isEmpty(fileName)) return;
		fileDao.removeFile(fileName);
		mongoDao.delete(AttachmentBean.class, fileName);
	}

	/**
	 * 根据业务模块uuid获取附件对象
	 * @param uuid
	 * @return
	 * @throws Exception
	 */
	@Override
	public AttachmentBean findAttachByID(String uuid) throws Exception{
		return (AttachmentBean) mongoDao.get(AttachmentBean.class, uuid);
	}

	/**
	 * 根据文件名查找文件对象
	 * @param fileName 文件名，例如：134510476148433.jpg
	 * @return
	 */
	@Override
	public GridFSDBFile getFileByFileName(String fileName){
		return fileDao.getFileByFileName(fileName);
	}
	
	/**
	 * 根据文件名获取文件RUL
	 * @param fileName 文件名，如：13472532646557118.jpg
	 * @return
	 */
	@Override
	public String getFileURL(String fileName){
		return getFileURL(fileName, null);
	}
	
	@Override
	public String getFileURL(String fileName, String imageSize){
		if(StringUtils.isEmpty(fileName)) return "";
		String ext = FileValidate.getInstance(getDatasource()).getExtensionName(fileName);
		MongoProperties p = new MongoDatasource(getDatasource()).getProps().get(getDatasource());
		String tableName = p.getDocAddress();//EnvironmentUtil.getInstance().getPropertyValue("MONGO_DB_DOC_ADDRESS");
		if(StringUtils.containsIgnoreCase(p.getPicExt(), ext)){//判断为图片文件
			if(!StringUtils.isEmpty(imageSize)){
				fileName = imageSize + fileName;
			}
			tableName = p.getPicAddress();
		}
		if(!fileName.startsWith("http://")){
			fileName = "http://" 
				+ p.getDomainName()
				+ "/"
				+ tableName
				+ "/"
				+ fileName;
		}
		return fileName;
	}
	
	private void dealWithImage(File file, ImageSizeBean size, String fileName, String tableName) throws Exception{
		//缩略图不加水印，大图、中图加水印
		File thumbFile = null;
		File bigFile = null;
		File midFile = null;
		if(size.getMinWidth()!=0 && size.getMinHeight()!=0){
			if(size.isMinWater())
				thumbFile = ImageUtil.waterMark(file, 0.8f);
			thumbFile = ImageUtil.Thumb(thumbFile==null?file:thumbFile, size.getMinWidth(), size.getMinHeight(), size.isMinThumb());
			fileDao.saveFile(thumbFile,IFileService.IMAGE_MIN + fileName, tableName);
		}
		if(size.getBigWidth()!=0 && size.getBigHeight()!=0){
			if(size.isMaxWater())
				bigFile = ImageUtil.waterMark(file, 0.8f);
			bigFile = ImageUtil.Thumb(bigFile==null?file:bigFile, size.getBigWidth(), size.getBigHeight(), size.isMaxThumb());
			fileDao.saveFile(bigFile,IFileService.IMAGE_MAX + fileName, tableName);
		}
		if(size.getMidWidth()!=0 && size.getMidHeight()!=0){
			if(size.isMidWater())
				midFile = ImageUtil.waterMark(file, 0.8f);
			midFile = ImageUtil.Thumb(midFile==null?file:midFile, size.getMidWidth(), size.getMidHeight(), size.isMidThumb());
			fileDao.saveFile(midFile,IFileService.IMAGE_MID + fileName, tableName);
		}
	}
	
	public IFileDao getFileDao() {
		return fileDao;
	}

	public void setFileDao(IFileDao fileDao) {
		this.fileDao = fileDao;
	}

	public IMongoDao<AttachmentBean> getMongoDao() {
		return mongoDao;
	}

	public void setMongoDao(IMongoDao<AttachmentBean> mongoDao) {
		this.mongoDao = mongoDao;
	}

	public void setMongoExcelDao(IMongoDao<ExcelBean> mongoExcelDao) {
		this.mongoExcelDao = mongoExcelDao;
	}

	public IMongoDao<ExcelBean> getMongoExcelDao() {
		return mongoExcelDao;
	}
	@Override
	public String getDatasource() {
		if(StringUtils.isEmpty(datasource)) datasource = MongoDatasource.getDefaultDatasource();
		return datasource;
	}
	@Override
	public void setDatasource(String datasource) {
		this.datasource = datasource;
		getMongoDao().setDatasource(datasource);
	}
	final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	@Override
	public String addFileByUrl(String url) throws Exception {
		// TODO Auto-generated method stub
		File file =  File.createTempFile(sdf.format(new Date()),  url.substring(url.lastIndexOf("."))) ;
		URL fileUrl = new URL(url);
		InputStream is = fileUrl.openStream();
        
		OutputStream os = new FileOutputStream(file);
        int bytesRead = 0;
        byte[] buffer = new byte[8192];
        while((bytesRead = is.read(buffer,0,8192))!=-1){
        	os.write(buffer,0,bytesRead);
        }
        
        String u = "";
        try {
        	AttachmentBean bean = new AttachmentBean();
    		bean.setFile(file);
    		bean.setFileName(file.getName());
    		u = this.addFile(bean);
		} catch (Exception e) {
			// TODO: handle exception
			throw e;
		} finally {
			try {
    			file.delete();
    		} catch (Exception e) {
    			// TODO: handle exception
    			e.printStackTrace();
    		}
		}
        
		return u;
	}	
}