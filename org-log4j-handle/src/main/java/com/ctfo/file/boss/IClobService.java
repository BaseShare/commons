package com.ctfo.file.boss;

import java.util.List;

public interface IClobService {

	/**
	 * 保存Clob、Blob字段
	 * @param uuid 业务模块记录的uuid
	 * @param clobStr Clob、Blob字段
	 * @return	uuid
	 * @throws Exception
	 */
	public String addClobStr(String uuid, String clobStr) throws Exception;
	
	/**
	 * 根据uuid更新Clob、Blob字段
	 * @param uuid 业务模块记录的uuid
	 * @param clobStr 新的Clob字段
	 * @return
	 * @throws Exception
	 */
	public String updateClobStr(String uuid, String clobStr) throws Exception;
	
	/**
	 * 根据业务模块uuid获取Clob、Blob字段内容
	 * @param uuid 业务模块记录uuid
	 * @return 
	 */
	public String findClobByID(String uuid) throws Exception;
	
	/**
	 * 根据Clob字段模糊查询，返回ID的集合
	 * @param patten 模糊查询的匹配项
	 * @return
	 */
	public List<String> findClobsByLike(String patten);
}
