package com.ctfo.file.boss.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.mongodb.morphia.query.Query;

import com.ctfo.csm.annotations.AnnotationName;
import com.ctfo.file.bean.ClobBean;
import com.ctfo.file.boss.IClobService;
import com.ctfo.file.dao.IMongoDao;
import com.ctfo.util.DateUtil;


@AnnotationName(name = "【MongoDB】Clob管理")
public class ClobServiceImpl implements IClobService {
	
	private IMongoDao<ClobBean> mongoDao;

	/**
	 * 保存Clob、Blob字段
	 * @param uuid 业务模块记录的uuid
	 * @param clobStr Clob、Blob字段
	 * @return	uuid
	 * @throws Exception
	 */
	@Override
	public String addClobStr(String uuid, String clobStr) throws Exception{
		ClobBean entity = new ClobBean();
		entity.setId(uuid);
		entity.setClob(clobStr);
		entity.setTime(DateUtil.getDateStr(DateUtil.DEFAULT_FORMATSTR, new Date()));
		getMongoDao().save(entity);
		return uuid;
	}
	
	/**
	 * 根据uuid更新Clob、Blob字段
	 * @param uuid 业务模块记录的uuid
	 * @param clobStr 新的Clob字段
	 * @return
	 * @throws Exception
	 */
	@Override
	public String updateClobStr(String uuid, String clobStr) throws Exception{
		ClobBean entity = new ClobBean();
		entity.setId(uuid);
		entity.setClob(clobStr);
		entity.setTime(DateUtil.getDateStr(DateUtil.DEFAULT_FORMATSTR, new Date()));
		getMongoDao().save(entity);
		return uuid;
	}
	
	/**
	 * 根据业务模块uuid获取Clob、Blob字段内容
	 * @param uuid 业务模块记录uuid
	 * @return 
	 */
	@Override
	public String findClobByID(String uuid) throws Exception{
		if(uuid == null){
			return "";
		}
		ClobBean entity = getMongoDao().get(ClobBean.class, uuid);
		return entity == null ? null : entity.getClob();
	}
	
	/**
	 * 根据Clob字段模糊查询，返回ID的集合
	 * @param patten 模糊查询的匹配项
	 * @return
	 */
	@Override
	public List<String> findClobsByLike(String patten){
		Query<ClobBean> query = getMongoDao().getQuery(ClobBean.class);
		query.field("clob").contains(patten);
		List<ClobBean> result = new ArrayList<ClobBean>();
		try {
			result = getMongoDao().query(ClobBean.class, query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		List<String> list = new ArrayList<String>();
		for(ClobBean entity : result){
			list.add(entity.getClob());
		}
		return list;
	}

	public IMongoDao<ClobBean> getMongoDao() {
		return mongoDao;
	}

	public void setMongoDao(IMongoDao<ClobBean> mongoDao) {
		this.mongoDao = mongoDao;
	}
	
}
