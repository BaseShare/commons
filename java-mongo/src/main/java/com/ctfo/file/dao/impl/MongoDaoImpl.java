package com.ctfo.file.dao.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

import com.ctfo.csm.local.DynamicSqlParameter;
import com.ctfo.file.dao.IMongoDao;
import com.ctfo.util.JsonUtil;
import com.ctfo.util.MongoDatasource;
import com.mongodb.WriteConcern;

public class MongoDaoImpl<T extends Object> implements IMongoDao<T> {
	
	private final static Log logger = LogFactory.getLog(MongoDaoImpl.class);
	
	private Datastore ds;
	
	private long count;
	
	private String datasource = null;
	
	public void save(T entity) throws Exception {
		save(entity, WriteConcern.NORMAL);
	}
	
	public void save(T entity, WriteConcern wc) throws Exception {
		if(entity == null){
			throw new Exception("entity can not be null!");
		}
		ds = new MongoDatasource(getDatasource()).getDatastore(entity.getClass());
		ds.save(entity, wc);
	}
	
	public void insertBatch(Iterable<T> it, Class<T> clazz) throws Exception{
		insertBatch(it, clazz, WriteConcern.NORMAL);
	}
	
	public void insertBatch(Iterable<T> it, Class<T> clazz, WriteConcern wc) throws Exception{
		if(it == null){
			throw new Exception("entity can not be null!");
		}
		ds = new MongoDatasource(getDatasource()).getDatastore(clazz);
		ds.save(it, wc);
	}
	
	public T get(Class<T> clazz, Object id) throws Exception{
		if(id == null){
			throw new Exception("id can not be null!");
		}
		ds = new MongoDatasource(getDatasource()).getDatastore(clazz);
		if(id instanceof String){
			return ds.createQuery(clazz).field(Mapper.ID_KEY).equal((String)id).get();
		}else if(id instanceof ObjectId){
			return ds.createQuery(clazz).field(Mapper.ID_KEY).equal((ObjectId)id).get();
		}
		return null;
	}
	
	public void delete(Class<T> clazz, Object id) throws Exception{
		if(id == null){
			throw new Exception("id can not be null!");
		}
		ds = new MongoDatasource(getDatasource()).getDatastore(clazz);
		if(id instanceof String){
			ds.delete(ds.createQuery(clazz).field(Mapper.ID_KEY).equal((String)id));
		}else if(id instanceof ObjectId){
			ds.delete(ds.createQuery(clazz).field(Mapper.ID_KEY).equal((ObjectId)id));
		}
	}
	
	public void delete(Class<T> clazz, Query<T> query) throws Exception{
		if (query == null) {
			throw new Exception(
					"parameter named \"query\" is null, " +
					"please call method \"getQuery\" to get a new Query Object first!");
		}
		ds.delete(query);
		
	}
	
	public void update(Key<T> key, UpdateOperations<T> operations) throws Exception{
		ds.update(key, operations);
	}
	
	public UpdateOperations<T> createUpdateOperations(Class<T> clazz){
		ds = new MongoDatasource(getDatasource()).getDatastore(clazz);
		return ds.createUpdateOperations(clazz);
	}
	
	public void update(Query<T> query, UpdateOperations<T> operations) throws Exception{
		ds.update(query, operations);
	}
	
	public List<T> query(Class<T> clazz, Query<T> query) throws Exception {
		if (query == null) {
			throw new Exception(
					"parameter named \"query\" is null, " +
					"please call method \"getQuery\" to get a new Query Object first!");
		}
		this.count = query.countAll();
		logger.debug("调用MongoDao执行query：" + query.toString());
		return query.asList();
	}
	
	public Query<T> getQuery(Class<T> clazz){
		ds = new MongoDatasource(getDatasource()).getDatastore(clazz);
		return (Query<T>) ds.createQuery(clazz);
	}
	
	public List<T> query(Class<T> clazz, DynamicSqlParameter param) throws Exception{
		ds = new MongoDatasource(getDatasource()).getDatastore(clazz);
		Query<T> query = (Query<T>) ds.createQuery(clazz);
		try {
			if(param!=null){
				this.convertParam(query, param);
			}
		} catch (Exception e) {
			logger.error(e);
			throw new Exception(e);
		}
		this.count = query.countAll();
		logger.debug("调用MongoDao执行query：" + query.toString());
		return query.asList();
	}
	
	private void convertParam(Query<T> query, DynamicSqlParameter param){
		if(param.getEqual()!=null){
			Map<String,String> map = param.getEqual();
			Iterator<String> it = map.keySet().iterator();
			while(it.hasNext()){
				String key = it.next();
				if(key!=null && map.get(key)!=null)
					query = query.field(key).equal(map.get(key));
			}
		}
		if(param.getLike()!=null){
			Map<String,String> map = param.getLike();
			Iterator<String> it = map.keySet().iterator();
			while(it.hasNext()){
				String key = it.next();
				if(key!=null && map.get(key)!=null){
					String value = map.get(key);
					value = StringUtils.removeEnd(value, "%");
					value = StringUtils.removeStart(value, "%");
					query = query.field(key).contains(JsonUtil.jsonCharFormat(value));
				}
			}
		}
		if(param.getInMap()!=null){
			Map<String,Object> map = param.getInMap();
			Iterator<String> it = map.keySet().iterator();
			List<String> temList;
			while(it.hasNext()){
				String key = it.next();
				if(key!=null && map.get(key)!=null){
					/**
					 * 此处map.get(key)取到的是一个数组，而query.field(key).in()中要求实现了Iterable接口的类，
					 * 如果在页面中requestParam.inMap.auditState='1,2'这样输入，这里会报类型转换异常，因此这里需要转换。
					 * by malq 2013-03-22
					 * 
					 * 增加数据与集合两种参数形式的兼容性
					 * by HollyLee 2013-04-09
					 */
					Object value = map.get(key);
					if(value instanceof Iterable){
						if(((Iterable<?>) map.get(key)).iterator().hasNext()){
							query = query.field(key).in((Iterable<?>) value);
						}
					}else if(value instanceof String[]){
						temList = new ArrayList<String>();
						for(String val : ((String[])value)[0].split(",")){
							temList.add(val);
						}
						query = query.field(key).in(temList);			
					}
				}
			}
		}
		if(param.getStartwith()!=null){
			Map<String,Object> map = param.getStartwith();
			Iterator<String> it = map.keySet().iterator();
			while(it.hasNext()){
				String key = it.next();
				if(key!=null && map.get(key)!=null)
					query = query.field(key).greaterThanOrEq(map.get(key));
			}
		}
		if(param.getEndwith()!=null){
			Map<String,Object> map = param.getEndwith();
			Iterator<String> it = map.keySet().iterator();
			while(it.hasNext()){
				String key = it.next();
				if(key!=null && map.get(key)!=null)
					query = query.field(key).lessThan(map.get(key));
			}
		}
		if(param.getOrder()!=null){
			if(param.getSort()!=null && 
					param.getSort().equalsIgnoreCase("desc") && 
					!StringUtils.startsWith(param.getOrder(), "-"))
				query.order("-" + param.getOrder());
			else
				query.order(param.getOrder());
		}
		if(param.getRows()!=0 && param.getPage()!=0){
			query.offset((param.getPage() - 1) * param.getRows());
			query.limit(param.getRows());
		}
	}
	
	public long getCount(){
		return this.count;
	}
	
	public long getCount(Query<T> query) throws Exception{
		if (query == null) {
			throw new Exception(
					"parameter named \"query\" is null, " +
					"please call method \"getQuery\" to get a new Query Object first!");
		}
		return query.countAll();
	}
	
	public long getCount(Class<T> clazz, DynamicSqlParameter param) throws Exception{
		if(param==null){
			throw new Exception("param can not be a null value!");
		}
		ds = new MongoDatasource(getDatasource()).getDatastore(clazz);
		Query<T> query = (Query<T>) ds.createQuery(clazz);
		this.convertParam(query, param);
		return query.countAll();
	}

	public String getDatasource() {
		return datasource;
	}

	@Override
	public void setDatasource(String datasource) {
		this.datasource = datasource;
	}

}
