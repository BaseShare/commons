package com.ctfo.file.boss.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.Shape;
import org.mongodb.morphia.query.UpdateOperations;

import com.ctfo.csm.annotations.AnnotationName;
import com.ctfo.csm.local.DynamicSqlParameter;
import com.ctfo.file.bean.PoiBean;
import com.ctfo.file.boss.IMongoService;
import com.ctfo.file.dao.IMongoDao;
import com.ctfo.util.JsonUtil;
import com.mongodb.WriteConcern;

@AnnotationName(name = "【MongoDB】对象管理")
public class MongoServiceImpl<T extends Object> implements IMongoService<T>{
	
	private IMongoDao<T> mongoDao;
	
	private String datasource;

	@Override
	public void save(T entity) throws Exception {
		mongoDao.save(entity);
	}
	
	public void save(T entity, WriteConcern wc) throws Exception{
		mongoDao.save(entity, wc);
	}
	
	@Override
	public void insertBatch(Iterable<T> it, Class<T> clazz) throws Exception{
		mongoDao.insertBatch(it, clazz);
	}
	
	public void insertBatch(Iterable<T> it, Class<T> clazz, WriteConcern wc) throws Exception{
		mongoDao.insertBatch(it, clazz, wc);
	}
	
	@Override
	public void insertBatch(Iterable<T> it) throws Exception{
		mongoDao.insertBatch(it, null);
	}
	@Override
	public T get(Class<T> clazz, Object id) throws Exception{
		return mongoDao.get(clazz, id);
	}
	@Override
	public void delete(Class<T> clazz, Object id) throws Exception{
		mongoDao.delete(clazz, id);
	}
	
	@Override
	public void delete(Class<T> clazz, Query<T> query) throws Exception {
		mongoDao.delete(clazz, query);
	}
	
	@Override
	public UpdateOperations<T> createUpdateOperations(Class<T> clazz){
		return mongoDao.createUpdateOperations(clazz);
	}
	@Override
	public void update(Key<T> key, UpdateOperations<T> operations) throws Exception{
		mongoDao.update(key, operations);
	}
	@Override
	public void update(Query<T> query, UpdateOperations<T> operations) throws Exception{
		mongoDao.update(query, operations);
	}
	@Override
	public Query<T> getQuery(Class<T> clazz){
		return mongoDao.getQuery(clazz);
	}
	@Override
	public Query<T> getQuery(Class<T> clazz, String className, double x, double y, double radius, String queryType){
		Query<T> query = mongoDao.getQuery(clazz);
		query.field("className").equal(className);
		if(!StringUtils.isEmpty(queryType) && queryType.equals(IMongoService.QUERY_TYPE_WITHIN))
			query.field("poi").within(Shape.center(new Shape.Point(x, y), radius/111320d) );
		else
			query.field("poi").near(x, y, radius/111320d);
		return query;
	}
	@Override
	public List<T> query(Class<T> clazz, Query<T> query) throws Exception{
		return mongoDao.query(clazz, query);
	}
	@Override
	public List<T> query(Class<T> clazz, DynamicSqlParameter param) throws Exception{
		return mongoDao.query(clazz, param);
	}
	@Override
	public long getCount(){
		return mongoDao.getCount();
	}
	@Override
	public long getCount(Query<T> query) throws Exception{
		return mongoDao.getCount(query);
	}
	@Override
	public long getCount(Class<T> clazz, DynamicSqlParameter param) throws Exception{
		return mongoDao.getCount(clazz, param);
	}
	@Override
	public List<PoiBean> near(PoiBean entity, double radius, String sort) throws Exception{
		if(entity==null)
			throw new Exception("entity can not be null value!");
		Class clazz = PoiBean.class;
		Query query = mongoDao.getQuery(clazz);
		if(!StringUtils.isEmpty(entity.getType()))
			query.field("type").equal(entity.getType());
		if(!StringUtils.isEmpty(entity.getName()))
			query.field("name").equal(entity.getName());
		if(!StringUtils.isEmpty(entity.getAddress()))
			query.field("address").equal(entity.getAddress());
		if(!StringUtils.isEmpty(entity.getEpid()))
			query.field("epid").equal(entity.getEpid());
//		if(!StringUtils.isEmpty(entity.getWeight()))
//			query.field("weight").equal(entity.getWeight());
		if(!StringUtils.isEmpty(entity.getIcon()))
			query.field("icon").equal(entity.getIcon());
//		if(!StringUtils.isEmpty(entity.getAnchor()))
//			query.field("anchor").equal(entity.getAnchor());
		if(!StringUtils.isEmpty(entity.getClassId()))
			query.field("classId").equal(entity.getClassId());
		if(!StringUtils.isEmpty(sort))
			query.order(sort);
		if(radius<=0)
			query.field("poi").near(entity.getGeo().getCoordinates()[0], entity.getGeo().getCoordinates()[1]);
		else
			query.field("poi").near(entity.getGeo().getCoordinates()[0], entity.getGeo().getCoordinates()[1], radius/111320d);
		query.field("className").equal(entity.getClassName());
		
		return (List<PoiBean>) mongoDao.query(clazz, query);
	}
	@Override
	public List<PoiBean> within(PoiBean entity, double radius, String sort) throws Exception{
		if(entity==null)
			throw new Exception("entity can not be null value!");
		if(radius < 0)
			throw new Exception("radius can not be less than 0!");
		Class clazz = PoiBean.class;
		Query query = mongoDao.getQuery(clazz);
		if(!StringUtils.isEmpty(entity.getType()))
			query.field("type").equal(entity.getType());
		if(!StringUtils.isEmpty(entity.getName()))
			query.field("name").equal(entity.getName());
		if(!StringUtils.isEmpty(entity.getAddress()))
			query.field("address").equal(entity.getAddress());
		if(!StringUtils.isEmpty(entity.getEpid()))
			query.field("epid").equal(entity.getEpid());
//		if(!StringUtils.isEmpty(entity.getWeight()))
//			query.field("weight").equal(entity.getWeight());
		if(!StringUtils.isEmpty(entity.getIcon()))
			query.field("icon").equal(entity.getIcon());
//		if(!StringUtils.isEmpty(entity.getAnchor()))
//			query.field("anchor").equal(entity.getAnchor());
		if(!StringUtils.isEmpty(entity.getClassId()))
			query.field("classId").equal(entity.getClassId());
		if(!StringUtils.isEmpty(sort))
			query.order(sort);
		query.field("poi").within(Shape.center(new Shape.Point(entity.getGeo().getCoordinates()[0],  entity.getGeo().getCoordinates()[1]), radius/111320d) );
		query.field("className").equal(entity.getClassName());
		return (List<PoiBean>) mongoDao.query(clazz, query);
	}
	
	public void convertParamToQuery(Query<T> query, DynamicSqlParameter param){
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
		if(param.getRows()!=1 && param.getPage()!=0){
			query.offset((param.getPage() - 1) * param.getRows());
			query.limit(param.getRows());
		}
	}

	public IMongoDao<T> getMongoDao() {
		return mongoDao;
	}

	public void setMongoDao(IMongoDao<T> mongoDao) {
		this.mongoDao = mongoDao;
	}

	public String getDatasource() {
		return datasource;
	}

	public void setDatasource(String datasource) {
		this.datasource = datasource;
		getMongoDao().setDatasource(datasource);
	}
}
