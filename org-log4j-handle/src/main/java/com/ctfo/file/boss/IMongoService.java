package com.ctfo.file.boss;

import java.util.List;

import org.mongodb.morphia.Key;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

import com.ctfo.csm.local.DynamicSqlParameter;
import com.ctfo.file.bean.PoiBean;
import com.mongodb.WriteConcern;

public interface IMongoService<T extends Object> {
	
	public static final String QUERY_TYPE_NEAR = "near";
	
	public static final String QUERY_TYPE_WITHIN = "within";

	/**
	 * 保存对象至mongodb，ID字段不可为null或空，insertOrUpdate
	 * @param entity 对象对象
	 * @throws Exception
	 */
	public void save(T entity) throws Exception;
	
	public void save(T entity, WriteConcern wc) throws Exception;
	
	/**
	 * @param it
	 * @param clazz
	 * @throws Exception
	 */
	public void insertBatch(Iterable<T> it, Class<T> clazz) throws Exception;
	
	public void insertBatch(Iterable<T> it, Class<T> clazz, WriteConcern wc) throws Exception;
	
	/**
	 * 批量插入数据
	 * @param it 
	 * @throws Exception
	 */
	public void insertBatch(Iterable<T> it) throws Exception;
	
	/**
	 * 根据ID获取对象
	 * @param clazz 要获取对象的类型
	 * @param id 
	 * @return
	 * @throws Exception
	 */
	public T get(Class<T> clazz, Object id) throws Exception;
	
	/**
	 * 根据ID删除对象
	 * @param clazz 要删除对象的类型
	 * @param id
	 * @throws Exception
	 */
	public void delete(Class<T> clazz, Object id) throws Exception;
	
	/**
	 * 根据query条件删除对象
	 * @param clazz 要删除对象的类型
	 * @param query
	 * @throws Exception
	 */
	public void delete(Class<T> clazz, Query<T> query) throws Exception;
	
	public UpdateOperations<T> createUpdateOperations(Class<T> clazz);
	
	public void update(Key<T> key, UpdateOperations<T> operations) throws Exception;
	
	public void update(Query<T> query, UpdateOperations<T> operations) throws Exception;
	
	/**
	 * 初始化查询条件对象
	 * @param clazz
	 * @return
	 */
	public Query<T> getQuery(Class<T> clazz);
	
	/**
	 * 初始化空间查询对象
	 * @param clazz 实体类型
	 * @param className 表名/对象名
	 * @param classId 对象ID
	 * @param x 经度
	 * @param y 纬度
	 * @param radius 距离/半径
	 * @param queryType 查询类型，取值：IMongoService.QUERY_TYPE_WITHIN 或  IMongoService.QUERY_TYPE_NEAR
	 * @return
	 */
	public Query<T> getQuery(Class<T> clazz, String className, double x, double y, double radius, String queryType);
	
	/**
	 * 根据条件查询对象
	 * @param clazz 要获取对象的类型
	 * @param query 查询条件
	 * @return 查询结果的集合
	 * @throws Exception
	 */
	public List<T> query(Class<T> clazz, Query<T> query) throws Exception;
	
	/**
	 * 根据条件查询对象，支持的查询类型有【=】,【like】,【in】,【>=】,【<=】
	 * @param clazz 要获取对象的类型
	 * @param param 查询条件
	 * @return 查询结果的集合
	 * @throws Exception
	 */
	public List<T> query(Class<T> clazz, DynamicSqlParameter param) throws Exception;
	
	/**
	 * 在调用query后，调用此方法可获得查询的总数
	 * @return
	 */
	public long getCount();
	
	/**
	 * 根据条件查询数量
	 * @param query
	 * @return
	 * @throws Exception
	 */
	public long getCount(Query<T> query) throws Exception;
	
	/**
	 * 根据条件查询数量
	 * @param clazz
	 * @param param
	 * @return
	 * @throws Exception
	 */
	public long getCount(Class<T> clazz, DynamicSqlParameter param) throws Exception;
	
	/**
	 * 将Parameter转化成Query
	 * @param query
	 * @param param
	 */
	public void convertParamToQuery(Query<T> query, DynamicSqlParameter param);
	
	/**
	 * 空间查询，near查询根据距离提供查询结果，按照从近到远排序
	 * @param entity 查询条件
	 * @param radius 距离，半径
	 * @param sort 排序条件
	 * @return
	 */
	public List<PoiBean> near(PoiBean entity, double radius, String sort) throws Exception;
	
	/**
	 * 空间查询
	 * @param entity 查询条件
	 * @param radius 距离，半径
	 * @param sort 排序条件
	 * @return
	 */
	public List<PoiBean> within(PoiBean entity, double radius, String sort) throws Exception;

	/**
	 * 设置Mongodb数据源名称
	 * @param datasource 数据源名称，对应mongodb.properties中的DATA_SOURCES=BUSINESS,LOGS中的一个。
	 */
	public void setDatasource(String datasource);
	
	public String getDatasource();

}
