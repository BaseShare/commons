package com.ctfo.file.dao;

import java.util.List;

import org.mongodb.morphia.Key;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

import com.ctfo.csm.local.DynamicSqlParameter;
import com.mongodb.WriteConcern;

/**
 * 调用mongodb
 * 多数据源：在调用之前需先调用setDatasource方法来设置，否则使用默认数据源
 * @author caishuyang
 *
 * @param <T>
 */
public interface IMongoDao<T extends Object> {
	
	public void save(T entity) throws Exception;
	
	public void save(T entity, WriteConcern wc) throws Exception;
	
	public void insertBatch(Iterable<T> it, Class<T> clazz) throws Exception;
	
	public void insertBatch(Iterable<T> it, Class<T> clazz, WriteConcern wc) throws Exception;
	
	public T get(Class<T> clazz, Object id) throws Exception;
	
	public void delete(Class<T> clazz, Object id) throws Exception;
	
	public void delete(Class<T> clazz, Query<T> query) throws Exception;
	
	public UpdateOperations<T> createUpdateOperations(Class<T> clazz);
	
	public void update(Key<T> key, UpdateOperations<T> operations) throws Exception;
	
	public void update(Query<T> query, UpdateOperations<T> operations) throws Exception;
	
	public Query<T> getQuery(Class<T> clazz);
	
	public List<T> query(Class<T> clazz, Query<T> query) throws Exception;
	
	public List<T> query(Class<T> clazz, DynamicSqlParameter param) throws Exception;
	
	public long getCount();
	
	public long getCount(Query<T> query) throws Exception;
	
	public long getCount(Class<T> clazz, DynamicSqlParameter param) throws Exception;

	public void setDatasource(String datasource);
	
}
