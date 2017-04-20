package com.ctfo.cache.bean;

import java.io.Serializable;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;


@Entity(value="Cache_ParentCache", noClassnameStored=true)
public class ParentCacheBean extends BaseCacheBean implements Serializable{

	private static final long serialVersionUID = -2490511218874114272L;

	@Id
	private String id;
	
	private String serviceName;
	
	/**
	 * true  脏数据
	 * false 正式数据
	 */
	private boolean dirtyOrNot;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public boolean isDirtyOrNot() {
		return dirtyOrNot;
	}

	public void setDirtyOrNot(boolean dirtyOrNot) {
		this.dirtyOrNot = dirtyOrNot;
	}
}
