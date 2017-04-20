package com.ctfo.file.bean;

import java.io.Serializable;

import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.annotations.Transient;
import org.mongodb.morphia.utils.IndexDirection;

import com.ctfo.file.bean.Geo;



@Entity(value="bpois", noClassnameStored=true)
public class PoiBean implements Serializable{
	
	@Transient
	private static final long serialVersionUID = 3971063032010939325L;

	@Id
	private String _id;
	
	@Property("class_name")
	private String className;
	
	@Property("id")
	private String classId;
	
	@Embedded
	private Geo geo;
	
	private String type;
	
	@Indexed(IndexDirection.ASC)
	private String epid;
	
	@Indexed(IndexDirection.ASC)
	private String name;
	
	@Indexed(IndexDirection.ASC)
	private String address;
	
	@Indexed(IndexDirection.ASC)
	private String kind;
	
	private int weight = 1;
	
	private String icon;
	
	private int anchor = 4;
	
	/**
	 * 
	 * @param className 表名/对象名
	 * @param classId 表ID/对象ID
	 * @param x 经度
	 * @param y 纬度
	 */
	public PoiBean(String className, String classId, double x, double y){
		this.className = className;
		this.classId = classId;
		this.geo = new Geo(x, y);
	}

	protected PoiBean(){
		
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getClassId() {
		return classId;
	}

	public void setClassId(String classId) {
		this.classId = classId;
	}

	public String getEpid() {
		return epid;
	}

	public void setEpid(String epid) {
		this.epid = epid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String getIcon() {
		return icon;
	}

	public void setIcon(String icon) {
		this.icon = icon;
	}

	public int getAnchor() {
		return anchor;
	}

	public void setAnchor(int anchor) {
		this.anchor = anchor;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Geo getGeo() {
		return geo;
	}

	public void setGeo(Geo geo) {
		this.geo = geo;
	}

	public String getKind() {
		return kind;
	}

	public void setKind(String kind) {
		this.kind = kind;
	}

	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}
}
