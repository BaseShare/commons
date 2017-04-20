package com.ctfo.cache.bean;

import java.io.Serializable;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.utils.IndexDirection;



@Entity(value="Cache_AllianceStore", noClassnameStored=true)
public class AllianceStoreCacheBean extends BaseCacheBean implements Serializable {

	private static final long serialVersionUID = 6194809730252827931L;
	
	/**
	 * UUID
	 */
	@Id
	private String id;
	
	/**
	 * 商户编码
	 */
	private String alliancestoreCode;
	
	/**
	 * 商户名称
	 */
	private String alliancestoreName;
	
	/**
	 * 省份
	 */
	private String province;
	
	/**
	 * 城市
	 */
	private String city;
	
	/**
	 * 区域
	 */
	private String area;
	
	/**
	 * 详细地址
	 */
	private String address;
	
	/**
	 * 注册手机号
	 */
	private String mobile;
	
	/**
	 * 门店数量
	 */
	private Long countPortal;
	
	/**
	 * 会员数量
	 */
	private Long countMember;
	
	/**
	 * 合同数量
	 */
	private Long countContract;
	
	/**
	 * 分成利率
	 */
	private Double bonus;
	
	/**
	 * 分成状态
	 */
	private String bonusState;
	
	/**
	 * 审核状态
	 */
	private String auditState;
	
	/**
	 * 业务员姓名
	 */
	private String operName;
	
	/**
	 * 创建时间
	 */
	@Indexed(value=IndexDirection.ASC)
	private Long createTime;
	
	/**
	 * 最后更新时间
	 */
	@Indexed
	private Long updateTime;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getAlliancestoreCode() {
		return alliancestoreCode;
	}

	public void setAlliancestoreCode(String alliancestoreCode) {
		this.alliancestoreCode = alliancestoreCode;
	}

	public String getAlliancestoreName() {
		return alliancestoreName;
	}

	public void setAlliancestoreName(String alliancestoreName) {
		this.alliancestoreName = alliancestoreName;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public Long getCountPortal() {
		return countPortal;
	}

	public void setCountPortal(Long countPortal) {
		this.countPortal = countPortal;
	}

	public Long getCountMember() {
		return countMember;
	}

	public void setCountMember(Long countMember) {
		this.countMember = countMember;
	}

	public Long getCountContract() {
		return countContract;
	}

	public void setCountContract(Long countContract) {
		this.countContract = countContract;
	}

	public Double getBonus() {
		return bonus;
	}

	public void setBonus(Double bonus) {
		this.bonus = bonus;
	}

	public String getBonusState() {
		return bonusState;
	}

	public void setBonusState(String bonusState) {
		this.bonusState = bonusState;
	}

	public String getAuditState() {
		return auditState;
	}

	public void setAuditState(String auditState) {
		this.auditState = auditState;
	}

	public String getOperName() {
		return operName;
	}

	public void setOperName(String operName) {
		this.operName = operName;
	}

	public Long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Long createTime) {
		this.createTime = createTime;
	}

	public Long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Long updateTime) {
		this.updateTime = updateTime;
	}

}
