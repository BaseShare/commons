package com.ctfo.file.bean;

public class ImageSizeBean implements java.io.Serializable{

	private static final long serialVersionUID = -2269923415797628324L;

	private int minWidth = 0;

	private int minHeight = 0;
	/**
	 * 是否添加水印
	 */
	private boolean minWater = false;
	/**
	 * 是否等比缩放
	 */
	private boolean minThumb = true;
	
	private int midWidth = 0;
	
	private int midHeight = 0;
	/**
	 * 是否添加水印
	 */
	private boolean midWater = false;
	/**
	 * 是否等比缩放
	 */
	private boolean midThumb = true;
	
	private int bigWidth = 0;
	
	private int bigHeight = 0;
	/**
	 * 是否添加水印
	 */
	private boolean maxWater = false;
	/**
	 * 是否等比缩放
	 */
	private boolean maxThumb = true;
	
	public int getMinWidth() {
		return minWidth;
	}
	public void setMinWidth(int minWidth) {
		this.minWidth = minWidth;
	}
	public int getMinHeight() {
		return minHeight;
	}
	public void setMinHeight(int minHeight) {
		this.minHeight = minHeight;
	}
	public boolean isMinWater() {
		return minWater;
	}
	public void setMinWater(boolean minWater) {
		this.minWater = minWater;
	}
	public boolean isMinThumb() {
		return minThumb;
	}
	public void setMinThumb(boolean minThumb) {
		this.minThumb = minThumb;
	}
	public int getMidWidth() {
		return midWidth;
	}
	public void setMidWidth(int midWidth) {
		this.midWidth = midWidth;
	}
	public int getMidHeight() {
		return midHeight;
	}
	public void setMidHeight(int midHeight) {
		this.midHeight = midHeight;
	}
	public boolean isMidWater() {
		return midWater;
	}
	public void setMidWater(boolean midWater) {
		this.midWater = midWater;
	}
	public boolean isMidThumb() {
		return midThumb;
	}
	public void setMidThumb(boolean midThumb) {
		this.midThumb = midThumb;
	}
	public int getBigWidth() {
		return bigWidth;
	}
	public void setBigWidth(int bigWidth) {
		this.bigWidth = bigWidth;
	}
	public int getBigHeight() {
		return bigHeight;
	}
	public void setBigHeight(int bigHeight) {
		this.bigHeight = bigHeight;
	}
	public boolean isMaxWater() {
		return maxWater;
	}
	public void setMaxWater(boolean maxWater) {
		this.maxWater = maxWater;
	}
	public boolean isMaxThumb() {
		return maxThumb;
	}
	public void setMaxThumb(boolean maxThumb) {
		this.maxThumb = maxThumb;
	}

	

}
