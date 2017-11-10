package ch.interlis.ioxwkf.dbtools;

public class PgAttributeObject {
	private String attributeName=null;
	private Integer attributeType=null;
	private String attributeTypeName=null;
	
	public String getAttributeName() {
		return attributeName;
	}
	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}
	public Integer getAttributeType() {
		return attributeType;
	}
	public void setAttributeType(Integer attributeType) {
		this.attributeType = attributeType;
	}
	public String getAttributeTypeName() {
		return attributeTypeName;
	}
	public void setAttributeTypeName(String attributeTypeName) {
		this.attributeTypeName = attributeTypeName;
	}
}