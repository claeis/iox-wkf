package ch.interlis.ioxwkf.dbtools;

public class AttributePool {
	private String attributeName=null;
	private Integer attributeType=null;
	private String attributeTypeName=null;
	private Integer srid=null;
	private Integer coordDimension=0;
	private String geoColumnTypeName=null;
	
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
	public Integer getSrid() {
		return srid;
	}
	public void setSrid(Integer srid) {
		this.srid = srid;
	}
	public Integer getCoordDimension() {
		return coordDimension;
	}
	public void setCoordDimension(Integer coordDimension) {
		this.coordDimension = coordDimension;
	}
	public String getGeoColumnTypeName() {
		return geoColumnTypeName;
	}
	public void setGeoColumnTypeName(String geoColumnTypeName) {
		this.geoColumnTypeName = geoColumnTypeName;
	}
}