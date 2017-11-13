package ch.interlis.ioxwkf.dbtools;

public class AttributeDescriptor {
	private String attributeName=null;
	private Integer attributeType=null;
	private String attributeTypeName=null;
	// xml type
	public final static String SET_XML="xml";
	// uuid type
	public final static String SET_UUID="uuid";
	public final static String SET_GEOMETRY_MULTIPOLYGON="MULTIPOLYGON";
	public final static String SET_GEOMETRY_POLYGON="POLYGON";
	public final static String SET_GEOMETRY_MULTILINESTRING="MULTILINESTRING";
	public final static String SET_GEOMETRY_LINESTRING="LINESTRING";
	public final static String SET_GEOMETRY_MULTIPOINT="MULTIPOINT";
	public final static String SET_GEOMETRY_POINT="POINT";
	// geometry types
	public final static String SET_GEOMETRY="geometry";
	
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