package ch.interlis.ioxwkf.dbtools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import ch.interlis.iox.IoxException;

public class AttributeDescriptor {
	private String attributeName=null;
	private Integer attributeType=null;
	private String attributeTypeName=null;
	private String geomColumnTypeName=null;
	private Integer coordDimension=null;
	private Integer srId=null;
	
	// setting possibilities
	public final static String SET_XML="xml";
	public final static String SET_UUID="uuid";
	public final static String SET_SRID="srid";
	public final static String SET_DIMENSION="coord_dimension";
	public final static String SET_UDTNAME="udt_name";
	public final static String SET_COLUMNNAME="column_name";
	public final static String SET_TYPE="type";
	public final static String SET_BOOL="bool";
	// geometry type
	public final static String SET_GEOMETRY="geometry";
	public final static String SET_GEOMETRY_MULTIPOLYGON="MULTIPOLYGON";
	public final static String SET_GEOMETRY_POLYGON="POLYGON";
	public final static String SET_GEOMETRY_MULTILINESTRING="MULTILINESTRING";
	public final static String SET_GEOMETRY_LINESTRING="LINESTRING";
	public final static String SET_GEOMETRY_MULTIPOINT="MULTIPOINT";
	public final static String SET_GEOMETRY_POINT="POINT";
	
	protected String getAttributeName() {
		return attributeName;
	}
	protected void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}
	protected Integer getAttributeType() {
		return attributeType;
	}
	protected void setAttributeType(Integer attributeType) {
		this.attributeType = attributeType;
	}
	protected String getAttributeTypeName() {
		return attributeTypeName;
	}
	protected void setAttributeTypeName(String attributeTypeName) {
		this.attributeTypeName = attributeTypeName;
	}
	protected String getGeomColumnTypeName() {
		return geomColumnTypeName;
	}
	protected void setGeomColumnTypeName(String geomColumnTypeName) {
		this.geomColumnTypeName = geomColumnTypeName;
	}
	protected Integer getCoordDimension() {
		return coordDimension;
	}
	protected void setCoordDimension(Integer coordDimension) {
		this.coordDimension = coordDimension;
	}
	protected Integer getSrId() {
		return srId;
	}
	protected void setSrId(Integer srId) {
		this.srId = srId;
	}
	/** add geometry data to geometry attribute in given attribute descriptors.
	 * @param schemaName
	 * @param tableName
	 * @param attributeDesc
	 * @param db
	 * @return final list of attribute descriptors
	 * @throws SQLException 
	 */
	protected static List<AttributeDescriptor> addGeomDataToAttributeDescriptors(String schemaName, String tableName, List<AttributeDescriptor> attributeDesc, Connection db) throws SQLException {
		for(AttributeDescriptor attr:attributeDesc) {
			if(attr.getAttributeTypeName().equals(SET_GEOMETRY)) {
				ResultSet tableInDb =null;
				StringBuilder queryBuild=new StringBuilder();
				queryBuild.append("SELECT coord_dimension, srid, type FROM geometry_columns WHERE ");
				if(schemaName!=null) {
					queryBuild.append("f_table_schema='"+schemaName+"' AND ");
				}
				queryBuild.append("f_table_name='"+tableName+"' "); 
				queryBuild.append("AND f_geometry_column='"+attr.getAttributeName()+"';");
				try {
					Statement stmt = db.createStatement();
					tableInDb=stmt.executeQuery(queryBuild.toString());
				} catch (SQLException e) {
					throw new SQLException(e);
				}
				tableInDb.next();
				attr.setCoordDimension(tableInDb.getInt(SET_DIMENSION));
				attr.setSrId(tableInDb.getInt(SET_SRID));
				attr.setGeomColumnTypeName(tableInDb.getString(SET_TYPE));
			}
		}
		return attributeDesc;
	}
	/** create selection to table inside schema, create list of attribute descriptors and get it back.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return list of attribute descriptors.
	 * @throws IoxException
	 */
	protected static List<AttributeDescriptor> getAttributeDescriptors(String schemaName, String tableName, Connection db) throws IoxException {
		List<AttributeDescriptor> attrs=new ArrayList<AttributeDescriptor>();
		ResultSet tableInDb =null;
		StringBuilder queryBuild=new StringBuilder();
		queryBuild.append("SELECT * FROM ");
		if(schemaName!=null) {
			queryBuild.append(schemaName+".");
		}
		queryBuild.append(tableName+" WHERE 1<>1;");
		try {
			Statement stmt = db.createStatement();
			tableInDb=stmt.executeQuery(queryBuild.toString());
			if(tableInDb==null) {
				throw new IoxException("table "+schemaName+"."+tableName+" not found");
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		ResultSetMetaData rsmd;
		try {
			rsmd = tableInDb.getMetaData();
			for(int k=1;k<rsmd.getColumnCount()+1;k++) {
				tableInDb.next();
				// create attr descriptor
				AttributeDescriptor attr=new AttributeDescriptor();
				attr.setAttributeName(rsmd.getColumnName(k));
				attr.setAttributeType(rsmd.getColumnType(k));
				attr.setAttributeTypeName(rsmd.getColumnTypeName(k));
				attrs.add(attr);
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		return attrs;
	}
}