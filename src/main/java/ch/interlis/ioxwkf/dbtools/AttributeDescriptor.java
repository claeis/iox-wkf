package ch.interlis.ioxwkf.dbtools;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import ch.interlis.iox.IoxException;

/**<b>AttributeDescriptor</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * Describes an attribute and shows possibilities to set.<br>
 * <p>
 * 
 * <b>Create a new AttributeDescriptor</b><br>
 * <li>AttributeDescriptor attrDesc= new AttributeDescriptor()</li>
 * <p>
 * 
 * <b>(Optional) Setting possibilities</b><br>
 * <table border="1">
 * <tr>
 *   <th>Setting Name</th>
 *   <th>Description</th>
 *   <th>Example</th>
 * </tr>
 * <tr>
 *   <td>iomAttributeName</td>
 *   <td>
 *   	An Attribute of IomObject consists of an Attributename and a Value.
 *   	In this case, the Value could be a String or an Object.<p>
 *   	
 *  	example:
 *   	<li>IomObject iomObj.setattrvalue("attributeName", "attributeValue");</li>
 *   	<li>IomObject iomObj.addattrobj("AttributeName", "AttributeObject");</li>
 *   	<p>
 *		condition:
 *   	<li>If iomAttributeName not set, dbColumnName will be used.</li>
 *   	<li>If iomAttributeName is set, iomAttributeName will be used.</li>
 *   </td>
 *   <td>
 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
 *  	attrDesc.setIomAttributeName(String attributeName)
 *   </td>
 * </tr>
 * <tr>
 *   <td>dbColumnType</td>
 *   <td>
 *   	The type (integer, not the name) of the column in the data base table.<br>
 * 		requirements:<br>
 * 		<li>has to be a type of: java.sql.Types (see: Attachement)</li>
 *   </td>
 *   <td>
 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
 *  	attrDesc.setDbColumnType(Integer attributeType)
 *  </td>
 * </tr>
 * <tr>
 *   <td>dbColumnName</td>
 *   <td>The name (String) of the column in the data base table</td>
 *   <td>
 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
 *  	attrDesc.setDbColumnName(String attributeName)
 *  </td>
 * </tr>
 * <tr>
 *   <td>dbColumnTypeName</td>
 *   <td>The typeName (the name of the integer type) of the column in the data base table.<br>
 *   <p>
 *		dbColumnTypeNames:<br>
 *		<li>DBCOLUMN_TYPENAME_BOOL</li>
 *		<li>DBCOLUMN_TYPENAME_XML</li>
 *		<li>DBCOLUMN_TYPENAME_UUID</li>
 *		<li>DBCOLUMN_TYPENAME_GEOMETRY</li>
 *	 </td>
 *   <td>
 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
 *  	attrDesc.setDbColumnTypeName(String attributeTypeName)
 *  	<p>
 *  	attrDesc.setDbColumnTypeName(AttributeDescriptor.DBCOLUMN_TYPENAME_BOOL)<br>
 *  </td>
 * </tr>
 * <tr>
 *   <td>dbColumnGeomTypeName</td>
 *   <td>
 *   	The typeName (String) of the column of the data base table: geometry_columns.<br>
 * 		 <p>
 * 		 requirements:
 * 		 <li>isGeometry() has to be true</li>
 * 		 <p>
 * 		 dbColumnGeomTypeNames:<br>
 * 		 <li>GEOMETRYTYPE_MULTIPOLYGON</li>
 * 		 <li>GEOMETRYTYPE_POLYGON</li>
 *		 <li>GEOMETRYTYPE_MULTILINESTRING</li>
 * 		 <li>GEOMETRYTYPE_LINESTRING</li>
 * 		 <li>GEOMETRYTYPE_MULTIPOINT</li>
 * 		 <li>GEOMETRYTYPE_POINT</li>
 *	</td>
 *  <td>
 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
 *  	attrDesc.setDbColumnGeomTypeName(String dbColumnGeomTypeName)<br>
 *  	attrDesc.setDbColumnGeomTypeName(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)<br>
 *  </td>
 * </tr>
 * <tr>
 *   <td>coordDimension</td>
 *   <td>The dimension of the coordinates that define this Geometry.
 *   	<p>
 *      requirements:<br>
 * 		<li>isGeometry() has to be true</li><br>
 * 		coordDimenstion can be found in table geometry_columns.
 *   </td>
 *   <td>
 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
 *  	attrDesc.setCoordDimension(Integer coordDimension)
 *  </td>
 * </tr>
 * <tr>
 *   <td>srId</td>
 *   <td>The srId is an integer that uniquely identifies the Spatial Referencing System (SRS) within the database.
 *   	<p>
 *	 	requirements:<br>
 *      <li>isGeometry() has to be true</li><br>
 *      srid can be found in table geometry_columns.
 *   </td>
 *   <td>
 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
 *  	attrDesc.setSrId(Integer srId)
 *  </td>
 * </tr>
 * <tr>
 *   <td>precision</td>
 *   <td>Precision is the number of digits in the not scaled value.</td>
 *   <td>
 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
 *  	attrDesc.setPrecision(Integer precision)
 *   </td>
 * </tr>
 * </table>
 * <p>
 * 
 * <b>Attachement</b><br>
 * <li><a href="https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf">Shapespecification</a></li>
 * <li><a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
 */
public class AttributeDescriptor {
	private String dbColumnName=null;
	private String iomAttributeName=null;
	private Integer attributeType=null;
	private String attributeTypeName=null;
	private String dbColumnGeomTypeName=null;
	private Integer coordDimension=null;
	private Integer srId=null;
	private Integer precision=null;
	private Boolean mandatory=null;
	
	/** The typeName bool is an alias of boolean type.
	 * <p>
	 * get type:<br>
	 * <li>getDbColumnTypeName()</li>
	 */
	public final static String DBCOLUMN_TYPENAME_BOOL="bool";
	
	// JDBC/DB column type name if java.sql.Types.OTHER
	/** xml is a name of JDBC/DB column type.
	 * <p>
	 * requirements:
     * <li>has to be type of: 'java.sql.Types.OTHER'</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnTypeName()</li>
	 */
	public final static String DBCOLUMN_TYPENAME_XML="xml";
	/** uuid is a name of JDBC/DB column type.<br>
	 * the uuid (Universally Unique Identifier) references to the ISO 11578.
	 * <p>
	 * requirements:
     * <li>has to be type of: 'java.sql.Types.OTHER'</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnTypeName()</li>
	 */
	public final static String DBCOLUMN_TYPENAME_UUID="uuid";
	/** geometry is the name of JDBC/DB column type to identify the dataType as geometry.
	 * <p>
	 * requirements:
     * <li>has to be type of: 'java.sql.Types.OTHER'</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnTypeName()</li>
	 */
	public final static String DBCOLUMN_TYPENAME_GEOMETRY="geometry";
	// geometry types
	/** multiPolygon is the name of a type that is used for a multiPolygon.<br>
	 * multiPolygon is a collection of Polygons.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_MULTIPOLYGON="MULTIPOLYGON";
	/** polygon is the name of a type that is used for a polygon.<br>
	 * a polygon is a shape with linear edges, which includes shell and may includes holes.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_POLYGON="POLYGON";
	/** multiLineString is the name of a type that is used for a multiLineString.<br>
	 * a multiLineString could be one or more LineStrings.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_MULTILINESTRING="MULTILINESTRING";
	/** lineString is the name of a type that is used for a lineString.<br>
	 * lineString is a sequence of line segments.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_LINESTRING="LINESTRING";
	/** multiPoint is the name of a type that is used for a multiPoint.<br>
	 * multipoint is a geometric object consisting of one or more points.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_MULTIPOINT="MULTIPOINT";
	/** point is the name of a type that is used for a point.<br>
	 * point is a geometric object consisting of one point.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_POINT="POINT";
	
	public final static String GEOMETRYTYPE_COMPOUNDCURVE = "COMPOUNDCURVE";
	public final static String GEOMETRYTYPE_CURVEPOLYGON = "CURVEPOLYGON";
	
	private final static String GEOMCOLUMNS_COLUMN_TYPE="type";
	private final static String GEOMCOLUMNS_COLUMN_SRID="srid";
	private final static String GEOMCOLUMNS_COLUMN_DIMENSION="coord_dimension";
	
	/**  <td>The name (String) of the column in the data base table</td>
	 *   <p>
	 *   <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setDbColumnName(String attributeName)
	 *   </td>
	 *   <p>
	 *   @return dbColumnName
	 */
	protected String getDbColumnName() {
		return dbColumnName;
	}
	/**  <td>The name (String) of the column in the data base table</td>
	 *   <p>
	 *   <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setDbColumnName(String attributeName)
	 *   </td>
	 *   <p>
	 *   @param attributeName
	 */
	protected void setDbColumnName(String attributeName) {
		this.dbColumnName = attributeName;
	}
	/**  <td>
	 *   	An Attribute of IomObject consists of an Attributename and a Value.
	 *   	In this case, the Value could be a String or an Object.<p>
	 *   	
	 *  	example:
	 *   	<li>IomObject iomObj.setattrvalue("attributeName", "attributeValue");</li>
	 *   	<li>IomObject iomObj.addattrobj("AttributeName", "AttributeObject");</li>
	 *   	<p>
	 *		condition:
	 *   	<li>If iomAttributeName not set, dbColumnName will be used.</li>
	 *   	<li>If iomAttributeName is set, iomAttributeName will be used.</li>
	 *   </td>
	 *   <p>
	 *   <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setIomAttributeName(String attributeName)
	 *   </td>
	 *   @return iomAttributeName if !=null or dbColumnName
	 */
	protected String getIomAttributeName() {
		return iomAttributeName==null? dbColumnName : iomAttributeName;
	}
	/**  <td>
	 *   	An Attribute of IomObject consists of an Attributename and a Value.
	 *   	In this case, the Value could be a String or an Object.<p>
	 *   	
	 *  	example:
	 *   	<li>IomObject iomObj.setattrvalue("attributeName", "attributeValue");</li>
	 *   	<li>IomObject iomObj.addattrobj("AttributeName", "AttributeObject");</li>
	 *   	<p>
	 *		condition:
	 *   	<li>If iomAttributeName not set, dbColumnName will be used.</li>
	 *   	<li>If iomAttributeName is set, iomAttributeName will be used.</li>
	 *   </td>
	 *   <p>
	 *   <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setIomAttributeName(String attributeName)
	 *   </td>
	 *   @param attributeName
	 */
	protected void setIomAttributeName(String attributeName) {
		this.iomAttributeName = attributeName;
	}
	/** get the type of JDBC/DB column.
	 * <p>
	 * requirements:
     * <li>has to be a type of: <a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
     * <p>
	 * @return attributeType
	 */
	protected Integer getDbColumnType() {
		return attributeType;
	}
	/** set data base column type.
	 * <p>
	 * requirements:
     * <li>has to be a type of: <a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
     * <p>
	 * @param attributeType
	 */
	protected void setDbColumnType(Integer attributeType) {
		this.attributeType = attributeType;
	}
	/** <td>The typeName (the name of the integer type) of the column in the data base table.<br>
	 *  <p>
	 *		dbColumnTypeNames:<br>
	 *		<li>DBCOLUMN_TYPENAME_BOOL</li>
	 *		<li>DBCOLUMN_TYPENAME_XML</li>
	 *		<li>DBCOLUMN_TYPENAME_UUID</li>
	 *		<li>DBCOLUMN_TYPENAME_GEOMETRY</li>
	 *	</td>
	 *	<p>
	 *  <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setDbColumnTypeName(String attributeTypeName)
	 *  	<p>
	 *  	attrDesc.setDbColumnTypeName(AttributeDescriptor.DBCOLUMN_TYPENAME_BOOL)<br>
	 *  </td>
	 *  <p>
	 *  requirements:
     *  <li>isGeometry() has to be true</li>
     *  <p>
	 *  @return attributeTypeName
	 */
	protected String getDbColumnTypeName() {
		return attributeTypeName;
	}
	/** <td>The typeName (the name of the integer type) of the column in the data base table.<br>
	 *  <p>
	 *		dbColumnTypeNames:<br>
	 *		<li>DBCOLUMN_TYPENAME_BOOL</li>
	 *		<li>DBCOLUMN_TYPENAME_XML</li>
	 *		<li>DBCOLUMN_TYPENAME_UUID</li>
	 *		<li>DBCOLUMN_TYPENAME_GEOMETRY</li>
	 *	</td>
	 *	<p>
	 *  <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setDbColumnTypeName(String attributeTypeName)
	 *  	<p>
	 *  	attrDesc.setDbColumnTypeName(AttributeDescriptor.DBCOLUMN_TYPENAME_BOOL)<br>
	 *  </td>
	 *  <p>
	 *  requirements:
     *  <li>isGeometry() has to be true</li>
     *  <p>
	 *  @param attributeTypeName
	 */
	protected void setDbColumnTypeName(String attributeTypeName) {
		this.attributeTypeName = attributeTypeName;
	}
	/** <td>
	 *   	The typeName (String) of the column of the data base table: geometry_columns.<br>
	 * 		 <p>
	 * 		 requirements:
	 * 		 <li>isGeometry() has to be true</li>
	 * 		 <p>
	 * 		 dbColumnGeomTypeNames:<br>
	 * 		 <li>GEOMETRYTYPE_MULTIPOLYGON</li>
	 * 		 <li>GEOMETRYTYPE_POLYGON</li>
	 *		 <li>GEOMETRYTYPE_MULTILINESTRING</li>
	 * 		 <li>GEOMETRYTYPE_LINESTRING</li>
	 * 		 <li>GEOMETRYTYPE_MULTIPOINT</li>
	 * 		 <li>GEOMETRYTYPE_POINT</li>
	 *	 </td>
	 *	 <p>
	 *   <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setDbColumnGeomTypeName(String dbColumnGeomTypeName)<br>
	 *  	attrDesc.setDbColumnGeomTypeName(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)<br>
	 *   </td>
     * 	 @return dbColumnGeomTypeName
	 */
	protected String getDbColumnGeomTypeName() {
		return dbColumnGeomTypeName;
	}
	/**  <td>
	 *   	The typeName (String) of the column of the data base table: geometry_columns.<br>
	 * 		 <p>
	 * 		 requirements:
	 * 		 <li>isGeometry() has to be true</li>
	 * 		 <p>
	 * 		 dbColumnGeomTypeNames:<br>
	 * 		 <li>GEOMETRYTYPE_MULTIPOLYGON</li>
	 * 		 <li>GEOMETRYTYPE_POLYGON</li>
	 *		 <li>GEOMETRYTYPE_MULTILINESTRING</li>
	 * 		 <li>GEOMETRYTYPE_LINESTRING</li>
	 * 		 <li>GEOMETRYTYPE_MULTIPOINT</li>
	 * 		 <li>GEOMETRYTYPE_POINT</li>
	 *	 </td>
	 *	 <p>
	 *   <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setDbColumnGeomTypeName(String dbColumnGeomTypeName)<br>
	 *  	attrDesc.setDbColumnGeomTypeName(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)<br>
	 *   </td>
	 *   <p>
     * 	 @param dbColumnGeomTypeName
	 */
	protected void setDbColumnGeomTypeName(String dbColumnGeomTypeName) {
		this.dbColumnGeomTypeName = dbColumnGeomTypeName;
	}
	/** get the coordinate dimension.<br>
	 * the dimension of the coordinates that define this Geometry.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * coordDimenstion can be found in table geometry_columns.
     * @return coordDimension
	 */
	protected Integer getCoordDimension() {
		return coordDimension;
	}
	/** set the coordinate dimension.<br>
	 * the dimension of the coordinates that define this Geometry.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li><br>
     * coordDimenstion can be found in table geometry_columns.
     * @param coordDimension
	 */
	protected void setCoordDimension(Integer coordDimension) {
		this.coordDimension = coordDimension;
	}
	/** get the srid.<br>
	 * the srId is an integer value that uniquely identifies the Spatial Referencing System (SRS) within the database.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li><br>
     * srid can be found in table geometry_columns.
     * @return srId
	 */
	protected Integer getSrId() {
		return srId;
	}
	/** <td>The srId is an integer that uniquely identifies the Spatial Referencing System (SRS) within the database.
	 *   	<p>
	 *	 	requirements:<br>
	 *      <li>isGeometry() has to be true</li><br>
	 *      srid can be found in table geometry_columns.
	 *  </td>
	 *  <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setSrId(Integer srId)
	 *  </td>
     * @param srId
	 */
	protected void setSrId(Integer srId) {
		this.srId = srId;
	}
	/**  <td>get the designated column's specified column size. For numeric data, this is the maximum precision.
	 *   </td>
	 *   <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setPrecision(Integer precision)
	 *   </td>
	 * @return precision
	 */
	protected Integer getPrecision() {
		return precision;
	}
	/**  <td>get the designated column's specified column size. For numeric data, this is the maximum precision.
	 *   </td>
	 *   <td>
	 *  	AttributeDescriptor attrDesc= new AttributeDescriptor()<br>
	 *  	attrDesc.setPrecision(Integer precision)
	 *   </td>
	 * @param precision
	 */
	protected void setPrecision(Integer precision) {
		this.precision = precision;
	}
	
	/** add geometry data to geometry attribute in attribute descriptors.
	 * @param schemaName
	 * @param tableName
	 * @param attributeDesc
	 * @param db
	 * @return final list of attribute descriptors
	 * @throws SQLException 
	 * @throws IoxException 
	 */
	protected static List<AttributeDescriptor> addGeomDataToAttributeDescriptors(String schemaName, String tableName, List<AttributeDescriptor> attributeDesc, Connection db) throws SQLException, IoxException {
		for(AttributeDescriptor attr:attributeDesc) {
			if(attr.getDbColumnTypeName().equals(DBCOLUMN_TYPENAME_GEOMETRY)) {
				ResultSet tableInDb =null;
				StringBuilder queryBuild=new StringBuilder();
				queryBuild.append("SELECT "+GEOMCOLUMNS_COLUMN_DIMENSION+","+GEOMCOLUMNS_COLUMN_SRID+","+ GEOMCOLUMNS_COLUMN_TYPE+" FROM geometry_columns WHERE ");
				if(schemaName!=null) {
					queryBuild.append("f_table_schema='"+schemaName+"' AND ");
				}
				queryBuild.append("f_table_name='"+tableName+"' "); 
				queryBuild.append("AND f_geometry_column='"+attr.getDbColumnName()+"';");
				try {
					Statement stmt = db.createStatement();
					tableInDb=stmt.executeQuery(queryBuild.toString());
				} catch (SQLException e) {
					throw new SQLException(e);
				}
				
				tableInDb.next();
				
				attr.setCoordDimension(tableInDb.getInt(GEOMCOLUMNS_COLUMN_DIMENSION));
				attr.setSrId(tableInDb.getInt(GEOMCOLUMNS_COLUMN_SRID));
				attr.setDbColumnGeomTypeName(tableInDb.getString(GEOMCOLUMNS_COLUMN_TYPE));
			}
		}
		return attributeDesc;
	}
	
	/** create selection to table inside schema, create and return a list of attribute descriptors.
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
				attr.setPrecision(rsmd.getPrecision(k));
				attr.setDbColumnName(rsmd.getColumnName(k));
				attr.setDbColumnType(rsmd.getColumnType(k));
				attr.setDbColumnTypeName(rsmd.getColumnTypeName(k));
				int nullable = rsmd.isNullable(k);
				if(nullable == ResultSetMetaData.columnNullable) {
					attr.setMandatory(false);
				}else {
					attr.setMandatory(true);
				}
				
				attrs.add(attr);
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		try {
			addGeomDataToAttributeDescriptors(schemaName, tableName, attrs, db);
		}catch(SQLException e) {
			throw new IoxException(e);
		}
		return attrs;
	}
	
	/** check if this is a geometry type.
     * <p>
	 * get type:
	 * <li>getDbColumnGeomTypeName()</li>
	 * <p>
	 * @return true if datatype is a geometry, false if not.
	 */
	public boolean isGeometry() {
		return attributeType==Types.OTHER && attributeTypeName!=null && attributeTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_GEOMETRY);
	}
	
	public Boolean isMandatory() {
		return mandatory;
	}
	
	public void setMandatory(Boolean mandatory) {
		this.mandatory = mandatory;
	}
}