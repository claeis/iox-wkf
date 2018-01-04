package ch.interlis.ioxwkf.dbtools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import ch.interlis.iox.IoxException;

/** the AttributeDescriptor class contains attribute types used within a feature. Attribute types are distinguished by 2 main types:
 * <p>
 * <li>A geometry type includes dimensions of coordinates, as well as the appropriate srid.</li>
 * <li>An attribute type is distinguished by 'primitive' data types and 'other' data types, which fall under the term: 'OTHER' like uuid or xml for example.</li>
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
	
	// JDBC/DB column type name if java.sql.Types.OTHER
	/** bool is an additional type name of JDBC/DB to identify the dataType boolean and is defined as char type.
	 * <br>
	 * valid boolean values are:
	 * <li>'true'</li><li>'false'</li><li>'t'</li><li>'f'</li><li>'1'</li><li>'0'</li>
	 * <br>
	 * if boolean is true(t) and the JDBC/DB column type name is bool, then it is a boolean type. else could be char (1,0).
	 */
	public final static String DBCOLUMN_TYPENAME_BOOL="bool";
	/** xml fall under the term: 'OTHER'.<br>
	 * the first character must be a letter or underscore, followed by
	 * letters, numbers, periods, minus signs or underscores.<br>
	 * no colons aloud here.<p>
	 * @see <a href="www.w3.org/TR/REC-xml">www.w3.org/TR/REC-xml.</a>
	 */
	public final static String DBCOLUMN_TYPENAME_XML="xml";
	/** uuid's fall under the term: 'OTHER'.<br>
	 * They are defined by 36 characters in format of: ISO 11578.<br>
	 * The first character must be a letter or underscore, followed by
	 * letters, numbers, periods, minus signs or underscores.<br>
	 * No colons aloud here.<p>
	 * @see <a href="www.w3.org/TR/REC-xml">www.w3.org/TR/REC-xml.</a>
	 */
	public final static String DBCOLUMN_TYPENAME_UUID="uuid";
	/** geometry is the type name of JDBC/DB to identify the dataType as geometry.
	 */
	public final static String DBCOLUMN_TYPENAME_GEOMETRY="geometry";
	// geometry types
	/** multiPolygon is a collection of Polygons.<br>
	 * As per the OGC SFS specification, the Polygons in a MultiPolygon may not overlap,
	 * and may only touch at single points. This allows the topological point-set
	 * semantics to be well-defined.
	 */
	public final static String GEOMETRYTYPE_MULTIPOLYGON="MULTIPOLYGON";
	/** represents a polygon with linear edges, which may include holes.
	 * The outer boundary (shell) and inner boundaries (holes) of the polygon are represented by LinearRings.
	 * The boundary rings of the polygon may have any orientation.
	 * Polygons are closed and simple geometries by definition.
	 */
	public final static String GEOMETRYTYPE_POLYGON="POLYGON";
	/** a MultiLineString is defined by one or more LineStrings,
	 * referenced through lineString elements.
	 */
	public final static String GEOMETRYTYPE_MULTILINESTRING="MULTILINESTRING";
	/** a sequence of line segments, each having a parameterization like the one LineSegment.
	 * The class essentially combines a List of LineSegment into a single object.
	 */
	public final static String GEOMETRYTYPE_LINESTRING="LINESTRING";
	/** an aggregate class containing only instances of Point.
	 * The association role element shall be the set of points
	 * contained in this MultiPoint.
	 */
	public final static String GEOMETRYTYPE_MULTIPOINT="MULTIPOINT";
	/** a basic data type for a geometric object consisting of one point.
	 */
	public final static String GEOMETRYTYPE_POINT="POINT";
	
	private final static String GEOMCOLUMNS_COLUMN_TYPE="type";
	private final static String GEOMCOLUMNS_COLUMN_SRID="srid";
	private final static String GEOMCOLUMNS_COLUMN_DIMENSION="coord_dimension";
	
	protected String getDbColumnName() {
		return dbColumnName;
	}
	protected void setDbColumnName(String attributeName) {
		this.dbColumnName = attributeName;
	}
	protected String getIomAttributeName() {
		return iomAttributeName==null? dbColumnName : iomAttributeName;
	}
	protected void setIomAttributeName(String attributeName) {
		this.iomAttributeName = attributeName;
	}
	protected Integer getDbColumnType() {
		return attributeType;
	}
	protected void setDbColumnType(Integer attributeType) {
		this.attributeType = attributeType;
	}
	protected String getDbColumnTypeName() {
		return attributeTypeName;
	}
	protected void setDbColumnTypeName(String attributeTypeName) {
		this.attributeTypeName = attributeTypeName;
	}
	protected String getDbColumnGeomTypeName() {
		return dbColumnGeomTypeName;
	}
	protected void setDbColumnGeomTypeName(String dbColumnGeomTypeName) {
		this.dbColumnGeomTypeName = dbColumnGeomTypeName;
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
	protected Integer getPrecision() {
		return precision;
	}
	protected void setPrecision(Integer precision) {
		this.precision = precision;
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
				attr.setPrecision(rsmd.getPrecision(k));
				attr.setDbColumnName(rsmd.getColumnName(k));
				attr.setDbColumnType(rsmd.getColumnType(k));
				attr.setDbColumnTypeName(rsmd.getColumnTypeName(k));
				attrs.add(attr);
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		return attrs;
	}
	
	/** check if current datatype is part of geometry.
	 * @return true if datatype is part of geometry, false if not.
	 */
	public boolean isGeometry() {
		return attributeType==Types.OTHER && attributeTypeName!=null && attributeTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_GEOMETRY);
	}
}