package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;
import java.util.Map.Entry;
import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.converter.ConverterException;
import ch.ehi.ili2pg.converter.PostgisColumnConverter;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxException;

public abstract class AbstractImport2db {
	private PostgisColumnConverter pgConverter=new PostgisColumnConverter();
	private Integer srsCode=Config.SET_DEFAULT_SRSCODE;
	
	private static final String DATATYPENAME_GEOMETRY="geometry";
	private static final String DBCOLUMNNAME_SRID="srid";
	private static final String DBCOLUMNNAME_DIMENSION="coord_dimension";
	private static final String DBCOLUMNNAME_TYPE="type";
	
	private static final String GEOM_DATATYPENAME_POINT="POINT";
	private static final String GEOM_DATATYPENAME_MULTIPOINT="MULTIPOINT";
	private static final String GEOM_DATATYPENAME_LINESTRING="LINESTRING";
	private static final String GEOM_DATATYPENAME_MULTILINESTRING="MULTILINESTRING";
	private static final String GEOM_DATATYPENAME_POLYGON="POLYGON";
	private static final String GEOM_DATATYPENAME_MULTIPOLYGON="MULTIPOLYGON";
	
	public AbstractImport2db() {};
	
	/** import Data to database.
	 * @param file
	 * @param db
	 * @param config
	 * @throws IoxException 
	 * @throws SQLException 
	 */
	public abstract void importData(File file,Connection db,Settings config) throws SQLException, IoxException;

	/** insert attribute-values of IomObject from attribute-names of data base table,
	 * to data base (existing schema, else default schema) in contained table (if exists),
	 * via given data base connection details.
	 * @param schemaName
	 * @param tableName
	 * @param attributes
	 * @param db
	 * @param iomObj
	 * @throws IoxException
	 * @throws SQLException
	 * @throws ConverterException 
	 */
	protected void insertIntoTable(String schemaName, String tableName, Map<String, PgAttributeObject> attrsPool, Connection db, IomObject iomObj) throws IoxException, SQLException, ConverterException {
		StringBuffer queryBuild=new StringBuffer();
		
		// create insert statement
		queryBuild.append("INSERT INTO ");
		if(schemaName!=null) {
			queryBuild.append(schemaName);
			queryBuild.append(".");
		}
		queryBuild.append(tableName);
		queryBuild.append("(");
		String comma="";
		StringBuilder attrsNotInserted=new StringBuilder();
		for(Entry<String, PgAttributeObject> attribute:attrsPool.entrySet()) {
			PgAttributeObject attrObject=attribute.getValue();
			String attrName=attrObject.getAttributeName();
			attrsNotInserted.append(comma);
			queryBuild.append(comma);
			comma=", ";
			attrsNotInserted.append(attrName);
			queryBuild.append(attrName);
		}
		queryBuild.append(")VALUES(");
		comma="";
		String geoColumnTypeName=null;
		int position=1;
		ResultSet geomColumnTableInDb=null;
		for(Entry<String, PgAttributeObject> attribute:attrsPool.entrySet()) {
			PgAttributeObject attrObject=attribute.getValue();
			String attrName=attrObject.getAttributeName();
			Integer datatype=attrObject.getAttributeType();
			// dataTypeName
			String geoColumnTypeGeom=attrObject.getAttributeTypeName();
			
			queryBuild.append(comma);
			if(datatype.equals(Types.OTHER)) {
				if(geoColumnTypeGeom!=null && geoColumnTypeGeom.equals(DATATYPENAME_GEOMETRY)) {
					// attribute names of database table
					try {
						geomColumnTableInDb=openGeometryColumnTableInDb(schemaName, tableName, attrName, db);
					}catch(Exception e) {
						throw new IoxException(e);
					}
					while(geomColumnTableInDb.next()) {
						geoColumnTypeName=geomColumnTableInDb.getString(DBCOLUMNNAME_TYPE);
						srsCode=geomColumnTableInDb.getInt(DBCOLUMNNAME_SRID);
					}
					if(geoColumnTypeName.equals(GEOM_DATATYPENAME_POINT)) {
						queryBuild.append(pgConverter.getInsertValueWrapperCoord("?", srsCode));
					}else if(geoColumnTypeName.equals(GEOM_DATATYPENAME_MULTIPOINT)) {
						queryBuild.append(pgConverter.getInsertValueWrapperCoord("?", srsCode));
					}else if(geoColumnTypeName.equals(GEOM_DATATYPENAME_LINESTRING)) {
						queryBuild.append(pgConverter.getInsertValueWrapperPolyline("?", srsCode));
					}else if(geoColumnTypeName.equals(GEOM_DATATYPENAME_MULTILINESTRING)) {
						queryBuild.append(pgConverter.getInsertValueWrapperMultiPolyline("?", srsCode));
					}else if(geoColumnTypeName.equals(GEOM_DATATYPENAME_POLYGON)) {
						queryBuild.append(pgConverter.getInsertValueWrapperSurface("?", srsCode));
					}else if(geoColumnTypeName.equals(GEOM_DATATYPENAME_MULTIPOLYGON)) {
						queryBuild.append(pgConverter.getInsertValueWrapperMultiSurface("?", srsCode));
					}
				}else {
					queryBuild.append("?");
				}
			}else {
				queryBuild.append("?");
			}
			comma=", ";
		}
		queryBuild.append(")");
		PreparedStatement ps=null;
		try {
			ps = db.prepareStatement(queryBuild.toString());
			ps.clearParameters();
		}catch(Exception e) {
			throw new IoxException(e);
		}
		
		position=1;
		for(Entry<String, PgAttributeObject> attribute:attrsPool.entrySet()) {
			PgAttributeObject attrObject=attribute.getValue();
			String dataTypeName=attrObject.getAttributeTypeName();
			Integer dataType=attrObject.getAttributeType();
			String attrName=attrObject.getAttributeName();
			String attrValue=iomObj.getattrvalue(attrName);
			IomObject value=null;
			// dataTypeName
			String geoColumnTypeGeom=attrObject.getAttributeTypeName();
			Integer coordDimension=0;
			boolean is3D=false;
			
			if(attrValue==null || attrValue.isEmpty()){
				value=iomObj.getattrobj(attrName,0);
			}
			
			if((attrValue!=null && !attrValue.isEmpty()) || value!=null){
				if(dataType.equals(Types.OTHER)) {
					if(geoColumnTypeGeom!=null && geoColumnTypeGeom.equals(DATATYPENAME_GEOMETRY)) {
						// attribute names of database table
						try {
							geomColumnTableInDb=openGeometryColumnTableInDb(schemaName, tableName, attrName, db);
						}catch(Exception e) {
							throw new IoxException(e);
						}
						while(geomColumnTableInDb.next()) {
							geoColumnTypeName=geomColumnTableInDb.getString(DBCOLUMNNAME_TYPE);
							coordDimension=geomColumnTableInDb.getInt(DBCOLUMNNAME_DIMENSION);
							srsCode=geomColumnTableInDb.getInt(DBCOLUMNNAME_SRID);
						}
						if(coordDimension==3) {
							is3D=true;
						}else {
							is3D=false;
						}
						// point
						if(geoColumnTypeName.equals(Config.SET_GEOMETRY_POINT)) {
							ps.setObject(position, pgConverter.fromIomCoord(value, srsCode, is3D));
						// multipoint
						}else if(geoColumnTypeName.equals(Config.SET_GEOMETRY_MULTIPOINT)) {
							ps.setObject(position, pgConverter.fromIomCoord(value, srsCode, is3D));
						// line
						}else if(geoColumnTypeName.equals(Config.SET_GEOMETRY_LINESTRING)) {
							ps.setObject(position, pgConverter.fromIomPolyline(value, srsCode, is3D, 0));
						// multiline
						}else if(geoColumnTypeName.equals(Config.SET_GEOMETRY_MULTILINESTRING)) {
							ps.setObject(position, pgConverter.fromIomMultiPolyline(value, srsCode, is3D, 0));
						// polygon
						}else if(geoColumnTypeName.equals(Config.SET_GEOMETRY_POLYGON)) {
							ps.setObject(position, pgConverter.fromIomSurface(value, srsCode, false, is3D, 0));
						// multipolygon
						}else if(geoColumnTypeName.equals(Config.SET_GEOMETRY_MULTIPOLYGON)) {
							ps.setObject(position, pgConverter.fromIomMultiSurface(value, srsCode, false, is3D, 0));
						}
					}else {
						// uuid
						if(dataTypeName.equals(Config.SET_UUID)) {
							ps.setObject(position, pgConverter.fromIomUuid(attrValue));
						// xml	
						}else if(dataTypeName.equals(Config.SET_XML)) {
							ps.setObject(position, pgConverter.fromIomXml(attrValue));
						}
					}
				}else {
					if(dataType.equals(Types.BIT)) {
						if(dataTypeName.equals("bool")) {
							if(attrValue.equals("t")
									|| attrValue.equals("true")
									|| attrValue.equals("y")
									|| attrValue.equals("yes")
									|| attrValue.equals("on")
									|| attrValue.equals("1")) {
								ps.setObject(position, "t", Types.BOOLEAN);
							}else {
								ps.setObject(position, "f", Types.BOOLEAN);
							}
						}else {
							ps.setObject(position, attrValue.charAt(0), Types.BIT);
						}
					}else if(dataType.equals(Types.BLOB)) {
						Blob b = db.createBlob();
					    InputStream out = b.getBinaryStream(position, Long.valueOf(attrValue));
						ps.setBinaryStream(position, out);
					}else if(dataType.equals(Types.BINARY)) {
						ps.setByte(position, Byte.valueOf(attrValue));
					}else if(dataType.equals(Types.NUMERIC)) {
						ps.setObject(position, attrValue, Types.NUMERIC);
					}else if(dataType.equals(Types.SMALLINT)) {
						ps.setObject(position, attrValue, Types.SMALLINT);
					}else if(dataType.equals(Types.TINYINT)) {
						ps.setObject(position, attrValue, Types.TINYINT);
					}else if(dataType.equals(Types.INTEGER)) {
						ps.setObject(position, attrValue, Types.INTEGER);
					}else if(dataType.equals(Types.BIGINT)) {
						ps.setObject(position, attrValue, Types.BIGINT);
					}else if(dataType.equals(Types.FLOAT)) {
						ps.setFloat(position, Float.valueOf(attrValue));
					}else if(dataType.equals(Types.DOUBLE)) {
						ps.setDouble(position, Double.valueOf(attrValue));
					}else if(dataType.equals(Types.LONGNVARCHAR)) {
						ps.setLong(position, Long.valueOf(attrValue));
					}else if(dataType.equals(Types.DECIMAL)) {
						Long decLong=Long.valueOf(attrValue);
						ps.setBigDecimal(position, java.math.BigDecimal.valueOf(decLong));
					}else if(dataType.equals(Types.CHAR)) {
						ps.setObject(position, attrValue, Types.CHAR);
					}else if(dataType.equals(Types.VARCHAR)) {
						ps.setObject(position, attrValue, Types.VARCHAR);
					}else if(dataType.equals(Types.LONGVARCHAR)) {
						ps.setObject(position, attrValue, Types.LONGVARCHAR);
					}else if(dataType.equals(Types.BOOLEAN)) {
						ps.setObject(position, attrValue, Types.BOOLEAN);
					}else if(dataType.equals(Types.DECIMAL)) {
						Long decLong=Long.valueOf(attrValue);
						ps.setBigDecimal(position, java.math.BigDecimal.valueOf(decLong));
					}else if(dataType.equals(Types.DATE)) {
						// year format: year-1900, month-1 (0-11)
						String[] date=attrValue.split("T|\\-|\\.|\\,|\\:");
						ps.setDate(position, new Date(Integer.valueOf(date[0])-1900, Integer.valueOf(date[1])-1, Integer.valueOf(date[2])));
					}else if(dataType.equals(Types.TIME)) {
						String[] time=attrValue.split("T|\\-|\\.|\\,|\\:");
						ps.setTime(position, new Time(Integer.valueOf(time[0]), Integer.valueOf(time[1]), Integer.valueOf(time[2])));
					}else if(dataType.equals(Types.TIMESTAMP)) {
						Calendar cal=null;
						String[] dateTime=attrValue.split("T|\\-|\\.|\\,|\\:");
						// year format: year-1900, month-1 (0-11)
						ps.setTimestamp(position, new Timestamp(Integer.valueOf(dateTime[0])-1900, Integer.valueOf(dateTime[1])-1, Integer.valueOf(dateTime[2]), Integer.valueOf(dateTime[3]), Integer.valueOf(dateTime[4]), Integer.valueOf(dateTime[5]), Integer.valueOf(dateTime[6])), cal);
					}else {
						ps.setObject(position, attrValue, dataType);
					}
				}
			}else {
				ps.setNull(position, dataType);
			}
			position+=1;
		}
		try {
			int rs = ps.executeUpdate();
			if(rs==0) {
				if(schemaName!=null) {
					throw new IoxException("import of "+attrsNotInserted.toString()+" to "+schemaName+"."+tableName+" failed");
				}else {
					throw new IoxException("import of "+attrsNotInserted.toString()+" to "+tableName+" failed");
				}
			}
		}catch(SQLException e) {
			throw new IoxException(e);
		}
	}
	
	/** create selection to table inside schema and get resultset of data base table.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return resultset of db table
	 * @throws IoxException
	 */
	public ResultSet openTableInDb(String schemaName, String tableName, Connection db) throws IoxException {
		ResultSet rs =null;
		StringBuffer queryBuild=new StringBuffer();
		queryBuild.append("SELECT * FROM ");
		
		if(schemaName!=null) {
			queryBuild.append(schemaName+".");
		}
		queryBuild.append(tableName+";");
		
		try {
			Statement stmt = db.createStatement();
			rs=stmt.executeQuery(queryBuild.toString());
			if(rs==null) {
				throw new IoxException("table "+schemaName+"."+tableName+" not found");
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		return rs;
	}
	
	/** get resultset of geometry columns.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return resultset of geometry columns.
	 * @throws IoxException
	 */
	public ResultSet openGeometryColumnTableInDb(String schemaName, String tableName, String attrName, Connection db) throws IoxException {
		ResultSet rs =null;
		StringBuffer queryBuild=new StringBuffer();
		queryBuild.append("SELECT * FROM geometry_columns WHERE ");
		if(schemaName!=null) {
			queryBuild.append("f_table_schema='"+schemaName+"' AND ");
		}
		queryBuild.append("f_table_name='"+tableName+"' "); 
		queryBuild.append("AND f_geometry_column='"+attrName+"';");
		try {
			Statement stmt = db.createStatement();
			rs=stmt.executeQuery(queryBuild.toString());
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		return rs;
	}
}