package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.converter.ConverterException;
import ch.ehi.ili2pg.converter.PostgisColumnConverter;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;
import ch.interlis.iox.ObjectEvent;

public abstract class AbstractImport2db {
	
	/** list of attribute descriptors.
	 */
	private ArrayList<AttributeDescriptor> attrs=new ArrayList<AttributeDescriptor>();
	
	/** list of attribute which not were found.
	 */
	private ArrayList<String> attrsNotFound=new ArrayList<String>();
	
	/** postgis column converter to convert to db and back.
	 */
	private PostgisColumnConverter pgConverter=new PostgisColumnConverter();
	
	/** srsCode: set default value.
	 */
	private Integer srsCode=IoxWkfConfig.SETTING_SRSCODE_DEFAULT;
	
	/** create a reader in the appropriate format.
	 * @param file
	 * @param config
	 * @return reader
	 * @throws IoxException
	 */
	protected abstract IoxReader createReader(File file, Settings config) throws IoxException;
	
	/** import from file to data base.
	 * @param file to write to.
	 * @param connection to db.
	 * @param config to set by user.
	 * @throws IoxException
	 */
	public void importData(File file,Connection db,Settings config) throws IoxException {
		
		/** validity of connection
		 */
		if(db==null) {
			throw new IoxException("connection==null.");
		}else {
			EhiLogger.logState("connection to database: <success>.");
		}
		
		/** optional: set database schema, if table is not in default schema.
		 */
		String definedSchemaName=config.getValue(IoxWkfConfig.SETTING_DBSCHEMA);
		if(definedSchemaName==null) {
			EhiLogger.logState("no db schema name defined, get default schema.");
		}else {
			EhiLogger.logState("db schema name: <"+definedSchemaName+">.");
		}
		/** mandatory: set database table to insert data into.
		 */
		String definedTableName=config.getValue(IoxWkfConfig.SETTING_DBTABLE);
		if(definedTableName==null) {
			throw new IoxException("database table==null.");
		}else {
			EhiLogger.logState("db table name: <"+definedTableName+">.");
		}
				
		/** create appropriate IoxReader.
		 */
		IoxReader reader=createReader(file, config);
		
		/** read IoxEvents
		 */
		IoxEvent event=reader.read();
		EhiLogger.logState("start import");
		while(event instanceof IoxEvent){
			if(event instanceof ObjectEvent) {
				IomObject iomObj=((ObjectEvent)event).getIomObject();
				
				// table validity
				ResultSet tableInDb=null;
				if(config.getValue(IoxWkfConfig.SETTING_DBTABLE)!=null){
					// attribute names of database table
					try {
						tableInDb=openTableInDb(definedSchemaName, definedTableName, db);
					}catch(Exception e) {
						throw new IoxException("table "+definedTableName+" not found");
					}
				}else {
					throw new IoxException("expected tablename");
				}
				
				ResultSetMetaData rsmd;
				try {
					rsmd = tableInDb.getMetaData();
					for(int k=1;k<rsmd.getColumnCount()+1;k++) {
						String columnName=rsmd.getColumnName(k);
						int columnType=rsmd.getColumnType(k);
						String columnTypeName=rsmd.getColumnTypeName(k);
						
						for(int i=0;i<iomObj.getattrcount();i++) {
							if(columnName.equals(iomObj.getattrname(i))){
								String attrValue=iomObj.getattrvalue(iomObj.getattrname(i));
								if(attrValue==null) {
									attrValue=iomObj.getattrobj(iomObj.getattrname(i), 0).toString();
								}
								if(attrValue!=null) {
									AttributeDescriptor attrData=new AttributeDescriptor();
									attrData.setAttributeName(iomObj.getattrname(i));
									attrData.setAttributeType(columnType);
									attrData.setAttributeTypeName(columnTypeName);
									attrs.add(attrData);
								}
							}else {
								attrsNotFound.add(iomObj.getattrname(i));
							}
						}
					}
				} catch (SQLException e1) {
					throw new IoxException(e1);
				}
				// insert attributes to database
				try {
					try {
						insertIntoTable(definedSchemaName, definedTableName, db,iomObj);
					} catch (SQLException e) {
						throw new IoxException(e);
					}
				} catch (ConverterException e) {
					throw new IoxException("import failed"+e);
				}
				event=reader.read();
			}else {
				// next IoxEvent
				event=reader.read();
			}
		}
		EhiLogger.logState("end of import");
		EhiLogger.logState("import successful");
		
		// close reader
		if(reader!=null) {
			reader.close();
			reader=null;
		}
		event=null;
	}

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
	protected void insertIntoTable(String schemaName, String tableName, Connection db, IomObject iomObj) throws IoxException, SQLException, ConverterException {
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
		for(AttributeDescriptor attribute:attrs) {
			String attrName=attribute.getAttributeName();
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
		for(AttributeDescriptor attribute:attrs) {
			String attrName=attribute.getAttributeName();
			Integer datatype=attribute.getAttributeType();
			// dataTypeName
			String geoColumnTypeGeom=attribute.getAttributeTypeName();
			
			queryBuild.append(comma);
			if(datatype.equals(Types.OTHER)) {
				if(geoColumnTypeGeom!=null && geoColumnTypeGeom.equals(AttributeDescriptor.SET_GEOMETRY)) {
					// attribute names of database table
					try {
						geomColumnTableInDb=openGeometryColumnTableInDb(schemaName, tableName, attrName, db);
					}catch(Exception e) {
						throw new IoxException(e);
					}
					while(geomColumnTableInDb.next()) {
						geoColumnTypeName=geomColumnTableInDb.getString(AttributeDescriptor.SET_TYPE);
						srsCode=geomColumnTableInDb.getInt(AttributeDescriptor.SET_SRID);
					}
					if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POINT)) {
						queryBuild.append(pgConverter.getInsertValueWrapperCoord("?", srsCode));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOINT)) {
						queryBuild.append(pgConverter.getInsertValueWrapperMultiCoord("?", srsCode));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_LINESTRING)) {
						queryBuild.append(pgConverter.getInsertValueWrapperPolyline("?", srsCode));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTILINESTRING)) {
						queryBuild.append(pgConverter.getInsertValueWrapperMultiPolyline("?", srsCode));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POLYGON)) {
						queryBuild.append(pgConverter.getInsertValueWrapperSurface("?", srsCode));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOLYGON)) {
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
		}catch(Exception e) {
			throw new IoxException(e);
		}
		
		// clear parameters
		ps.clearParameters();
			
		position=1;
		for(AttributeDescriptor attribute:attrs) {
			String dataTypeName=attribute.getAttributeTypeName();
			Integer dataType=attribute.getAttributeType();
			String attrName=attribute.getAttributeName();
			String attrValue=iomObj.getattrvalue(attrName);
			IomObject value=null;
			// dataTypeName
			String geoColumnTypeGeom=attribute.getAttributeTypeName();
			Integer coordDimension=0;
			boolean is3D=false;
			
			if(attrValue==null || attrValue.isEmpty()){
				value=iomObj.getattrobj(attrName,0);
			}
			
			if((attrValue!=null && !attrValue.isEmpty()) || value!=null){
				if(dataType.equals(Types.OTHER)) {
					if(geoColumnTypeGeom!=null && geoColumnTypeGeom.equals(AttributeDescriptor.SET_GEOMETRY)) {
						// attribute names of database table
						try {
							geomColumnTableInDb=openGeometryColumnTableInDb(schemaName, tableName, attrName, db);
						}catch(Exception e) {
							throw new IoxException(e);
						}
						while(geomColumnTableInDb.next()) {
							geoColumnTypeName=geomColumnTableInDb.getString(AttributeDescriptor.SET_TYPE);
							coordDimension=geomColumnTableInDb.getInt(AttributeDescriptor.SET_DIMENSION);
							srsCode=geomColumnTableInDb.getInt(AttributeDescriptor.SET_SRID);
						}
						if(coordDimension==3) {
							is3D=true;
						}else {
							is3D=false;
						}
						// point
						if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POINT)) {
							ps.setObject(position, pgConverter.fromIomCoord(value, srsCode, is3D));
						// multipoint
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOINT)) {
							ps.setObject(position, pgConverter.fromIomMultiCoord(value, srsCode, is3D));
						// line
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_LINESTRING)) {
							ps.setObject(position, pgConverter.fromIomPolyline(value, srsCode, is3D, 0));
						// multiline
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTILINESTRING)) {
							ps.setObject(position, pgConverter.fromIomMultiPolyline(value, srsCode, is3D, 0));
						// polygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POLYGON)) {
							ps.setObject(position, pgConverter.fromIomSurface(value, srsCode, false, is3D, 0));
						// multipolygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOLYGON)) {
							ps.setObject(position, pgConverter.fromIomMultiSurface(value, srsCode, false, is3D, 0));
						}
					}else {
						// uuid
						if(dataTypeName.equals(AttributeDescriptor.SET_UUID)) {
							ps.setObject(position, pgConverter.fromIomUuid(attrValue));
						// xml	
						}else if(dataTypeName.equals(AttributeDescriptor.SET_XML)) {
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