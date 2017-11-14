package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.converter.ConverterException;
import ch.ehi.ili2pg.converter.PostgisColumnConverter;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxWriter;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;

public abstract class AbstractExportFromdb {
	
	/** list of attribute descriptors.
	 */
	private ArrayList<AttributeDescriptor> attrs=new ArrayList<AttributeDescriptor>();
	
	/** number of iomObjects which were created.
	 */
	private int objectCount=0;
	
	private IoxFactoryCollection factory;
	
	/** postgis column converter to convert to db and back.
	 */
	private PostgisColumnConverter pgConverter=new PostgisColumnConverter();
	
	/** srsCode: set default value.
	 */
	private Integer srsCode=IoxWkfConfig.SETTING_SRSCODE_DEFAULT;
	
	/** default model content.
	 */
	private static final String MODELNAME="model";
	
	/** default topic content.
	 */
	private static final String TOPICNAME="topic";
	
	/** create a writer in the appropriate format.
	 * @param file to write to.
	 * @param config to set by user.
	 * @return writer object.
	 * @throws IoxException
	 */
	protected abstract IoxWriter createWriter(File file, Settings config) throws IoxException;
	
	/** export from data base table to file.
	 * @param file to write to.
	 * @param connection to db.
	 * @param config to set by user.
	 * @throws IoxException
	 */
	public void exportData(File file,Connection db,Settings config) throws IoxException {
		/** data base connection has not to be null.
		 */
		if(db==null) {
			throw new IoxException("connection==null");
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

		/** create selection to get information about attributes of target data base table.
		 */
		ResultSet dbTable=null;
		try {
			dbTable=openTableInDb(definedSchemaName, definedTableName, db);
			if(definedSchemaName!=null) {
				EhiLogger.logState("db table <"+definedTableName+"> inside db schema <"+definedSchemaName+">: exist.");
			}else {
				EhiLogger.logState("db table <"+definedTableName+"> inside default db schema: exist.");
			}
		}catch(Exception e) {
			if(definedSchemaName!=null) {
				throw new IoxException("db table <"+definedTableName+"> inside db schema <"+definedSchemaName+">: not found.",e);
			}else{
				throw new IoxException("db table "+definedTableName+" inside default db schema: not found.",e);
			}
		}
		
		/** set attribute data to target attribute, to create wrapper selection.
		 */
		ResultSetMetaData metadataDbTable;
		try {
			metadataDbTable = dbTable.getMetaData();
		} catch (SQLException e1) {
			throw new IoxException(e1);
		}
		try {
			for(int k=1;k<metadataDbTable.getColumnCount()+1;k++) {
				// columnName
				String columnName=metadataDbTable.getColumnName(k);
				// columnType
				int columnType=metadataDbTable.getColumnType(k);
				// columnTypeName
				String columnTypeName=metadataDbTable.getColumnTypeName(k);
				
				// set PG Attribute Object data.
				AttributeDescriptor attr=new AttributeDescriptor();
				attr.setAttributeName(columnName);
				attr.setAttributeType(columnType);
				attr.setAttributeTypeName(columnTypeName);
				
				// put attribute to attribute descriptor list.
				attrs.add(attr);
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		
		/** create selection for appropriate datatypes.
		 *  geometry datatypes are wrapped from pg to ili.
		 */
		ResultSet pg2IliConvertedTable=null;
		try {
			pg2IliConvertedTable=openPgToIliConvertedTableInDb(definedSchemaName, definedTableName, db);
		}catch(IoxException e) {
			throw new IoxException(e);
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		
		/** The final IomObjects will be send in ioxObjectEvents and written as individual records to the given file.
		 */
		EhiLogger.logState("start transfer to file.");
		EhiLogger.logState("start to write records.");
		
		IomObject iomObject=null;
		
		/** create appropriate IoxWriter.
		 */
		IoxWriter writer=createWriter(file, config);
		
		// add attribute value, converted in appropriate type, list of attribute descriptors.
		try {
			while(pg2IliConvertedTable.next()) {
				// create iomObjects
				try {
					iomObject=getRecordsAsIomObjects(definedSchemaName,definedTableName, MODELNAME, TOPICNAME, pg2IliConvertedTable, db);
				} catch (IoxException e) {
					throw new IoxException(e);
				}
				if(iomObject.getattrcount()==0) {
					throw new IoxException("no data found to export to file.");
				}
				try {
					writer.write(new ch.interlis.iox_j.ObjectEvent(iomObject));
				}catch(Exception e) {
					throw new IoxException("export of: <"+iomObject.getobjecttag()+"> to file: <"+file.getAbsolutePath()+"> failed.",e);
				}
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		EhiLogger.logState("conversion of attributes: <successful>.");
		writer.write(new EndBasketEvent());
		writer.write(new EndTransferEvent());
		
		/** close writer if open.
		 */
		if(writer!=null) {
			writer.close();
			writer=null;
		}
		EhiLogger.logState("end transfer to file.");
		EhiLogger.logState("export: <successful>.");
	}
	
	/** select table in database and get attribute data.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return resultset of db table.
	 * @throws IoxException
	 * @throws SQLException 
	 */
	private ResultSet openTableInDb(String schemaName, String tableName, Connection db) throws IoxException, SQLException {
		StringBuffer queryBuild=new StringBuffer();
		queryBuild.append("SELECT * FROM ");
		if(schemaName!=null) {
			queryBuild.append(schemaName+".");
		}
		queryBuild.append(tableName+";");
		Statement stmt = db.createStatement();
		ResultSet rs=stmt.executeQuery(queryBuild.toString());
		return rs;
	}
	
	/** get resultset of geometry columns.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return resultset of geometry columns.
	 * @throws IoxException
	 */
	private ResultSet openGeometryColumnTableInDb(String schemaName, String tableName, String attrName, Connection db) throws IoxException {
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
	
	/** create a selection and wrapp all geometries, xml and blob datatypes from pg to ili format.
	 * @param schemaName
	 * @param tableName
	 * @param attrsPool
	 * @param db
	 * @return
	 * @throws IoxException
	 * @throws SQLException
	 * @throws ConverterException
	 */
	private ResultSet openPgToIliConvertedTableInDb(String schemaName, String tableName, Connection db) throws IoxException, SQLException {
		// create selection.
		StringBuilder selectionQueryBuild=new StringBuilder();
		String comma="";
		selectionQueryBuild.append("SELECT ");
		// attribute data type conversion.
		for(AttributeDescriptor attr:attrs) {
			// attrName
			String attrName=attr.getAttributeName();
			// dataType
			Integer datatype=attr.getAttributeType();
			// dataTypeName
			String geoColumnTypeGeom=attr.getAttributeTypeName();
			
			selectionQueryBuild.append(comma);
			if(datatype.equals(Types.OTHER)) {
				// dataType is an object.
				if(geoColumnTypeGeom!=null && geoColumnTypeGeom.equals(AttributeDescriptor.SET_GEOMETRY)) {
					// get geometry dataType information
					ResultSet geomColumnTableInDb=openGeometryColumnTableInDb(schemaName, tableName, attrName, db);
					String geoColumnTypeName=null;
					while(geomColumnTableInDb.next()) {
						geoColumnTypeName=geomColumnTableInDb.getString(AttributeDescriptor.SET_TYPE);
						// srId
						srsCode=geomColumnTableInDb.getInt(AttributeDescriptor.SET_SRID);
					}
					// the object is a geometry.
					if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POINT)) {
						selectionQueryBuild.append(pgConverter.getSelectValueWrapperCoord(attrName));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOINT)) {
						selectionQueryBuild.append(pgConverter.getSelectValueWrapperCoord(attrName));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_LINESTRING)) {
						selectionQueryBuild.append(pgConverter.getSelectValueWrapperPolyline(attrName));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTILINESTRING)) {
						selectionQueryBuild.append(pgConverter.getSelectValueWrapperMultiPolyline(attrName));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POLYGON)) {
						selectionQueryBuild.append(pgConverter.getSelectValueWrapperSurface(attrName));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOLYGON)) {
						selectionQueryBuild.append(pgConverter.getSelectValueWrapperMultiSurface(attrName));
					}
				}else {
					// the object is not part of geometry.
					selectionQueryBuild.append(attrName);
				}
			}else {
				// dataType is type of jdbc.
				selectionQueryBuild.append(attrName);
			}
			comma=",";
		}
		selectionQueryBuild.append(" FROM ");
		if(schemaName!=null) {
			selectionQueryBuild.append(schemaName+".");
		}
		selectionQueryBuild.append(tableName+";");
		// end of selection query, execute created query.
		Statement stmt=db.createStatement();
		ResultSet rs=stmt.executeQuery(selectionQueryBuild.toString());
		return rs;
	}
	
	/** get iomObject with all attributes in appropriate datatype.
	 * @param attribute
	 * @param exportData
	 * @param definedTableName
	 * @param modelName
	 * @param topicName
	 * @return complete iomObject
	 * @throws IoxException
	 * @throws SQLException
	 * @throws ConverterException
	 */
	private IomObject getRecordsAsIomObjects(String definedSchemaName,String definedTableName, String modelName, String topicName, ResultSet pg2IliConvertedTable, Connection db) throws IoxException, SQLException {
		IomObject geoIomObj=null;
		// create iomObject to add attributes or objects in.
		IomObject iomObj=createIomObject(modelName+"."+topicName+"."+definedTableName);
		String geoColumnTypeName=null;
		for(AttributeDescriptor attr:attrs) {
			// data of attribute
			String attrName=attr.getAttributeName();
			String dataTypeName=attr.getAttributeTypeName();
			Integer dataType=attr.getAttributeType();
			// dataTypeName
			String geoColumnTypeGeom=attr.getAttributeTypeName();
			Integer coordDimension=0;
			// attrValue
			Object attrObj=null;
			String attrValue=null;
			if(dataTypeName.equals(AttributeDescriptor.SET_GEOMETRY)) {
				attrObj=pg2IliConvertedTable.getObject("st_asewkb");
			}else {
				attrValue=pg2IliConvertedTable.getString(attrName);
			}
			boolean is3D=false;
			try {
				// get attribute value in appropriate data type.
				if(dataType.equals(Types.OTHER)) {
					if(geoColumnTypeGeom!=null && geoColumnTypeGeom.equals(AttributeDescriptor.SET_GEOMETRY)) {
						// add geometry dataType information to map of PG attribute objects.
						ResultSet geomColumnTableInDb=openGeometryColumnTableInDb(definedSchemaName, definedTableName, attrName, db);
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
							geoIomObj=pgConverter.toIomCoord(attrObj, srsCode.toString(), is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// multipoint
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOINT)) {
							geoIomObj=pgConverter.toIomCoord(attrObj, srsCode.toString(), is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// line
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_LINESTRING)) {
							geoIomObj=pgConverter.toIomPolyline(attrObj, srsCode.toString(), is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// multiline
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTILINESTRING)) {
							geoIomObj=pgConverter.toIomMultiPolyline(attrObj, srsCode.toString(), is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// polygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POLYGON)) {
							geoIomObj=pgConverter.toIomSurface(attrObj, srsCode.toString(), is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// multipolygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOLYGON)) {
							geoIomObj=pgConverter.toIomMultiSurface(attrObj, srsCode.toString(), is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						}
					}else {
						// uuid
						if(dataTypeName.equals(AttributeDescriptor.SET_UUID)) {
							iomObj.setattrvalue(attrName, attrValue);
						// xml	
						}else if(dataTypeName.equals(AttributeDescriptor.SET_XML)) {
							iomObj.setattrvalue(attrName, pgConverter.toIomXml(attrObj));
						}
					}
				}else {
					if(dataType.equals(Types.BOOLEAN) || dataTypeName.equals(AttributeDescriptor.SET_BOOL)) {
						if(attrValue.equals("true")||attrValue.equals("t")||attrValue.equals("y")||attrValue.equals("yes")||attrValue.equals("on")){
							if(dataTypeName.equals("bool")) {
								iomObj.setattrvalue(attrName, "true");
							}else {
								iomObj.setattrvalue(attrName, "1");
							}									
						}else if(attrValue.equals("false")||attrValue.equals("f")||attrValue.equals("n")||attrValue.equals("no")||attrValue.equals("off")){
							if(dataTypeName.equals(AttributeDescriptor.SET_BOOL)) {
								iomObj.setattrvalue(attrName, "false");
							}else {
								iomObj.setattrvalue(attrName, "0");
							}	
						}
					}else if(dataType.equals(Types.BLOB)) {
					    iomObj.setattrvalue(attrName,pgConverter.toIomBlob(attrValue));
					}else if(dataType.equals(Types.BINARY)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.NUMERIC)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.SMALLINT)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.TINYINT)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.INTEGER)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.BIGINT)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.FLOAT)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.DOUBLE)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.LONGNVARCHAR)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.DECIMAL)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.CHAR)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.VARCHAR)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.LONGVARCHAR)) {
						iomObj.setattrvalue(attrName,attrValue);
					}else if(dataType.equals(Types.DATE)) {
						String dateWithT=attrValue.replace(" ", "T");
						iomObj.setattrvalue(attrName,dateWithT);
					}else if(dataType.equals(Types.TIME)) {
						String timeWithT=attrValue.replace(" ", "T");
						iomObj.setattrvalue(attrName,timeWithT);
					}else if(dataType.equals(Types.TIMESTAMP)) {
						String datetimeWithT=attrValue.replace(" ", "T");
						iomObj.setattrvalue(attrName,datetimeWithT);
					}else {
						iomObj.setattrvalue(attrName,attrValue);
					}
				}
			}catch(ConverterException e) {
				// create error message.
				StringBuilder notConvertedAttr= new StringBuilder();
				notConvertedAttr.append("Attribute ");
				notConvertedAttr.append(attrName);
				notConvertedAttr.append(" of type ");
				if(dataType.equals(Types.OTHER)) {
					if(dataTypeName.equals(AttributeDescriptor.SET_GEOMETRY)) {
						notConvertedAttr.append(geoColumnTypeName);
					}else {
						notConvertedAttr.append(dataTypeName);
					}
				}else {
					notConvertedAttr.append(dataTypeName);
				}
				notConvertedAttr.append(" failed by converting.");
				throw new IoxException(notConvertedAttr.toString(),e);
			}
			
		}
		return iomObj;
	}
	
	/** create an iomObject.
	 * @param type
	 * @return iomObject.
	 * @throws IoxException
	 */
	private IomObject createIomObject(String type)throws IoxException{
    	factory=new ch.interlis.iox_j.DefaultIoxFactoryCollection();
    	objectCount+=1;
		String oid="o"+objectCount;
		return factory.createIomObject(type, oid);
	}
}