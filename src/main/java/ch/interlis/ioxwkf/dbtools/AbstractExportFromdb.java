package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
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
	private IoxFactoryCollection factory;
	private int objectCount=0;
	private PostgisColumnConverter pgConverter=new PostgisColumnConverter();
	
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
	protected void exportData(File file,Connection db,Settings config) throws IoxException {
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
		List<AttributeDescriptor> attributes=null;
		try {
			attributes=AttributeDescriptor.getAttributeDescriptors(definedSchemaName, definedTableName, db);
			AttributeDescriptor.addGeomDataToAttributeDescriptors(definedSchemaName, definedTableName, attributes, db);
		}catch(Exception e) {
			if(definedSchemaName!=null) {
				throw new IoxException("db table <"+definedTableName+"> inside db schema <"+definedSchemaName+">: not found.",e);
			}else{
				throw new IoxException("db table "+definedTableName+" inside default db schema: not found.",e);
			}
		}
		
		/** The final IomObjects will be send in ioxObjectEvents and written as individual records to the given file.
		 */
		EhiLogger.logState("start transfer to file.");
		EhiLogger.logState("start to write records.");
		
		IomObject iomObject=null;
		
		/** create appropriate IoxWriter.
		 */
		IoxWriter writer=createWriter(file, config);
		
		PreparedStatement ps=null;
		ResultSet rs=null;
		try {
			/** create selection for appropriate datatypes.
			 *  geometry datatypes are wrapped from db to ili.
			 */
			String selectQuery = getSelectStatement(definedSchemaName, definedTableName, attributes, db);
			ps = db.prepareStatement(selectQuery);
			ps.clearParameters();
			rs = ps.executeQuery();
			while(rs.next()) {
				/** convert records to iomObject data types.
				 */
				iomObject=convertRecordToIomObject(definedSchemaName,definedTableName, MODELNAME, TOPICNAME, attributes, rs, db);
				try {
					writer.write(new ch.interlis.iox_j.ObjectEvent(iomObject));
				}catch(IoxException e) {
					throw new IoxException("export of: <"+iomObject.getobjecttag()+"> to file: <"+file.getAbsolutePath()+"> failed.",e);
				}
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}finally{
			if(rs!=null) {
				try {
					rs.close();
				} catch (SQLException e) {
					throw new IoxException(e);
				}
				rs=null;
			}
			if(ps!=null) {
				try {
					ps.close();
				} catch (SQLException e) {
					throw new IoxException(e);
				}
				ps=null;
			}
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
	
	/** create a selection and wrapp all geometries, xml and blob datatypes from db to ili format.
	 * @param schemaName
	 * @param tableName
	 * @param attrsPool
	 * @param db
	 * @return
	 * @throws IoxException
	 * @throws SQLException
	 * @throws ConverterException
	 */
	private String getSelectStatement(String schemaName, String tableName, List<AttributeDescriptor> attrs, Connection db) throws SQLException {
		StringBuilder selectionQueryBuild=new StringBuilder();
		String comma="";
		selectionQueryBuild.append("SELECT ");
		/** convert each attribute to db valid data type.
		 */
		for(AttributeDescriptor attr:attrs) {
			String attrName=attr.getAttributeName();
			Integer datatype=attr.getAttributeType();
			String geoColumnTypeGeom=attr.getAttributeTypeName();
			String geoColumnTypeName=attr.getGeomColumnTypeName();
			selectionQueryBuild.append(comma);
			if(datatype.equals(Types.OTHER)) {
				// dataType is an object.
				if(geoColumnTypeGeom!=null && geoColumnTypeGeom.equals(AttributeDescriptor.SET_GEOMETRY)) {
					// the object is a geometry.
					if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POINT)) {
						selectionQueryBuild.append(pgConverter.getSelectValueWrapperCoord(attrName));
					}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOINT)) {
						selectionQueryBuild.append(pgConverter.getSelectValueWrapperMultiCoord(attrName));
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
				selectionQueryBuild.append(attrName);
			}
			comma=",";
		}
		selectionQueryBuild.append(" FROM ");
		if(schemaName!=null) {
			selectionQueryBuild.append(schemaName+".");
		}
		selectionQueryBuild.append(tableName+";");
		return selectionQueryBuild.toString();
	}
	
	/** get iomObject with all attributes in appropriate datatype.
	 * @param attribute
	 * @param exportData
	 * @param definedTableName
	 * @param modelName
	 * @param topicName
	 * @return complete iomObject
	 * @throws SQLException
	 * @throws IoxException 
	 * @throws ConverterException
	 */
	private IomObject convertRecordToIomObject(String definedSchemaName,String definedTableName, String modelName, String topicName, List<AttributeDescriptor> attrs, ResultSet rs, Connection db) throws IoxException, SQLException {
		IomObject geoIomObj=null;
		// create iomObject to put attributes or objects inside.
		IomObject iomObj;
		try {
			iomObj = createIomObject(modelName+"."+topicName+"."+definedTableName);
		} catch (IoxException e1) {
			throw new IoxException(e1);
		}
		String geoColumnTypeName=null;
		int position=0;
		for(AttributeDescriptor attr:attrs) {
			position+=1;
			String attrName=attr.getAttributeName();
			String dataTypeName=attr.getAttributeTypeName();
			Integer dataType=attr.getAttributeType();
			String attrValue;
			try {
				attrValue = rs.getString(position);
			} catch (SQLException e1) {
				throw new IoxException(e1);
			}
			boolean is3D=false;
			try {
				/** get attribute value in appropriate data type.
				 */
				if(dataType.equals(Types.OTHER)) {
					if(dataTypeName!=null && dataTypeName.equals(AttributeDescriptor.SET_GEOMETRY)) {
						geoColumnTypeName=attr.getGeomColumnTypeName();
						int coordDimension=attr.getCoordDimension();
						String srsCode=String.valueOf(attr.getSrId());
						if(coordDimension==3) {
							is3D=true;
						}else {
							is3D=false;
						}
						// point
						if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POINT)) {
							geoIomObj=pgConverter.toIomCoord(rs.getObject(2), srsCode, is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// multipoint
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOINT)) {
							geoIomObj=pgConverter.toIomMultiCoord(rs.getObject(2), srsCode, is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// line
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_LINESTRING)) {
							geoIomObj=pgConverter.toIomPolyline(rs.getObject(2), srsCode, is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// multiline
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTILINESTRING)) {
							geoIomObj=pgConverter.toIomMultiPolyline(rs.getObject(2), srsCode, is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// polygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POLYGON)) {
							geoIomObj=pgConverter.toIomSurface(rs.getObject(2), srsCode, is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						// multipolygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOLYGON)) {
							geoIomObj=pgConverter.toIomMultiSurface(rs.getObject(2), srsCode, is3D);
							iomObj.addattrobj(attrName, geoIomObj);
						}
					}else {
						// uuid
						if(dataTypeName.equals(AttributeDescriptor.SET_UUID)) {
							iomObj.setattrvalue(attrName, attrValue);
						// xml	
						}else if(dataTypeName.equals(AttributeDescriptor.SET_XML)) {
							iomObj.setattrvalue(attrName, pgConverter.toIomXml(rs.getObject(2)));
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
					}else if(dataType.equals(Types.BIT)) {
						iomObj.setattrvalue(attrName,attrValue);
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
			}catch(SQLException e2) {
				throw new SQLException(e2);
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