package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.converter.ConverterException;
import ch.ehi.ili2pg.converter.PostgisColumnConverter;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;

public abstract class AbstractExportFromdb {
	protected ArrayList<AttributeDescriptor> attrs=new ArrayList<AttributeDescriptor>();
	private int objectCount=0;
	private IoxFactoryCollection factory;
	private PostgisColumnConverter pgConverter=new PostgisColumnConverter();
	private Integer srsCode=IoxWkfConfig.SETTING_SRSCODE_DEFAULT;
	
	public AbstractExportFromdb(){};
	
	/** export Data from database.
	 * @param file
	 * @param db
	 * @param config
	 * @throws IoxException 
	 * @throws SQLException 
	 */
	public abstract void exportData(File file,Connection db,Settings config) throws SQLException, IoxException;
	
	/** select table in database and get attribute data.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return resultset of db table.
	 * @throws IoxException
	 * @throws SQLException 
	 */
	public ResultSet openTableInDb(String schemaName, String tableName, Connection db) throws IoxException, SQLException {
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
	public ResultSet openPgToIliConvertedTableInDb(String schemaName, String tableName, Connection db) throws IoxException, SQLException {
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
	public IomObject getRecordsAsIomObjects(String definedSchemaName,String definedTableName, String modelName, String topicName, ResultSet pg2IliConvertedTable, Connection db) throws IoxException, SQLException {
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
				// throw exception
				throw new IoxException(notConvertedAttr.toString(),e);
			}
			
		}
		return iomObj;
	}
	
	public IomObject createIomObject(String type)throws IoxException{
    	factory=new ch.interlis.iox_j.DefaultIoxFactoryCollection();
    	objectCount+=1;
		String oid="o"+objectCount;
		return factory.createIomObject(type, oid);
	}
}