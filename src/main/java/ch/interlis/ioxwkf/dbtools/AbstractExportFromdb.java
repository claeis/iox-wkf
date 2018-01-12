package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
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

/**<b>AbstractExportFromdb</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * export data (converted from PostGis to Interlis dataTypes) from database and write data with appropriate IoxWriter to file.
 * <p>
 * 
 * <b>Create a new AbstractExportFromdb</b><br>
 * <li>Create an Db2Csv object. Db2Csv extends AbstractExportFromdb class.</li>
 * <li>Create an Db2Shp object. Db2Shp extends AbstractExportFromdb class.</li>
 * <p>
 * 
 * <b>AbstractExport Settings</b><br>
 * <table border="1">
 * <tr>
 *   <th>Setting Name</th>
 *   <th>Description</th>
 *   <th>Example</th>
 * </tr>
 * <tr>
 *   <td>Set File (Mandatory)</td>
 *   <td>The file to write to.<p>
 * 	 	 File will be created in folder of defined path.</td>
 *   <td>File file=new File("C:\file.csv");<br>
 *		 Db2Csv csvExport= new Db2Csv();<br>
 *		 csvExport.exportData(file,"Connection", config);
 *	 </td>
 * </tr>
 * <tr>
 *   <td>Connection (Mandatory)</td>
 *   <td>The Connection has to be valid.</td>
 *   <td>-</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DBSCHEMA</td>
 *   <td>The data base schema.<p>
 * 		Decide between:<br>
 * 		<li>Set dbschema and export data from dbtable inside defined dbschema.</li>
 * 		<li>Set no dbschema and export data from default dbschema.</li></td>
 *   <td>Settings config=new Settings();<br>
 *		 config.setValue(IoxWkfConfig.SETTING_DBSCHEMA,"schemaName");<br>
 *		 Db2Csv csvExport= new Db2Csv();<br>
 *		 csvExport.exportData(new File("file"),"Connection", config);
 *	 </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DBTABLE <b>(Mandatory)</b></td>
 *   <td>The data base table.<br>
 *   	 Import/Export data to this table inside defined or default dbschema.</td>
 *   <td>Settings config=new Settings();<br>
 *		 config.setValue(IoxWkfConfig.SETTING_DBTABLE,"tableName");<br>
 *		 Db2Csv csvExport= new Db2Csv();<br>
 *		 csvExport.exportData(new File("file"),"Connection", config);
 *	 </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_FIRSTLINE</td>
 *   <td> CSV import/export<br>
 *		the first line of CSV file.<p>
 *		decide between:<br>
 *		<li>SETTING_FIRSTLINE_AS_HEADER</li>
 * 		<li>SETTING_FIRSTLINE_AS_VALUE</li></td>
 *   <td>
 *   Settings config=new Settings();<br>
 *	 Db2Csv csvExport= new Db2Csv();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_VALUE);<br>
 *	 csvExport.exportData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DATEFORMAT</td>
 *   <td>The date format.<p>
 * 		 decide between:<br>
 * 		 <li>the default date format pattern: yyyy-MM-dd SETTING_DEFAULTFORMAT_DATE.</li>
 * 		 <li>user defined date format pattern.</li>
 * 		 <p>
 * 		 See: <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a></td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *	 	Db2Csv csvExport= new Db2Csv();<br>
 *	 	config.setValue(IoxWkfConfig.SETTING_DATEFORMAT, IoxWkfConfig.SETTING_DEFAULTFORMAT_DATE);<br>
 *		config.setValue(IoxWkfConfig.SETTING_DATEFORMAT, "yyyy-MM-dd");<br>
 *	 	csvExport.exportData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_TIMEFORMAT</td>
 *   <td>The time format.<p>
 * 		 decide between:<br>
 * 		 <li>the default time format pattern: HH:mm:ss SETTING_DEFAULTFORMAT_TIME.</li>
 * 		 <li>user defined time format pattern.</li>
 *  	 <p>
 * 		 See: <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a></td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *	 	Db2Csv csvExport= new Db2Csv();<br>
 *	 	config.setValue(IoxWkfConfig.SETTING_TIMEFORMAT, IoxWkfConfig.SETTING_DEFAULTFORMAT_TIME);<br>
 *		config.setValue(IoxWkfConfig.SETTING_TIMEFORMAT, "HH:mm:ss");<br>
 *	 	csvExport.exportData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_TIMESTAMPFORMAT</td>
 *   <td>The time stamp format.<p>
 *		 decide between:<br>
 *		 <li>the default time stamp format pattern: yyyy-MM-dd'T'HH:mm:ss.SSS SETTING_DEFAULTFORMAT_TIMESTAMP.</li>
 *		 <li>user defined time stamp format pattern.</li>
 *		 <p>
 *		 See: <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a></td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *	 	Db2Csv csvExport= new Db2Csv();<br>
 *	 	config.setValue(IoxWkfConfig.SETTING_TIMESTAMPFORMAT, IoxWkfConfig.SETTING_DEFAULTFORMAT_TIMESTAMP);<br>
 *		config.setValue(IoxWkfConfig.SETTING_TIMESTAMPFORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSS");<br>
 *	 	csvExport.exportData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 * </table>
 * <p>
 * 
 * <b>AttributeDescriptor possibilities</b><br>
 * {@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}<br>
 * <p>
 * 
 * <b>Setting possibilities</b><br>
 * {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}<br>
 * <p>
 * 
 * <b>Attachement</b><br>
 * <li><a href="https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf">Shapespecification</a></li>
 * <li><a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
 */
public abstract class AbstractExportFromdb {
	private IoxFactoryCollection factory;
	private int objectCount=0;
	private PostgisColumnConverter pgConverter=new PostgisColumnConverter();
	private SimpleDateFormat dateFormat;
	private SimpleDateFormat timeFormat;
	private SimpleDateFormat timeStampFormat;
	
	/** the default model content.
	 */
	private static final String MODELNAME="model";
	
	/** the default topic content.
	 */
	private static final String TOPICNAME="topic";
	/** create a writer in the appropriate format.<br>
	 * <p>
	 * 
	 * Set File (Mandatory)<br>
	 * The file to write to.
	 * <p>
	 * 
	 * Path has to exist.<br>
	 * File file=new File("C:\file.csv");<br>
	 * Db2Csv csvExport= new Csv2db();<br>
	 * csvExport.exportData(file,"Connection", config);
	 * <p>
	 * 
	 * Setting possibilities:<br>
	 * <li>Setting possibilities<br>
	 *	   {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
	 * </li>
	 * 
	 * AttributeDescriptor possibilities<br>
	 * Data base columns of AttributeDescriptor<br>
	 * {@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}
	 * <p>
	 * 
	 * @param file to write to.
	 * @param config to set by user.
	 * @param dbColumns[] 
	 * @return IoxWriter
	 * @throws IoxException
	 */
	protected abstract IoxWriter createWriter(File file, Settings config, AttributeDescriptor dbColumns[]) throws IoxException;
	
	/** exportData.<br>
	 * export from data base table to file.<br>
	 * <p>
	 * 
	 * Set File (Mandatory)<br>
	 * The file to write to.<br>
	 * File has to exist.<br>
	 * <p>
	 * 
	 * File file=new File("C:\file.csv");<br>
	 * Db2Csv csvExport= new Db2Csv();<br>
	 * csvExport.exportData(file,"Connection", config);
	 * <p>
	 * 
	 * Connection (Mandatory)<br>
	 * The Connection has to be valid.
	 * 
	 * Setting possibilities:<br>
	 * <li>Setting possibilities<br>
	 *	   {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
	 * </li>
	 * <p>
	 * 
	 * @param file to write to.
	 * @param db
	 * @param config to set by user.
	 * @throws IoxException
	 */
	public void exportData(File file,Connection db,Settings config) throws IoxException {
		// data base connection has not to be null.
		if(db==null) {
			throw new IoxException("connection==null");
		}else {
			EhiLogger.logState("connection to database: <success>.");
		}
		
		// optional: set the dateFormat.
		String dateFormatPattern=config.getValue(IoxWkfConfig.SETTING_DATEFORMAT);
		if(dateFormatPattern==null) {
			dateFormatPattern=IoxWkfConfig.SETTING_DEFAULTFORMAT_DATE;
		}
		dateFormat = new SimpleDateFormat(dateFormatPattern);
		
		// optional: set the timeFormat.
		String timeFormatPattern=config.getValue(IoxWkfConfig.SETTING_TIMEFORMAT);
		if(timeFormatPattern==null) {
			timeFormatPattern=IoxWkfConfig.SETTING_DEFAULTFORMAT_TIME;
		}
		timeFormat = new SimpleDateFormat(timeFormatPattern);
		
		// optional: set the timeStampFormat.
		String timeStampFormatPattern=config.getValue(IoxWkfConfig.SETTING_TIMESTAMPFORMAT);
		if(timeStampFormatPattern==null) {
			timeStampFormatPattern=IoxWkfConfig.SETTING_DEFAULTFORMAT_TIMESTAMP;
		}
		timeStampFormat = new SimpleDateFormat(timeStampFormatPattern);
		
		// optional: set database schema, if table is not in default schema.
		String definedSchemaName=config.getValue(IoxWkfConfig.SETTING_DBSCHEMA);
		if(definedSchemaName==null) {
			EhiLogger.logState("no db schema name defined, get default schema.");
		}else {
			EhiLogger.logState("db schema name: <"+definedSchemaName+">.");
		}
		
		// mandatory: set database table to insert data into.
		String definedTableName=config.getValue(IoxWkfConfig.SETTING_DBTABLE);
		if(definedTableName==null) {
			throw new IoxException("database table==null.");
		}else {
			EhiLogger.logState("db table name: <"+definedTableName+">.");
		}

		// create selection to get information about attributes of target data base table.
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
		
		IomObject iomObject=null;
		
		// create appropriate IoxWriter.
		IoxWriter writer=createWriter(file, config,attributes.toArray(new AttributeDescriptor[attributes.size()]));

		EhiLogger.logState("start to write records.");
		
		PreparedStatement ps=null;
		ResultSet rs=null;
		try {
			// create selection for appropriate datatypes.
			// geometry datatypes are wrapped from db to ili.
			String selectQuery = getSelectStatement(definedSchemaName, definedTableName, attributes, db);
			ps = db.prepareStatement(selectQuery);
			ps.clearParameters();
			rs = ps.executeQuery();
			while(rs.next()) {
				// convert records to iomObject data types.
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
		writer.write(new EndBasketEvent());
		writer.write(new EndTransferEvent());
		
		// close writer if open.
		if(writer!=null) {
			writer.close();
			writer=null;
		}
		EhiLogger.logState("export: <successful>.");
	}
	
	/** getSelectStatement<br>
	 * create a selection and wrapp all geometries, xml and blob datatypes from db to ili format.<br>
	 * <table border="1">
	 *  <tr>
	 *   <td>SETTING_DBSCHEMA</td>
	 *   <td>The data base schema.<p>
	 * 		Decide between:<br>
	 * 		<li>Set dbschema and export data from dbtable inside defined dbschema.</li>
	 * 		<li>Set no dbschema and export data from default dbschema.</li></td>
	 *   <td>Settings config=new Settings();<br>
	 *		 config.setValue(IoxWkfConfig.SETTING_DBSCHEMA,"schemaName");<br>
	 *		 Db2Csv csvExport= new Db2Csv();<br>
	 *		 csvExport.exportData(new File("file"),"Connection", config);
	 *	 </td>
	 * </tr>
	 * <tr>
	 *   <td>SETTING_DBTABLE <b>(Mandatory)</b></td>
	 *   <td>The data base table.<br>
	 *   	 Import/Export data to this table inside defined or default dbschema.</td>
	 *   <td>Settings config=new Settings();<br>
	 *		 config.setValue(IoxWkfConfig.SETTING_DBTABLE,"tableName");<br>
	 *		 Db2Csv csvExport= new Csv2db();<br>
	 *		 csvExport.exportData(new File("file"),"Connection", config);
	 *	 </td>
	 *  </tr>
	 *  <tr>
	 * 		<td>AttributeDescriptor</td>
	 * 		<td>-</td>
	 * 		<td>{@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}</td>
	 *  </tr>
	 *  <tr>
	 * 		<td>Connection</td>
	 * 		<td>-</td>
	 * 		<td>{@link java.sql.Connection}</td>
	 *  </tr>
	 * </table>
	 * 
	 * @param schemaName
	 * @param tableName
	 * @param attrs
	 * @param db
	 * @return selectionQueryBuild.toString()
	 * @throws SQLException
	 */
	private String getSelectStatement(String schemaName, String tableName, List<AttributeDescriptor> attrs, Connection db) throws SQLException {
		StringBuilder selectionQueryBuild=new StringBuilder();
		String comma="";
		selectionQueryBuild.append("SELECT ");
		// convert each attribute to db valid data type.
		for(AttributeDescriptor attr:attrs) {
			String dbColName="\""+attr.getDbColumnName()+"\"";
			Integer datatype=attr.getDbColumnType();
			String geoColumnTypeGeom=attr.getDbColumnTypeName();
			String geoColumnTypeName=attr.getDbColumnGeomTypeName();
			selectionQueryBuild.append(comma);
			if(attr.isGeometry()) {
				// the object is a geometry.
				if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
					selectionQueryBuild.append(pgConverter.getSelectValueWrapperCoord(dbColName));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOINT)) {
					selectionQueryBuild.append(pgConverter.getSelectValueWrapperMultiCoord(dbColName));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
					selectionQueryBuild.append(pgConverter.getSelectValueWrapperPolyline(dbColName));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTILINESTRING)) {
					selectionQueryBuild.append(pgConverter.getSelectValueWrapperMultiPolyline(dbColName));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
					selectionQueryBuild.append(pgConverter.getSelectValueWrapperSurface(dbColName));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)) {
					selectionQueryBuild.append(pgConverter.getSelectValueWrapperMultiSurface(dbColName));
				}
			}else if(datatype.equals(Types.OTHER)) {
				// the object is not part of geometry.
				selectionQueryBuild.append(dbColName);
			}else {
				selectionQueryBuild.append(dbColName);
			}
			comma=",";
		}
		selectionQueryBuild.append(" FROM ");
		if(schemaName!=null) {
			selectionQueryBuild.append("\""+schemaName+"\""+".");
		}
		selectionQueryBuild.append("\""+tableName+"\""+";");
		return selectionQueryBuild.toString();
	}
	
	/** convertRecordToIomObject<br>
	 * get iomObject with all attributes in appropriate datatype.<br>
	 * <p>
	 * 
	 * <table border="1">
	 *  <tr>
	 *   <td>SETTING_DBSCHEMA</td>
	 *   <td>The data base schema.<p>
	 * 		Decide between:<br>
	 * 		<li>Set dbschema and export data from dbtable inside defined dbschema.</li>
	 * 		<li>Set no dbschema and export data from default dbschema.</li></td>
	 *   <td>Settings config=new Settings();<br>
	 *		 config.setValue(IoxWkfConfig.SETTING_DBSCHEMA,"schemaName");<br>
	 *		 Db2Csv csvExport= new Db2Csv();<br>
	 *		 csvExport.exportData(new File("file"),"Connection", config);
	 *	 </td>
	 *  </tr>
	 *  <tr>
	 *   <td>SETTING_DBTABLE <b>(Mandatory)</b></td>
	 *   <td>The data base table.<br>
	 *   	 Import/Export data to this table inside defined or default dbschema.</td>
	 *   <td>Settings config=new Settings();<br>
	 *		 config.setValue(IoxWkfConfig.SETTING_DBTABLE,"tableName");<br>
	 *		 Db2Csv csvExport= new Db2Csv();<br>
	 *		 csvExport.exportData(new File("file"),"Connection", config);
	 *	 </td>
	 *  </tr>
	 *  <tr>
	 *   <td>ModelName</td>
	 *   <td>The name of the model.</td>
	 *   <td>-</td>
	 *  </tr>
	 *  <tr>
	 *   <td>TopicName</td>
	 *   <td>The name of the topic.</td>
	 *   <td>-</td>
	 *  </tr>
	 * </table>
	 *
	 * AttributeDescriptor possibilities<br>
	 * {@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}<br>
	 * <p>
	 * 
	 * ResultSet:<br>
	 * {@link java.sql.ResultSet}
	 * <p>
	 * 
	 * Connection db:<br>
	 * {@link java.sql.Connection}
	 * <p>
	 * 
	 * @param definedSchemaName
	 * @param definedTableName
	 * @param modelName
	 * @param topicName
	 * @param attrs
	 * @param rs
	 * @param db
	 * @return iomObj
	 * @throws IoxException
	 * @throws SQLException
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
			String iomAttrName=attr.getIomAttributeName();
			String dataTypeName=attr.getDbColumnTypeName();
			Integer dataType=attr.getDbColumnType();
			boolean is3D=false;
			try {
				// get attribute value in appropriate data type.
				if(attr.isGeometry()) {
					geoColumnTypeName=attr.getDbColumnGeomTypeName();
					int coordDimension=attr.getCoordDimension();
					String srsCode=String.valueOf(attr.getSrId());
					if(coordDimension==3) {
						is3D=true;
					}else {
						is3D=false;
					}
					Object objValue=rs.getObject(position);
					if(!rs.wasNull()) {
						// point
						if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
							geoIomObj=pgConverter.toIomCoord(objValue, srsCode, is3D);
							iomObj.addattrobj(iomAttrName, geoIomObj);
						// multipoint
						}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOINT)) {
							geoIomObj=pgConverter.toIomMultiCoord(objValue, srsCode, is3D);
							iomObj.addattrobj(iomAttrName, geoIomObj);
						// line
						}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
							geoIomObj=pgConverter.toIomPolyline(objValue, srsCode, is3D);
							iomObj.addattrobj(iomAttrName, geoIomObj);
						// multiline
						}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTILINESTRING)) {
							geoIomObj=pgConverter.toIomMultiPolyline(objValue, srsCode, is3D);
							iomObj.addattrobj(iomAttrName, geoIomObj);
						// polygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
							geoIomObj=pgConverter.toIomSurface(objValue, srsCode, is3D);
							iomObj.addattrobj(iomAttrName, geoIomObj);
						// multipolygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)) {
							geoIomObj=pgConverter.toIomMultiSurface(objValue, srsCode, is3D);
							iomObj.addattrobj(iomAttrName, geoIomObj);
						}
					}
				}else if(dataType.equals(Types.OTHER)) {
					// uuid in format String
					if(dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_UUID)) {
						String value=rs.getString(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName, value);
						}
					// xml	
					}else if(dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_XML)) {
						Object value=rs.getObject(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName, pgConverter.toIomXml(value));
						}
					}
				}else {
					if(dataType.equals(Types.BOOLEAN) || dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_BOOL)){
						boolean value=rs.getBoolean(position);
						if(!rs.wasNull()) {
							if(value==true) {
								iomObj.setattrvalue(iomAttrName, "true");
							}else if(value==false) {
								iomObj.setattrvalue(iomAttrName, "false");
							}
						}
					}else if(dataType.equals(Types.BIT)) {
						int bytePrecision=attr.getPrecision();
						if(bytePrecision==0 || bytePrecision==1) {
							boolean bit=rs.getBoolean(position);
							if(!rs.wasNull()) {
								if(bit==true) {
									iomObj.setattrvalue(iomAttrName,"true");
								}else if(bit==false) {
									iomObj.setattrvalue(iomAttrName,"false");
								}
							}
						}else if(bytePrecision>=2){
							String bit=rs.getString(position);
							if(!rs.wasNull()) {
								iomObj.setattrvalue(iomAttrName, bit);
							}
						}
					}else if(dataType.equals(Types.BLOB)) {
						Object value=rs.getObject(position);
						if(!rs.wasNull()) {
						    iomObj.setattrvalue(iomAttrName,pgConverter.toIomBlob(value));
						}
					}else if(dataType.equals(Types.BINARY)) {
						byte[] binary2ByteArr=rs.getBytes(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,Arrays.toString(binary2ByteArr));
						}
					}else if(dataType.equals(Types.NUMERIC)) {
						java.math.BigDecimal numeric2BigDec=rs.getBigDecimal(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,numeric2BigDec.toPlainString());
						}
					}else if(dataType.equals(Types.SMALLINT)) {
						Short smallInt2Short=rs.getShort(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,smallInt2Short.toString());
						}
					}else if(dataType.equals(Types.TINYINT)) {
						Byte tinyInt2Byte=rs.getByte(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,tinyInt2Byte.toString());
						}
					}else if(dataType.equals(Types.INTEGER)) {
						int integer2Int=rs.getInt(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,String.valueOf(integer2Int));
						}
					}else if(dataType.equals(Types.BIGINT)) {
						long bigInt2Long=rs.getLong(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,String.valueOf(bigInt2Long));
						}
					}else if(dataType.equals(Types.FLOAT)) {
						double float2Double=rs.getDouble(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,String.valueOf(float2Double));
						}
					}else if(dataType.equals(Types.DOUBLE)) {
						double doubleValue=rs.getDouble(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,String.valueOf(doubleValue));
						}
					}else if(dataType.equals(Types.LONGNVARCHAR)) {
						String longVarChar2Long=rs.getString(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,longVarChar2Long);
						}
					}else if(dataType.equals(Types.DECIMAL)) {
						java.math.BigDecimal decimal2BigDec=rs.getBigDecimal(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,decimal2BigDec.toPlainString());
						}
					}else if(dataType.equals(Types.CHAR)) {
						String character2String=rs.getString(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,character2String);
						}
					}else if(dataType.equals(Types.VARCHAR)) {
						String varchar2String=rs.getString(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,varchar2String);
						}
					}else if(dataType.equals(Types.DATE)) {
						String dateStr=null;
						java.sql.Date date=rs.getDate(position);
						if(!rs.wasNull()) {
							dateStr=dateFormat.format(date);
							iomObj.setattrvalue(iomAttrName,dateStr);
						}
					}else if(dataType.equals(Types.TIME)) {
						String timeStr=null;
						java.sql.Time time=rs.getTime(position);
						if(!rs.wasNull()) {
							timeStr=timeFormat.format(time);
							iomObj.setattrvalue(iomAttrName,timeStr);
						}
					}else if(dataType.equals(Types.TIMESTAMP)) {
						String timestampStr=null;
						java.sql.Timestamp timestamp=rs.getTimestamp(position);
						if(!rs.wasNull()) {
							timestampStr=timeStampFormat.format(timestamp);
							iomObj.setattrvalue(iomAttrName,timestampStr);
						}
					}else {
						String value=rs.getString(position);
						if(!rs.wasNull()) {
							iomObj.setattrvalue(iomAttrName,value);
						}
					}
				}
			}catch(ConverterException e) {
				// create error message.
				StringBuilder notConvertedAttr= new StringBuilder();
				notConvertedAttr.append("Attribute ");
				notConvertedAttr.append(iomAttrName);
				notConvertedAttr.append(" of type ");
				if(dataType.equals(Types.OTHER)) {
					if(dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_GEOMETRY)) {
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
	
	/** createIomObject<br>
	 * create an IomObject.<br>
	 * <p>
	 * 
	 * type:<br>
	 * the type consists of:<br>
	 * <li>modelname.</li>
	 * <li>topicname.</li>
	 * <li>classname</li>
	 * <p>
	 * 
	 * example:<br>
	 * iomObj = createIomObject(modelName+"."+topicName+"."+definedTableName);<br>
	 * <p>
	 *
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