package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.converter.ConverterException;
import ch.ehi.ili2pg.converter.PostgisColumnConverter;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;
import ch.interlis.iox.ObjectEvent;
import ch.interlis.iox.StartBasketEvent;
import ch.interlis.iox_j.IoxInvalidDataException;

/** read data from a IoxReader into a database.
 * The IoxReader can be configured by overriding the method {@link #createReader(File, Settings)}.
 * {@link IoxWkfConfig#SETTING_DBSCHEMA}
 * {@link IoxWkfConfig#SETTING_DBTABLE}
 * {@link IoxWkfConfig#SETTING_DATEFORMAT}
 * {@link IoxWkfConfig#SETTING_TIMEFORMAT}
 * {@link IoxWkfConfig#SETTING_TIMESTAMPFORMAT}
 */
public abstract class AbstractImport2db {
	private PostgisColumnConverter pgConverter=new PostgisColumnConverter();
	private SimpleDateFormat dateFormat;
	private SimpleDateFormat timeFormat;
	private SimpleDateFormat timeStampFormat;
	
	/** Create the IoxReader. 
	 * @param file 
	 * @param config
	 * @return IoxReader
	 * @throws IoxException
	 */
	protected abstract IoxReader createReader(File file, Settings config) throws IoxException;
	
	/** importData.<br>
	 * Set File (Mandatory)<br>
	 * The file to read from.<br>
	 * File has to exist and has to be readable.<br>
	 * <p>
	 * 
	 * File file=new File("C:\file.csv");<br>
	 * Csv2db csvImport= new Csv2db();<br>
	 * csvImport.importData(file,"Connection", config);
	 * <p>
	 * 
	 * Connection (Mandatory)<br>
	 * The Connection has to be valid.
	 * 
	 * Setting possibilities:<br>
	 * <li>Setting possibilities<br>
	 *	   {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
	 * </li>
	 * @param file
	 * @param db
	 * @param config
	 * @throws IoxException
	 */
	public void importData(File file,Connection db,Settings config) throws IoxException {
		// validity of connection
		if(db==null) {
			throw new IoxException("connection==null.");
		}else {
			EhiLogger.logState("connection to database: <success>.");
		}
		
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
		
		// create appropriate IoxReader.
		IoxReader reader=createReader(file, config);
		
		// create list with all attribute descriptors including data
		List<AttributeDescriptor> attrDescriptors=null;
		if(config.getValue(IoxWkfConfig.SETTING_DBTABLE)!=null){
			attrDescriptors=AttributeDescriptor.getAttributeDescriptors(definedSchemaName, definedTableName, db);
		}else {
			throw new IoxException("expected tablename");
		}
		// insert statement to insert data to db.
		String insertQuery=getInsertStatement(definedSchemaName, definedTableName, attrDescriptors, db);
		PreparedStatement ps=null;
		try {
			ps = db.prepareStatement(insertQuery);
		}catch(Exception e) {
			throw new IoxException(e);
		}
		
		// read IoxEvents
		IoxEvent event=reader.read();
		EhiLogger.logState("start import");
		while(event instanceof IoxEvent){
			if(event instanceof ObjectEvent) {
				IomObject iomObj=((ObjectEvent)event).getIomObject();
				int rs;
				try {
					ps.clearParameters();
					// convert data to import data type.
					convertObject(attrDescriptors, iomObj, ps, db, config, dateFormatPattern);
					rs = ps.executeUpdate();
				} catch (SQLException e) {
					throw new IoxException(e);
				} catch (ConverterException e) {
					throw new IoxException(e);
				}
				if(rs==0) {
					if(definedSchemaName!=null) {
						throw new IoxException("import of "+iomObj.getobjecttag()+" to "+definedSchemaName+"."+definedTableName+" failed");
					}else {
						throw new IoxException("import of "+iomObj.getobjecttag()+" to "+definedTableName+" failed");
					}
				}
			}else if(event instanceof StartBasketEvent) {
				ArrayList<String> missingAttributes=new ArrayList<String>();
				assignIomAttr2DbColumn(reader,attrDescriptors,missingAttributes);
			}
			event=reader.read();
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

	/** assigns the attributes (from the reader) to a db column.
	 * Set the attribute name as read by the reader in the column descriptors of the target table of the import.
	 * @param reader 
	 * @param dbColumns 
	 * @param missingDbColumns attributes from the reader, that are not assigned to a column in the database target table
	 */
	protected abstract void assignIomAttr2DbColumn(IoxReader reader, List<AttributeDescriptor> dbColumns,List<String> missingDbColumns);
	
	/** convertObject<br>
	 * convert attributes of IomObject to PostGis dataTypes.<br>
	 * <p>
	 * 
	 * AttributeDescriptor possibilities<br>
	 * {@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}<br>
	 * <p>
	 * 
	 * IomObject:<br>
	 * {@link ch.interlis.iom.IomObject}
	 * <p>
	 * 
	 * Prepared Statement:<br>
	 * {@link java.sql.PreparedStatement}
	 * <p>
	 * 
	 * Connection:<br>
	 * {@link java.sql.Connection}
	 * <p>
	 * 
	 * Settings:<br>
	 * {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
	 * <p>
	 * 
	 * Defined Format:<br>
	 * <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">date format</a>
	 * <p>
	 * 
	 * @param attrDescriptors
	 * @param iomObj
	 * @param ps
	 * @param db
	 * @param config
	 * @param definedFormat
	 * @throws SQLException
	 * @throws ConverterException
	 * @throws IoxException
	 */
	private void convertObject(List<AttributeDescriptor> attrDescriptors, IomObject iomObj, PreparedStatement ps, Connection db, Settings config, String definedFormat) throws SQLException, ConverterException, IoxException {
		int position=1;
		// add attribute information to attribute descriptors
		for(AttributeDescriptor attribute:attrDescriptors) {
			String dataTypeName=attribute.getDbColumnTypeName();
			Integer dataType=attribute.getDbColumnType();
			String iomAttrName=attribute.getIomAttributeName();
			String attrValue=iomObj.getattrvalue(iomAttrName);
			
			if(attrValue!=null || iomObj.getattrobj(iomAttrName,0)!=null){
				if(attribute.isGeometry()) {
					IomObject attrObjValue=iomObj.getattrobj(iomAttrName,0);
					int srsCode=attribute.getSrId();
					int coordDimension=0;
					boolean is3D=false;
					if(coordDimension==3) {
						is3D=true;
					}else {
						is3D=false;
					}
					// point
					String geoColumnTypeName=attribute.getDbColumnGeomTypeName();
					if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
						ps.setObject(position, pgConverter.fromIomCoord(attrObjValue, srsCode, is3D));
						position+=1;
						// multipoint
					}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOINT)) {
						ps.setObject(position, pgConverter.fromIomMultiCoord(attrObjValue, srsCode, is3D));
						position+=1;
						// line
					}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
						ps.setObject(position, pgConverter.fromIomPolyline(attrObjValue, srsCode, is3D, 0));
						position+=1;
						// multiline
					}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTILINESTRING)) {
						ps.setObject(position, pgConverter.fromIomMultiPolyline(attrObjValue, srsCode, is3D, 0));
						position+=1;
						// polygon
					}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
						ps.setObject(position, pgConverter.fromIomSurface(attrObjValue, srsCode, false, is3D, 0));
						position+=1;
						// multipolygon
					}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)) {
						ps.setObject(position, pgConverter.fromIomMultiSurface(attrObjValue, srsCode, false, is3D, 0));
						position+=1;
					}
				}else if(dataType.equals(Types.OTHER)) {
					// uuid
					if(dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_UUID)) {
						ps.setObject(position, pgConverter.fromIomUuid(attrValue));
						position+=1;
					// xml	
					}else if(dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_XML)) {
						ps.setObject(position, pgConverter.fromIomXml(attrValue));
						position+=1;
					}
				}else {
					if(dataType.equals(Types.BIT)) {
						if(dataTypeName.equals("bool")) {
							ps.setBoolean(position, parseBoolean(attrValue));
							position+=1;
						}else {
							// ps.setObject(position, attrValue.charAt(0), Types.BIT);
							ps.setBoolean(position, parseBoolean(attrValue));
							position+=1;
						}
					}else if(dataType.equals(Types.BLOB)) {
						throw new java.lang.UnsupportedOperationException();
					}else if(dataType.equals(Types.BINARY)) {
						throw new java.lang.UnsupportedOperationException();
					}else if(dataType.equals(Types.NUMERIC)) {
						try {
							ps.setBigDecimal(position, new BigDecimal(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.SMALLINT)) {
						try {
							ps.setShort(position, Short.parseShort(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.TINYINT)) {
						try {
							ps.setByte(position, Byte.parseByte(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.INTEGER)) {
						try {
							ps.setInt(position, Integer.parseInt(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.BIGINT)) {
						try {
							ps.setLong(position, Long.parseLong(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.FLOAT)) {
						try {
							ps.setFloat(position, Float.parseFloat(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.DOUBLE)) {
						try {
							ps.setDouble(position, Double.parseDouble(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.LONGNVARCHAR)) {
						ps.setString(position, attrValue);
						position+=1;
					}else if(dataType.equals(Types.DECIMAL)) {
						try {
							ps.setBigDecimal(position, new BigDecimal(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.CHAR)) {
						ps.setString(position, attrValue.substring(0,1));
						position+=1;
					}else if(dataType.equals(Types.VARCHAR)) {
						ps.setString(position, attrValue);
						position+=1;
					}else if(dataType.equals(Types.LONGVARCHAR)) {
						ps.setString(position, attrValue);
						position+=1;
					}else if(dataType.equals(Types.BOOLEAN)) {
						ps.setBoolean(position, parseBoolean(attrValue));
						position+=1;
					}else if(dataType.equals(Types.DECIMAL)) {
						try {
							ps.setBigDecimal(position, new java.math.BigDecimal(attrValue));
						} catch (NumberFormatException e) {
							throw new IoxInvalidDataException(e);
						}
						position+=1;
					}else if(dataType.equals(Types.DATE)) {
						java.sql.Date sqlDate=null;
						try {
							// match attrValue to format.
							java.util.Date utilDate=dateFormat.parse(attrValue);
							sqlDate = new java.sql.Date(utilDate.getTime());
						} catch (ParseException e) {
							// attrValue not match format.
							throw new IoxException(attrValue+" does not match format: "+dateFormat.toPattern()+".");
						}
						ps.setDate(position, sqlDate);
						position+=1;
					}else if(dataType.equals(Types.TIME)) {
						java.sql.Time sqlTime=null;
						try {
							// match attrValue to format.
							java.util.Date utilDate=timeFormat.parse(attrValue);
							sqlTime = new java.sql.Time(utilDate.getTime());
						} catch (ParseException e) {
							// attrValue not match format.
							throw new IoxException(attrValue+" does not match format: "+timeFormat.toPattern()+".");
						}
						ps.setTime(position, sqlTime);
						position+=1;
					}else if(dataType.equals(Types.TIMESTAMP)) {
						Timestamp sqlTimeStamp=null;
						try {
							// match attrValue to format.
							java.util.Date utilDate=timeStampFormat.parse(attrValue);
							sqlTimeStamp = new Timestamp(utilDate.getTime());
						}catch (ParseException e) {
							// attrValue not match format.
							throw new IoxException(attrValue+" does not match format: "+timeStampFormat.toPattern()+".");
						}
						ps.setTimestamp(position, sqlTimeStamp);
						position+=1;
					}else {
						ps.setObject(position, attrValue, dataType);
						position+=1;
					}
				}
			}else {
				ps.setNull(position, dataType);
				position+=1;
			}
		}
	}

	private boolean parseBoolean(String attrValue) {
		if(attrValue.equalsIgnoreCase("t")
				|| attrValue.equalsIgnoreCase("true")
				|| attrValue.equalsIgnoreCase("y")
				|| attrValue.equalsIgnoreCase("yes")
				|| attrValue.equalsIgnoreCase("on")
				|| attrValue.equals("1")) {
			return true;
		}
		return false;
	}

	/** getInsertStatement<br>
	 * create and return insert statement.<br>
	 * <table border="1">
	 *  <tr>
	 *   <td>SETTING_DBSCHEMA</td>
	 *   <td>The data base schema.<p>
	 * 		Decide between:<br>
	 * 		<li>Set dbschema and import data into dbtable inside defined dbschema.</li>
	 * 		<li>Set no dbschema and import data into default dbschema.</li></td>
	 *   <td>Settings config=new Settings();<br>
	 *		 config.setValue(IoxWkfConfig.SETTING_DBSCHEMA,"schemaName");<br>
	 *		 Csv2db csvImport= new Csv2db();<br>
	 *		 csvImport.importData(new File("file"),"Connection", config);
	 *	 </td>
	 * </tr>
	 * <tr>
	 *   <td>SETTING_DBTABLE <b>(Mandatory)</b></td>
	 *   <td>The data base table.<br>
	 *   	 Import/Export data to this table inside defined or default dbschema.</td>
	 *   <td>Settings config=new Settings();<br>
	 *		 config.setValue(IoxWkfConfig.SETTING_DBTABLE,"tableName");<br>
	 *		 Csv2db csvImport= new Csv2db();<br>
	 *		 csvImport.importData(new File("file"),"Connection", config);
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
	 * @param attrDesc
	 * @param db
	 * @return queryBuild.toString()
	 * @throws IoxException
	 */
	private String getInsertStatement(String schemaName, String tableName, List<AttributeDescriptor> attrDesc, Connection db) throws IoxException {
		StringBuilder queryBuild=new StringBuilder();
		// create insert statement
		queryBuild.append("INSERT INTO ");
		if(schemaName!=null) {
			queryBuild.append("\"");
			queryBuild.append(schemaName);
			queryBuild.append("\"");
			queryBuild.append(".");
		}
		queryBuild.append("\"");
		queryBuild.append(tableName);
		queryBuild.append("\"");
		queryBuild.append("(");
		String comma="";
		for(AttributeDescriptor attribute:attrDesc) {
			String attrName=attribute.getDbColumnName();
			queryBuild.append(comma);
			comma=", ";
			queryBuild.append("\"");
			queryBuild.append(attrName);
			queryBuild.append("\"");
		}
		queryBuild.append(")VALUES(");
		comma="";
		for(AttributeDescriptor attribute:attrDesc) {
			queryBuild.append(comma);
			String geoColumnTypeGeom=attribute.getDbColumnTypeName();
			if(attribute.isGeometry()) {
				int srsCode=attribute.getSrId();
				String geoColumnTypeName=attribute.getDbColumnGeomTypeName();
				if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
					queryBuild.append(pgConverter.getInsertValueWrapperCoord("?", srsCode));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOINT)) {
					queryBuild.append(pgConverter.getInsertValueWrapperMultiCoord("?", srsCode));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
					queryBuild.append(pgConverter.getInsertValueWrapperPolyline("?", srsCode));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTILINESTRING)) {
					queryBuild.append(pgConverter.getInsertValueWrapperMultiPolyline("?", srsCode));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
					queryBuild.append(pgConverter.getInsertValueWrapperSurface("?", srsCode));
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)) {
					queryBuild.append(pgConverter.getInsertValueWrapperMultiSurface("?", srsCode));
				}
			}else {
				queryBuild.append("?");
			}
			comma=", ";
		}
		queryBuild.append(")");
		return queryBuild.toString();
	}	
}