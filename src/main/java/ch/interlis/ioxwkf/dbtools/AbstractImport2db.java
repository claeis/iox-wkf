package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
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

public abstract class AbstractImport2db {
	private PostgisColumnConverter pgConverter=new PostgisColumnConverter();
	
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
	protected void importData(File file,Connection db,Settings config) throws IoxException {
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
		
		/** create list with all attribute descriptors include data
		 */
		List<AttributeDescriptor> attrDescriptors=null;
		if(config.getValue(IoxWkfConfig.SETTING_DBTABLE)!=null){
			attrDescriptors=AttributeDescriptor.getAttributeDescriptors(definedSchemaName, definedTableName, db);
			try {
				AttributeDescriptor.addGeomDataToAttributeDescriptors(definedSchemaName, definedTableName, attrDescriptors, db);
			} catch (SQLException e) {
				throw new IoxException(e);
			}
		}else {
			throw new IoxException("expected tablename");
		}
		/** insert statement to insert data to db.
		 */
		String insertQuery=getInsertStatement(definedSchemaName, definedTableName, attrDescriptors, db);
		PreparedStatement ps=null;
		try {
			ps = db.prepareStatement(insertQuery);
		}catch(Exception e) {
			throw new IoxException(e);
		}
		
		/** read IoxEvents
		 */
		IoxEvent event=reader.read();
		EhiLogger.logState("start import");
		while(event instanceof IoxEvent){
			if(event instanceof ObjectEvent) {
				IomObject iomObj=((ObjectEvent)event).getIomObject();
				int rs;
				try {
					ps.clearParameters();
					/** convert data to import data type.
					 */
					convertObject(attrDescriptors, iomObj, ps, db);
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

	/** convert to valid datatypes.
	 * @param attrDescriptors
	 * @param iomObj
	 * @param ps
	 * @param db
	 * @return preparedstatement of converted objects.
	 * @throws SQLException 
	 * @throws ConverterException 
	 */
	private void convertObject(List<AttributeDescriptor> attrDescriptors, IomObject iomObj, PreparedStatement ps, Connection db) throws SQLException, ConverterException {
		int position=1;
		// add attribute information to attribute descriptors
		for(AttributeDescriptor attribute:attrDescriptors) {
			String dataTypeName=attribute.getAttributeTypeName();
			Integer dataType=attribute.getAttributeType();
			String attrName=attribute.getAttributeName();
			String attrValue=iomObj.getattrvalue(attrName);
			
			if(attrValue!=null || iomObj.getattrobj(attrName,0)!=null){
				if(dataType.equals(Types.OTHER)) {
					if(dataTypeName!=null && dataTypeName.equals(AttributeDescriptor.SET_GEOMETRY)) {
						IomObject attrObjValue=iomObj.getattrobj(attrName,0);
						int srsCode=attribute.getSrId();
						int coordDimension=0;
						boolean is3D=false;
						if(coordDimension==3) {
							is3D=true;
						}else {
							is3D=false;
						}
						// point
						String geoColumnTypeName=attribute.getGeomColumnTypeName();
						if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POINT)) {
							ps.setObject(position, pgConverter.fromIomCoord(attrObjValue, srsCode, is3D));
							position+=1;
							// multipoint
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOINT)) {
							ps.setObject(position, pgConverter.fromIomMultiCoord(attrObjValue, srsCode, is3D));
							position+=1;
							// line
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_LINESTRING)) {
							ps.setObject(position, pgConverter.fromIomPolyline(attrObjValue, srsCode, is3D, 0));
							position+=1;
							// multiline
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTILINESTRING)) {
							ps.setObject(position, pgConverter.fromIomMultiPolyline(attrObjValue, srsCode, is3D, 0));
							position+=1;
							// polygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_POLYGON)) {
							ps.setObject(position, pgConverter.fromIomSurface(attrObjValue, srsCode, false, is3D, 0));
							position+=1;
							// multipolygon
						}else if(geoColumnTypeName.equals(AttributeDescriptor.SET_GEOMETRY_MULTIPOLYGON)) {
							ps.setObject(position, pgConverter.fromIomMultiSurface(attrObjValue, srsCode, false, is3D, 0));
							position+=1;
						}
					}else {
						// uuid
						if(dataTypeName.equals(AttributeDescriptor.SET_UUID)) {
							ps.setObject(position, pgConverter.fromIomUuid(attrValue));
							position+=1;
						// xml	
						}else if(dataTypeName.equals(AttributeDescriptor.SET_XML)) {
							ps.setObject(position, pgConverter.fromIomXml(attrValue));
							position+=1;
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
								position+=1;
							}else {
								ps.setObject(position, "f", Types.BOOLEAN);
								position+=1;
							}
						}else {
							ps.setObject(position, attrValue.charAt(0), Types.BIT);
							position+=1;
						}
					}else if(dataType.equals(Types.BLOB)) {
						Blob b = db.createBlob();
					    InputStream out = b.getBinaryStream(position, Long.valueOf(attrValue));
						ps.setBinaryStream(position, out);
						position+=1;
					}else if(dataType.equals(Types.BINARY)) {
						ps.setByte(position, Byte.valueOf(attrValue));
						position+=1;
					}else if(dataType.equals(Types.NUMERIC)) {
						ps.setObject(position, attrValue, Types.NUMERIC);
						position+=1;
					}else if(dataType.equals(Types.SMALLINT)) {
						ps.setObject(position, attrValue, Types.SMALLINT);
						position+=1;
					}else if(dataType.equals(Types.TINYINT)) {
						ps.setObject(position, attrValue, Types.TINYINT);
						position+=1;
					}else if(dataType.equals(Types.INTEGER)) {
						ps.setObject(position, attrValue, Types.INTEGER);
						position+=1;
					}else if(dataType.equals(Types.BIGINT)) {
						ps.setObject(position, attrValue, Types.BIGINT);
						position+=1;
					}else if(dataType.equals(Types.FLOAT)) {
						ps.setFloat(position, Float.valueOf(attrValue));
						position+=1;
					}else if(dataType.equals(Types.DOUBLE)) {
						ps.setDouble(position, Double.valueOf(attrValue));
						position+=1;
					}else if(dataType.equals(Types.LONGNVARCHAR)) {
						ps.setLong(position, Long.valueOf(attrValue));
						position+=1;
					}else if(dataType.equals(Types.DECIMAL)) {
						Long decLong=Long.valueOf(attrValue);
						ps.setBigDecimal(position, java.math.BigDecimal.valueOf(decLong));
						position+=1;
					}else if(dataType.equals(Types.CHAR)) {
						ps.setObject(position, attrValue, Types.CHAR);
						position+=1;
					}else if(dataType.equals(Types.VARCHAR)) {
						ps.setObject(position, attrValue, Types.VARCHAR);
						position+=1;
					}else if(dataType.equals(Types.LONGVARCHAR)) {
						ps.setObject(position, attrValue, Types.LONGVARCHAR);
						position+=1;
					}else if(dataType.equals(Types.BOOLEAN)) {
						ps.setObject(position, attrValue, Types.BOOLEAN);
						position+=1;
					}else if(dataType.equals(Types.DECIMAL)) {
						Long decLong=Long.valueOf(attrValue);
						ps.setBigDecimal(position, java.math.BigDecimal.valueOf(decLong));
						position+=1;
					}else if(dataType.equals(Types.DATE)) {
						// year format: year-1900, month-1 (0-11)
						String[] date=attrValue.split("T|\\-|\\.|\\,|\\:");
						ps.setDate(position, new Date(Integer.valueOf(date[0])-1900, Integer.valueOf(date[1])-1, Integer.valueOf(date[2])));
						position+=1;
					}else if(dataType.equals(Types.TIME)) {
						String[] time=attrValue.split("T|\\-|\\.|\\,|\\:");
						ps.setTime(position, new Time(Integer.valueOf(time[0]), Integer.valueOf(time[1]), Integer.valueOf(time[2])));
						position+=1;
					}else if(dataType.equals(Types.TIMESTAMP)) {
						Calendar cal=null;
						String[] dateTime=attrValue.split("T|\\-|\\.|\\,|\\:");
						// year format: year-1900, month-1 (0-11)
						ps.setTimestamp(position, new Timestamp(Integer.valueOf(dateTime[0])-1900, Integer.valueOf(dateTime[1])-1, Integer.valueOf(dateTime[2]), Integer.valueOf(dateTime[3]), Integer.valueOf(dateTime[4]), Integer.valueOf(dateTime[5]), Integer.valueOf(dateTime[6])), cal);
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

	/** create insert statement in format of prepared statement
	 * @param schemaName
	 * @param tableName
	 * @param iomObj
	 * @param attrDesc
	 * @param db
	 * @return
	 * @throws IoxException
	 */
	private String getInsertStatement(String schemaName, String tableName, List<AttributeDescriptor> attrDesc, Connection db) throws IoxException {
		StringBuilder queryBuild=new StringBuilder();
		// create insert statement
		queryBuild.append("INSERT INTO ");
		if(schemaName!=null) {
			queryBuild.append(schemaName);
			queryBuild.append(".");
		}
		queryBuild.append(tableName);
		queryBuild.append("(");
		String comma="";
		for(AttributeDescriptor attribute:attrDesc) {
			String attrName=attribute.getAttributeName();
			queryBuild.append(comma);
			comma=", ";
			queryBuild.append(attrName);
		}
		queryBuild.append(")VALUES(");
		comma="";
		for(AttributeDescriptor attribute:attrDesc) {
			queryBuild.append(comma);
			String geoColumnTypeGeom=attribute.getAttributeTypeName();
			if(geoColumnTypeGeom!=null && geoColumnTypeGeom.equals(AttributeDescriptor.SET_GEOMETRY)) {
				int srsCode=attribute.getSrId();
				String geoColumnTypeName=attribute.getGeomColumnTypeName();
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
			comma=", ";
		}
		queryBuild.append(")");
		return queryBuild.toString();
	}	
}