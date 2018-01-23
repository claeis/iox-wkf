package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;

/** export all DB-Tables, inside defined DB-Schema, as INTERLIS 2.3 classes.
 */
public class Db2Ili{
	// ili file
	private static final String ILIFILE_VERSION="2.3";
	private static final String ILIFILE_MAILTO=System.getProperties().getProperty("user.name")+"@localhost.ch";
	public static final String TOPICNAME="topic1";
    private static final char NEWLINE='\n';
	private static final String DEFINEDOVERLAB="0.01";
	private static final String INDENT="  ";
	
	// coordnames of coorddefinitions
	public static final String LCOORD2056="lcoord2056";
	public static final String HCOORD2056="hcoord2056";
	public static final String LCOORD21781="lcoord21781";
	public static final String HCOORD21781="hcoord21781";
	
	/** export tablestructure to ili-model.
	 * {@link IoxWkfConfig#SETTING_DBSCHEMA} DB-Schema includes all tables to export.
	 * @throws IOException 
	 */
	public void exportData(File ilifile,Connection db,Settings config) throws IoxException, SQLException {
		// mandatory: file to write to has not to be null.
		if(ilifile!=null) {
			EhiLogger.logState("file to write to: <"+ilifile.getName()+">");
		}else {
			throw new IoxException("file==null");
		}
		
		// mandatory: data base connection has not to be null.
		if(db!=null) {
			EhiLogger.logState("connection to database: <success>.");
		}else {
			throw new IoxException("connection==null");
		}
		
		// mandatory: set the DB-Schema from which all database tables are exported.
		String dbSchemaName=config.getValue(IoxWkfConfig.SETTING_DBSCHEMA);
		if(dbSchemaName!=null) {
			EhiLogger.logState("db schema name: <"+dbSchemaName+">.");
		}else {
			throw new IoxException("db schema name==null");
		}
		
		// get all DB-Table names inside target DB-Schema.
		List<String> dbTableNames=null;
		try {
			dbTableNames=getTables(db, dbSchemaName);
		} catch (IoxException e) {
			throw new IoxException(e);
		}
		
		// create new FileWriter.
		FileWriter writer=null;
		try {
			writer=new FileWriter(ilifile);
		} catch (IOException e) {
			throw new IoxException(e);
		}
		
		try {
			writer.write("INTERLIS "+ILIFILE_VERSION+";");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			// model
			writer.write("MODEL "+dbSchemaName+" AT \"mailto:"+ILIFILE_MAILTO+"\" VERSION \""+getCurrentDate()+"\" =");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			// topic
			writer.write(INDENT);
			writer.write("TOPIC "+TOPICNAME+" =");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			// domain
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("DOMAIN");
			writer.write(NEWLINE);
			
		} catch (IOException e) {
			throw new IoxException(e);
		}
		
		try {
			for(String dbTableName : dbTableNames) {
				List<AttributeDescriptor> attrDesc=AttributeDescriptor.getAttributeDescriptors(dbSchemaName, dbTableName, db);
				if(attrDesc!=null) {
					writeCoordDefinition(attrDesc.toArray(new AttributeDescriptor[attrDesc.size()]), writer);
					writeClass(writer, dbTableName, attrDesc.toArray(new AttributeDescriptor[attrDesc.size()]));
				}else {
					throw new IoxException("no attributes found.");
				}
			}
			writer.write(NEWLINE);
			// end Topic.
			writer.write(INDENT);
			writer.write("END ");
			writer.write(TOPICNAME);
			writer.write(";");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			// end Model.
			writer.write("END ");
			writer.write(dbSchemaName);
			writer.write(".");
			
			close(writer);
			
		} catch (IOException e) {
			throw new IoxException(e);
		}
	}

	private String getCurrentDate() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		return dateFormat.format(date);
	}

	/** return all tables from defined schema.
	 * @param db DB-Connection
	 * @param schema which contains all tables.
	 * @return list of all tablenames.
	 * @throws IoxException 
	 */
	private List<String> getTables(Connection db, String schema) throws IoxException {
		PreparedStatement ps=null;
		DatabaseMetaData md;
		try {
			md = db.getMetaData();
		} catch (SQLException e2) {
			throw new IoxException(e2);
		}
		
		ResultSet rs;
		try {
			// get all tables of defined schema.
			rs = md.getTables(null, schema, "%", null);
		} catch (SQLException e1) {
			throw new IoxException(e1);
		}
		try {
			List<String> dbTableNames=new ArrayList<String>();
			while (rs.next()) {
				dbTableNames.add(rs.getString(3));
			}
			if(dbTableNames.size()>0) {
				return dbTableNames;
			}else {
				throw new IoxException("no table found in schema");
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
	}
	
	/** write out a DB-Table to an ili-Classname.
	 * 
	 * @param writer ili-file
	 * @param tablename tablename/classname
	 * @param attributes of table
	 * @throws IoxException
	 */
	private void writeClass(Writer writer,String tablename,AttributeDescriptor attributes[]) throws IoxException{
		try {
			// create Class.
			writer.write(NEWLINE);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("CLASS ");
			writer.write(tablename);
			writer.write(" =");
			writer.write(NEWLINE);
			for(AttributeDescriptor attribute:attributes) {
				try {
					writer.write(INDENT);
					writer.write(INDENT);
					writer.write(INDENT);
					writeAttribute(writer, attribute);
				} catch (IoxException e) {
					throw new IoxException(e);
				}
			}
			// end Class.
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("END ");
			writer.write(tablename);
			writer.write(";");
			writer.write(NEWLINE);
		} catch (IOException e) {
			throw new IoxException(e);
		}
	}
	
	/** write a DB-Column as an ili-type.
	 * 
	 * @param writer ili-file
	 * @param attribute write in Interlis-Syntax.
	 * @throws IoxException
	 */
	private void writeAttribute(Writer writer,AttributeDescriptor attribute) throws IoxException{
		Boolean isMandatory=false;
		try {
			String attrName=attribute.getIomAttributeName();
			if(attrName!=null) {
				// attrname
				writer.write(attrName);
			}else {
				throw new IoxException("no IomAttributeName defined.");
			}
			// ili type
			writer.write(" : ");
			// mandatory
			isMandatory=attribute.isMandatory();
			if(isMandatory!=null && isMandatory) {
				writer.write("MANDATORY ");
			}
			String iliType = getIliTypeDefinition(attribute);
			if(iliType==null) {
				throw new IoxException("type not found.");
			}
			writer.write(iliType);
			writer.write(";");
			writer.write(NEWLINE);
		} catch (IOException e) {
			throw new IoxException(e);
		}
	}
	
	/** return the ili type.
	 * @param attribute get the type of attribute
	 * @return attribute type definition.
	 * @throws IoxException
	 */
	private String getIliTypeDefinition(AttributeDescriptor attribute) throws IoxException {
		StringBuilder resultType=new StringBuilder();
		String dataTypeName=attribute.getDbColumnTypeName();
		Integer dataType=attribute.getDbColumnType();
		// geometry
		if(attribute.isGeometry()) {
			Integer coordDimension=attribute.getCoordDimension();
			Integer epsg=attribute.getSrId();
			String geoColumnTypeName=attribute.getDbColumnGeomTypeName();
			if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
				resultType.append(getCoordDefinition(coordDimension, epsg));
			}else {
				if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
					resultType.append("POLYLINE WITH (STRAIGHTS) VERTEX ");
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
					resultType.append("SURFACE WITH (STRAIGHTS) VERTEX ");
				// curve/circular
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_COMPOUNDCURVE)) {
					resultType.append("POLYLINE WITH (STRAIGHTS,ARCS) VERTEX ");
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_CURVEPOLYGON)) {
					resultType.append("SURFACE WITH (STRAIGHTS,ARCS) VERTEX ");
				}
				String coordDefinition=getCoordDefinition(coordDimension, epsg);
				if(coordDefinition!=null) {
					resultType.append(coordDefinition);
				}
				String addOverlab=getOverlapDefinition();
				if(addOverlab!=null) {
					resultType.append(addOverlab);
				}
			}
		}else if(dataType.equals(Types.OTHER)) {
			if(dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_UUID)) {
				resultType.append("INTERLIS.UUIDOID");
			}
		}else {
			Integer precision=attribute.getPrecision();
			if(dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_XML)) {
				resultType.append("BLACKBOX XML");
			}else if(dataType.equals(Types.BOOLEAN) || dataTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_BOOL)){
				resultType.append("BOOLEAN");
			}else if(dataType.equals(Types.BIT)) {
				int bytePrecision=attribute.getPrecision();
				if(bytePrecision==0 || bytePrecision==1) {
					resultType.append("BOOLEAN");
				}else if(bytePrecision>=2){
					resultType.append("BLACKBOX BINARY");
				}
			}else if(dataType.equals(Types.BLOB)) {
				resultType.append("TEXT");
			}else if(dataType.equals(Types.BINARY)) {
				resultType.append("BLACKBOX BINARY");
			}else if(dataType.equals(Types.NUMERIC)) {
				resultType.append("-"+precision+" .. "+precision);
			}else if(dataType.equals(Types.SMALLINT)) {
				resultType.append("-32768 .. 32767");
			}else if(dataType.equals(Types.INTEGER)) {
				resultType.append("-2147483648 .. 2147483647");
			}else if(dataType.equals(Types.BIGINT)) {
				resultType.append("-9223372036854775808 .. 9223372036854775807");
			}else if(dataType.equals(Types.FLOAT) || dataTypeName.equals("float4")) {
				resultType.append("-"+String.format("%.6f",Float.MAX_VALUE)+" .. "+String.format("%.6f",Float.MAX_VALUE));
			}else if(dataType.equals(Types.DOUBLE) || dataTypeName.equals("float8")) {
				resultType.append("-"+String.format("%.15f",Double.MAX_VALUE)+" .. "+String.format("%.15f",Double.MAX_VALUE));
			}else if(dataType.equals(Types.REAL)) {
				resultType.append("-"+String.format("%.6f",Double.MAX_VALUE)+" .. "+String.format("%.6f",Double.MAX_VALUE));
			}else if(dataType.equals(Types.LONGVARCHAR)) {
				resultType.append("TEXT*"+precision);
			}else if(dataType.equals(Types.DECIMAL)) {
				resultType.append("-"+precision+" .. "+precision);
			}else if(dataType.equals(Types.CHAR)) {
				resultType.append("TEXT*"+precision);
			}else if(dataType.equals(Types.VARCHAR)) {
				resultType.append("TEXT*"+precision);
			}else if(dataType.equals(Types.DATE)) {
				resultType.append("INTERLIS.XMLDate");
			}else if(dataType.equals(Types.TIME)) {
				resultType.append("INTERLIS.XMLTime");
			}else if(dataType.equals(Types.TIME_WITH_TIMEZONE)) {
				resultType.append("INTERLIS.XMLTime");
			}else if(dataType.equals(Types.TIMESTAMP)) {
				resultType.append("INTERLIS.XMLDateTime");
			}else if(dataType.equals(Types.TIMESTAMP_WITH_TIMEZONE)) {
				resultType.append("INTERLIS.XMLDateTime");
			}else {
				resultType.append("TEXT");
			}
		}
		return resultType.toString();
	}
	
	private void writeCoordDefinition(AttributeDescriptor[] attributes, FileWriter writer) throws IOException {
		List<String> coordDimList=new ArrayList<String>();
		for(AttributeDescriptor attribute:attributes) {
			if(attribute.isGeometry()) {
				Integer epsg=attribute.getSrId();
				Integer coordDimension=attribute.getCoordDimension();
				if(epsg==null) {
					continue;
				}else {
					// epsg: CHLV95
					if(epsg==2056) {
						if(coordDimension==2) {
							String lcoord="lcoord2056 = COORD 2460000.000 .. 2870000.000 [INTERLIS.m],\r\n" + 
									"	            1045000.000 .. 1310000.000 [INTERLIS.m],\r\n" + 
									"	            ROTATION 2 -> 1;";
							if(!coordDimList.contains(lcoord)) {
								coordDimList.add(lcoord);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(lcoord);
							}
						}else if(coordDimension==3) {
							String hcoord="hcoord2056 = COORD 2460000.000 .. 2870000.000 [INTERLIS.m],\r\n" + 
									"	            1045000.000 .. 1310000.000 [INTERLIS.m],\r\n" + 
									"	            -200.000 .. 5000.000 [INTERLIS.m],\r\n" + 
									"	            ROTATION 2 -> 1;";
							if(!coordDimList.contains(hcoord)) {
								coordDimList.add(hcoord);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(hcoord);
							}
						}
					// epsg: CHLV03
					}else if(epsg==21781) {
						if(coordDimension==2) {
							String lcoord="lcoord21781 = COORD 460000.000 .. 870000.000 [INTERLIS.m],\r\n" + 
										"	        45000.000 .. 310000.000 [INTERLIS.m],\r\n" + 
										"	        ROTATION 2 -> 1;";
							if(!coordDimList.contains(lcoord)) {
								coordDimList.add(lcoord);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(lcoord);
							}
						}else if(coordDimension==3) {
							String hcoord="hcoord21781 = COORD 460000.000 .. 870000.000 [INTERLIS.m],\r\n" + 
									"	            45000.000 .. 310000.000 [INTERLIS.m],\r\n" + 
									"	            -200.000 .. 5000.000 [INTERLIS.m],\r\n" + 
									"	            ROTATION 2 -> 1;";
							if(!coordDimList.contains(hcoord)) {
								coordDimList.add(hcoord);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(hcoord);
							}
						}
					// epsg: other or null
					}else if(epsg!=21781 && epsg!=2056){
						if(coordDimension==2) {
							String lcoord="lcoord"+epsg+" = COORD 0.000 .. 999999.999 [INTERLIS.m],\r\n" +
									"	            0.000 .. 999999.999 [INTERLIS.m],\r\n" +
									"	            ROTATION 2 -> 1;";
							if(!coordDimList.contains(lcoord)) {
								coordDimList.add(lcoord);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(lcoord);
							}
						}else if(coordDimension==3) {
							String hcoord="hcoord"+epsg+" = COORD 0.000 .. 999999.999 [INTERLIS.m],\r\n" +
									"	            0.000 .. 999999.999 [INTERLIS.m],\r\n" +
									"	            -999.999 .. 9999.999 [INTERLIS.m],\r\n" + 
									"	            ROTATION 2 -> 1;";
							if(!coordDimList.contains(hcoord)) {
								coordDimList.add(hcoord);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(hcoord);
							}
						}
					}
					writer.write(NEWLINE);
				}
			}
		}
	}
	
	private String getCoordDefinition(Integer coordDimension, Integer epsg){
		String coordDomainName=null;
		if(coordDimension==2) {
			coordDomainName="lcoord"+epsg;
		}else if(coordDimension==3) {
			coordDomainName="hcoord"+epsg;
		}
		return coordDomainName;
	}
	
	private String getOverlapDefinition() {
		if(DEFINEDOVERLAB!=null) {
			return " WITHOUT OVERLAPS > "+DEFINEDOVERLAB;
		}
		return null;
	}
	
	private void close(FileWriter writer) throws IOException {
		writer.flush();
		writer.close();
	}
}