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
	private static final String ILIFILE_MAILTO="ce@eisenhutinformatik.ch";
	private static final String TOPICNAME="topic1";
    private static final char NEWLINE='\n';
	private static final String DEFINEDOVERLAB="0.01";
	private static final String INDENT="  ";
	
	/** export tablestructure to ili-model.
	 * {@link IoxWkfConfig#SETTING_DBSCHEMA} DB-Schema includes all tables to export.
	 * @throws IOException 
	 */
	public void exportData(File ilifile,Connection db,Settings config) throws IoxException {
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
		String definedSchemaName=config.getValue(IoxWkfConfig.SETTING_DBSCHEMA);
		if(definedSchemaName!=null) {
			EhiLogger.logState("db schema name: <"+definedSchemaName+">.");
		}else {
			throw new IoxException("db schema name==null");
		}
		
		// get all DB-Table names inside target DB-Schema.
		List<String> dbTableNames=null;
		try {
			dbTableNames=getTables(db, definedSchemaName);
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
		
		// get the list of attributeDescriptors of each DB-Table
		List<AttributeDescriptor> attrDesc=null;
		try {
			writer.write("INTERLIS "+ILIFILE_VERSION+";");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			// model
			writer.write("MODEL "+definedSchemaName+" AT \"mailto:"+ILIFILE_MAILTO+"\" VERSION \""+getCurrentDate()+"\" =");
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
			
			// epsg 2056 coord length definition
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("scoord2056 = COORD 2460000.000 .. 2870000.000 [INTERLIS.m];");
			writer.write(NEWLINE);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("hcoord2056 = COORD 2460000.000 .. 2870000.000 [INTERLIS.m], 1045000.000 .. 1310000.000 [INTERLIS.m], ROTATION 2 -> 1;");
			writer.write(NEWLINE);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("lcoord2056 = COORD 2460000.000 .. 2870000.000 [INTERLIS.m], 1045000.000 .. 1310000.000 [INTERLIS.m], -200.000 .. 5000.000 [INTERLIS.m], ROTATION 2 -> 1;");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			
			// epsg 21781 coord length definition
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("scoord21781 = COORD 460000.000 .. 870000.000 [INTERLIS.m];");
			writer.write(NEWLINE);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("hcoord21781 = COORD 460000.000 .. 870000.000 [INTERLIS.m], 45000.000 .. 310000.000 [INTERLIS.m], ROTATION 2 -> 1;");
			writer.write(NEWLINE);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("lcoord21781 = COORD 460000.000 .. 870000.000 [INTERLIS.m], 45000.000 .. 310000.000 [INTERLIS.m], -200.000 .. 5000.000 [INTERLIS.m], ROTATION 2 -> 1;");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			
			// epsg other coord length definition
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("scoordOther = COORD 0.000 .. 999999.999 [INTERLIS.m];");
			writer.write(NEWLINE);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("hcoordOther = COORD 0.000 .. 999999.999 [INTERLIS.m], 0.000 .. 999999.999 [INTERLIS.m], ROTATION 2 -> 1;");
			writer.write(NEWLINE);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("lcoordOther = COORD 0.000 .. 999999.999 [INTERLIS.m], 0.000 .. 999999.999 [INTERLIS.m], -999.999 .. 9999.999 [INTERLIS.m], ROTATION 2 -> 1;");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			
		} catch (IOException e) {
			throw new IoxException(e);
		}
		try {
			for(String dbTableName : dbTableNames) {
				attrDesc=AttributeDescriptor.getAttributeDescriptors(definedSchemaName, dbTableName, db);
				if(attrDesc!=null) {
					try {
						AttributeDescriptor.addGeomDataToAttributeDescriptors(definedSchemaName, dbTableName, attrDesc, db);
					} catch (SQLException e) {
						throw new IoxException(e);
					}
					// write to class
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
			writer.write(definedSchemaName);
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
			Boolean isMandatory=attribute.isMandatory();
			if(isMandatory!=null && isMandatory) {
				writer.write("MANDATORY");
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
				resultType.append("-131072.16383 .. 131072.16383");
			}else if(dataType.equals(Types.SMALLINT)) {
				resultType.append("-32768 .. 32767");
			}else if(dataType.equals(Types.INTEGER)) {
				resultType.append("-2147483648 .. 2147483647");
			}else if(dataType.equals(Types.BIGINT)) {
				resultType.append("-9223372036854775808 .. 9223372036854775807");
			}else if(dataType.equals(Types.FLOAT)) {
				resultType.append("-131072.16383 .. 131072.16383");
			}else if(dataType.equals(Types.DOUBLE)) {
				resultType.append("-131072.16383 .. 131072.16383");
			}else if(dataType.equals(Types.REAL)) {
				resultType.append("-131072.163830 .. 131072.163830");
			}else if(dataType.equals(Types.LONGNVARCHAR)) {
				resultType.append("TEXT");
			}else if(dataType.equals(Types.DECIMAL)) {
				resultType.append("-131072.163830000000000 .. 131072.16383000000000");
			}else if(dataType.equals(Types.CHAR)) {
				resultType.append("TEXT");
			}else if(dataType.equals(Types.NCHAR)) {
				resultType.append("TEXT*"+precision);
			}else if(dataType.equals(Types.VARCHAR)) {
				resultType.append("TEXT");
			}else if(dataType.equals(Types.NVARCHAR)) {
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
	
	private String getCoordDefinition(Integer coordDimension, Integer epsg) {
		String coordDefinition=null;
		// epsg: CHLV95
		if(epsg!=null && epsg==2056) {
			if(coordDimension==1) {
				coordDefinition="scoord2056";
			}else if(coordDimension==2) {
				coordDefinition="hcoord2056";
			}else if(coordDimension==3) {
				coordDefinition="lcoord2056";
			}
		// epsg: CHLV03
		}else if(epsg!=null && epsg==21781) {
			if(coordDimension==1) {
				coordDefinition="scoord21781";
			}else if(coordDimension==2) {
				coordDefinition="hcoord21781";
			}else if(coordDimension==3) {
				coordDefinition="lcoord21781";
			}
		// epsg: other or null
		}else {
			if(coordDimension==1) {
				coordDefinition="scoordOther";
			}else if(coordDimension==2) {
				coordDefinition="hcoordOther";
			}else if(coordDimension==3) {
				coordDefinition="lcoordOther";
			}
		}
		return coordDefinition;
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