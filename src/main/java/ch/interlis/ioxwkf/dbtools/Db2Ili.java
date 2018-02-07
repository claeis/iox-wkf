package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;

/** export all DB-Tables, inside defined DB-Schema, as INTERLIS 2.3 classes.
 */
public class Db2Ili{
	// ili file
	private static final String ILIFILE_VERSION="2.3";
	private static final String ILIFILE_MAILTO=System.getProperties().getProperty("user.name")+"@localhost.ch";
	private static final String TOPICNAME="topic1";
    private static final String NEWLINE=System.getProperty("line.separator");
	private static final String DEFINEDOVERLAB="0.01";
	private static final String INDENT="  ";
	private static final String TABLE_ROLE1_NAME="object";
	private static final String DOMAIN="DOMAIN";
	
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
		
		// optional: set the tables from the database which only have to be exported to the ili file.
		String includeTables=config.getValue(IoxWkfConfig.SETTING_INCLUDETABLES);
		if(includeTables!=null) {
			EhiLogger.logState("includeTables: <"+includeTables+">.");
		}
		
		// optional: set the tables from the database which not have to be exported to the ili file.
		String excludeTables=config.getValue(IoxWkfConfig.SETTING_EXCLUDETABLES);
		if(excludeTables!=null) {
			EhiLogger.logState("excludeTables: <"+excludeTables+">.");
		}
		
		// get all DB-Table names inside target DB-Schema.
		List<TableDescription> tableDescs = getTables(db, dbSchemaName);
		
		// get revised Tables.
		List<TableDescription> revisedTables = getEditedTables(db, dbSchemaName, tableDescs, config);
		
		// write model to ili file.
		writeModel(dbSchemaName, revisedTables, ilifile);
	}
	
	/** return all tables from defined schema.
	 * @param db DB-Connection
	 * @param schema which contains all tables.
	 * @return list of all tablenames.
	 * @throws IoxException 
	 */
	private List<TableDescription> getTables(Connection db, String schema) throws IoxException {
		DatabaseMetaData md;
		try {
			md = db.getMetaData();
		} catch (SQLException e2) {
			throw new IoxException(e2);
		}
		ResultSet rs;
		try {
			String[] tableTypes=new String[7];
			tableTypes[0]="TABLE";
			tableTypes[1]="VIEW";
			tableTypes[2]="SYSTEM TABLE";
			tableTypes[3]="GLOBAL TEMPORARY";
			tableTypes[4]="LOCAL TEMPORARY";
			tableTypes[5]="ALIAS";
			tableTypes[6]="SYNONYM";
			// get all tables of defined schema.
			rs = md.getTables(null, schema, "%", tableTypes);
		} catch (SQLException e1) {
			throw new IoxException(e1);
		}
		try {
			List<TableDescription> dbTableDescriptions=new ArrayList<TableDescription>();
			while (rs.next()) {
				TableDescription tableDesc=new TableDescription(rs.getString(TableDescription.JDBC_GETCOLUMNS_TABLENAME),rs.getString(AttributeDescriptor.JDBC_GETCOLUMNS_REMARKS));
				if(tableDesc!=null) {
					dbTableDescriptions.add(tableDesc);
				}
			}
			if(dbTableDescriptions.size()>0) {
				return dbTableDescriptions;
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
		}
	}
	
	private List<TableDescription> getEditedTables(Connection db, String dbSchemaName, List<TableDescription> tableDescs, Settings config) throws IoxException {
		String includeTables=config.getValue(IoxWkfConfig.SETTING_INCLUDETABLES);
		String excludeTables=config.getValue(IoxWkfConfig.SETTING_EXCLUDETABLES);
		
		int position=0;
		while(position<tableDescs.size()){
			TableDescription tableDesc=tableDescs.get(position);
			int tableIndex=tableDescs.indexOf(tableDesc);
			position+=1;
			
			boolean isPartOfIncludeTables=false;
			if(includeTables!=null) {
				isPartOfIncludeTables=enumerationContainsTargetName(tableDesc.getName(), includeTables);
			}
			boolean isPartOfExcludeTables=false;
			if(excludeTables!=null) {
				isPartOfExcludeTables=enumerationContainsTargetName(tableDesc.getName(), excludeTables);
			}
			
			// include/exclude tables.
			if(includeTables!=null && excludeTables==null) {
				if(isPartOfIncludeTables) {
					// include table
				}else {
					tableDescs.remove(tableIndex);
					position=0;
				}
			}else if(includeTables==null && excludeTables!=null){
				if(isPartOfExcludeTables) {
					// exclude table
					tableDescs.remove(tableIndex);
					position=0;
				}else {
					// do nothing
				}
			}else if(includeTables!=null && excludeTables!=null){
				if(isPartOfExcludeTables) {
					if(isPartOfIncludeTables) {
						// include table
					}else {
						// exclude table
						tableDescs.remove(tableIndex);
						position=0;
					}
				}else {
					if(isPartOfIncludeTables) {
						// include table
					}else {
						// exclude table
						tableDescs.remove(tableIndex);
						position=0;
					}
				}
			}
			if(tableDescs.contains(tableDesc)) {
				List<AttributeDescriptor> attrDescList=null;
				attrDescList=AttributeDescriptor.getAttributeDescriptors(dbSchemaName, tableDesc.getName(), db);
				if(attrDescList!=null) {
					// include/exclude attributes.
					int attrPosition=0;
					while(attrPosition<attrDescList.size()){
						AttributeDescriptor attribute=attrDescList.get(attrPosition);
						attrPosition+=1;
						
						// add attribute type definition.
						String iliType=getIliTypeDefinition(attribute);
						if(iliType!=null) {
							attribute.setAttributeTypeDefinition(iliType);
						}
					}
					tableDesc.setAttrDesc(attrDescList);
				}else {
					throw new IoxException("no attributes found.");
				}
			}
		}
		
		// add associations
		DatabaseMetaData md;
		try {
			md = db.getMetaData();
		} catch (SQLException e2) {
			throw new IoxException(e2);
		}
		ResultSet refResult;
		try {
			refResult = md.getCrossReference(null, dbSchemaName, null, null, dbSchemaName, null);
			while (refResult.next()) {
				for(TableDescription tableDesc:tableDescs) {
					addReferences(refResult, tableDesc);
				}
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		return tableDescs;
	}

	private void writeModel(String dbSchemaName, List<TableDescription> tableDescs, File ilifile) throws IoxException {
		// create writer
		FileWriter writer=null;
		try {
			writer=new FileWriter(ilifile);
		} catch (IOException e) {
			throw new IoxException(e);
		}
		
		// write model info
		try {
			writer.write("INTERLIS "+ILIFILE_VERSION+";");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			// model
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date date = new Date();
			writer.write("MODEL "+dbSchemaName+" AT \"mailto:"+ILIFILE_MAILTO+"\" VERSION \""+dateFormat.format(date)+"\" =");
			writer.write(NEWLINE);
			writer.write(NEWLINE);
			// topic
			writer.write(INDENT);
			writer.write("TOPIC "+TOPICNAME+" =");
			writer.write(NEWLINE);
		
			boolean domainNotDefined=true;
			List<String> coordDimList=new ArrayList<String>();
			List<AttributeDescriptor> attrDescList=null;
			for(TableDescription tableDesc : tableDescs) {
				attrDescList=tableDesc.getAttrDesc();
				for(int i=0;i<attrDescList.size();i++) {
					AttributeDescriptor attribute=attrDescList.get(i);
					if(attribute.isGeometry()) {
						Integer epsg=attribute.getSrId();
						if(epsg==null) {
							continue;
						}else {
							if(domainNotDefined) {
								domainNotDefined=false;
								// domain
								writer.write(NEWLINE);
								writer.write(INDENT);
								writer.write(INDENT);
								writer.write(DOMAIN);
								writer.write(NEWLINE);
							}
							// domain coord definition
							writeCoordValue(writer, attribute, coordDimList);
						}
					}
				}
				// write class
				writeClass(writer, tableDesc, attrDescList);
			}
			// write association
			for(TableDescription tableDesc : tableDescs) {
				List<AttributeDescriptor> attrDescs=tableDesc.getAttrDesc();
				if(attrDescs!=null) {
					for(AttributeDescriptor attrDesc : attrDescs) {
						writeAssociation(writer, tableDesc.getName(), attrDesc);
					}
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
			// close writer
			close(writer);
		} catch (IOException e) {
			throw new IoxException(e);
		}
	}
	
	/** write out a DB-Table to an ili-Classname.
	 * @param writer ili-file
	 * @param tablename tablename/classname
	 * @param attributes of table
	 * @throws IoxException
	 */
	private void writeClass(Writer writer,TableDescription tableDesc,List<AttributeDescriptor> attributes) throws IoxException{
		try {
			// create Class.
			writer.write(NEWLINE);
			writer.write(INDENT);
			writer.write(INDENT);
			if(tableDesc.getDescription()!=null) {
				writer.write("/** ");
				writer.write(tableDesc.getDescription());
				writer.write(" */");
				writer.write(NEWLINE);
				writer.write(INDENT);
				writer.write(INDENT);
			}
			writer.write("CLASS ");
			writer.write(tableDesc.getName());
			writer.write(" =");
			writer.write(NEWLINE);
			if(attributes!=null) {
				for(AttributeDescriptor attribute:attributes) {
					try {
						if(!attribute.isReference()) {
							writeAttribute(writer, attribute);
						}
					} catch (IoxException e) {
						throw new IoxException(e);
					}
				}
			}
			// end Class.
			writer.write(INDENT);
			writer.write(INDENT);
			writer.write("END ");
			writer.write(tableDesc.getName());
			writer.write(";");
			writer.write(NEWLINE);
		} catch (IOException e) {
			throw new IoxException(e);
		}
	}
	
	/** write a DB-Column as an ili-type.
	 * @param writer ili-file
	 * @param attribute write in Interlis-Syntax.
	 * @throws IoxException
	 * @throws IOException 
	 */
	private void writeAttribute(Writer writer,AttributeDescriptor attribute) throws IoxException, IOException {
		Boolean isMandatory=false;
		writer.write(INDENT);
		writer.write(INDENT);
		writer.write(INDENT);
		try {
			if(attribute.getColumnRemarks()!=null) {
				writer.write("/** ");
				writer.write(attribute.getColumnRemarks());
				writer.write(" */");
				writer.write(NEWLINE);
				writer.write(INDENT);
				writer.write(INDENT);
				writer.write(INDENT);
			}
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
			String iliType = attribute.getAttributeTypeDefinition();
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
				if(coordDimension==2) {
					resultType.append("lcoord"+epsg);
				}else if(coordDimension==3) {
					resultType.append("hcoord"+epsg);
				}
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
				if(coordDimension==2) {
					resultType.append("lcoord"+epsg);
				}else if(coordDimension==3) {
					resultType.append("hcoord"+epsg);
				}
				if(DEFINEDOVERLAB!=null) {
					resultType.append(" WITHOUT OVERLAPS > "+DEFINEDOVERLAB);
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
			}else if(dataType.equals(Types.LONGVARCHAR)) {
				resultType.append("TEXT*"+precision);
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
			}else if(dataType.equals(Types.DECIMAL)) {
				resultType.append("-"+precision+" .. "+precision);
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
			}else {
				resultType.append("TEXT");
			}
		}
		return resultType.toString();
	}
	
	private void addReferences(ResultSet refResult, TableDescription tableDesc) throws SQLException {
		List<AttributeDescriptor> attrDescs=tableDesc.getAttrDesc();
		for(AttributeDescriptor attrDesc : attrDescs) {
			String fkColumn=refResult.getString(AttributeDescriptor.JDBC_GETCOLUMNS_FKCOLUMNNAME);
			String pkColumn=refResult.getString(AttributeDescriptor.JDBC_GETCOLUMNS_PKTABLENAME);
			String columnname=attrDesc.getDbColumnName();
			if(columnname!=null && columnname.equals(fkColumn)) {
				attrDesc.setTargetTableName(pkColumn);
				attrDesc.setReferenceColumnName(fkColumn);
			}
		}
	}
	
	private void writeCoordValue(FileWriter writer, AttributeDescriptor attribute, List<String> coordDimList) throws IOException {
		Integer coordDimension=attribute.getCoordDimension();
		Integer epsg=attribute.getSrId();
		writer.write(INDENT);
		writer.write(INDENT);
		writer.write(INDENT);
		// epsg: CHLV95
		if(epsg==2056) {
			if(coordDimension==2) {
				String lcoord="lcoord"+epsg;
				if(!coordDimList.contains(lcoord)) {
					coordDimList.add(lcoord);
					writer.write(lcoord);
					writer.write(" = COORD 2460000.000 .. 2870000.000 [INTERLIS.m],"+NEWLINE+ 
							"	            1045000.000 .. 1310000.000 [INTERLIS.m],"+NEWLINE+ 
							"	            ROTATION 2 -> 1;");
				}
			}else if(coordDimension==3) {
				String hcoord="hcoord"+epsg;
				if(!coordDimList.contains(hcoord)) {
					coordDimList.add(hcoord);
					writer.write(hcoord);
					writer.write(" = COORD 2460000.000 .. 2870000.000 [INTERLIS.m],"+NEWLINE+
							"	            1045000.000 .. 1310000.000 [INTERLIS.m],"+NEWLINE+  
							"	            -200.000 .. 5000.000 [INTERLIS.m],"+NEWLINE+ 
							"	            ROTATION 2 -> 1;");
				}
			}
		// epsg: CHLV03
		}else if(epsg==21781) {
			if(coordDimension==2) {
				String lcoord="lcoord"+epsg;
				if(!coordDimList.contains(lcoord)) {
					coordDimList.add(lcoord);
					writer.write(lcoord);
					writer.write(" = COORD 460000.000 .. 870000.000 [INTERLIS.m],"+NEWLINE+ 
							"	        45000.000 .. 310000.000 [INTERLIS.m],"+NEWLINE+ 
							"	        ROTATION 2 -> 1;");
				}
			}else if(coordDimension==3) {
				String hcoord="hcoord"+epsg;
				if(!coordDimList.contains(hcoord)) {
					coordDimList.add(hcoord);
					writer.write(hcoord);
					writer.write(" = COORD 460000.000 .. 870000.000 [INTERLIS.m],"+NEWLINE+
							"	            45000.000 .. 310000.000 [INTERLIS.m],"+NEWLINE+
							"	            -200.000 .. 5000.000 [INTERLIS.m],"+NEWLINE+
							"	            ROTATION 2 -> 1;");
				}
			}
		// epsg: other or null
		}else if(epsg!=21781 && epsg!=2056){
			if(coordDimension==2) {
				String lcoord="lcoord"+epsg;
				if(!coordDimList.contains(lcoord)) {
					coordDimList.add(lcoord);
					writer.write(lcoord);
					writer.write(" = COORD -999999.999 .. 999999.999,"+NEWLINE+ 
							"	            -999999.999 .. 999999.999,"+NEWLINE+ 
							"	            ROTATION 2 -> 1;");
				}
			}else if(coordDimension==3) {
				String hcoord="hcoord"+epsg;
				if(!coordDimList.contains(hcoord)) {
					coordDimList.add(hcoord);
					writer.write(hcoord);
					writer.write(" = COORD -999999.999 .. 999999.999,"+NEWLINE+ 
							"	            -999999.999 .. 999999.999,"+NEWLINE+ 
							"	            -9999.999 .. 9999.999,"+NEWLINE+ 
							"	            ROTATION 2 -> 1;");
				}
			}
		}
		writer.write(NEWLINE);
	}
	
	private void writeAssociation(FileWriter writer, String sourceTableName,AttributeDescriptor referenceColumn) throws IoxException {
		String fkColumn=referenceColumn.getReferenceColumnName();
		String refTableName=referenceColumn.getTargetTableName();
		Boolean mandatory = referenceColumn.isMandatory();
		String refMinCard="0";
		if(mandatory) {
			refMinCard="1";
		}
		String refMaxCard="1";
		
		String sourceMinCard="0";
		// card==Cardinality.UNBOUND
		String sourceMaxCard="*";
		
		if(refTableName!=null && fkColumn!=null){
			try {
				// create Association.
				writer.write(NEWLINE);
				writer.write(INDENT);
				writer.write(INDENT);
				writer.write("ASSOCIATION ");
				writer.write(sourceTableName+fkColumn);
				writer.write(" =");
				writer.write(NEWLINE);
				writer.write(INDENT);
				writer.write(INDENT);
				writer.write(INDENT);
				writer.write(Db2Ili.TABLE_ROLE1_NAME+" -- {"+sourceMinCard+".."+sourceMaxCard+"} "+sourceTableName+";");
				writer.write(NEWLINE);
				writer.write(INDENT);
				writer.write(INDENT);
				writer.write(INDENT);
				if(referenceColumn.getColumnRemarks()!=null) {
					writer.write("/** "+referenceColumn.getColumnRemarks()+" */");
					writer.write(NEWLINE);
					writer.write(INDENT);
					writer.write(INDENT);
					writer.write(INDENT);
				}
				writer.write(fkColumn+" -- {"+refMinCard+".."+refMaxCard+"} "+refTableName+";");
				writer.write(NEWLINE);
				// end Association.
				writer.write(INDENT);
				writer.write(INDENT);
				writer.write("END ");
				writer.write(sourceTableName+fkColumn);
				writer.write(";");
				writer.write(NEWLINE);
			} catch (IOException e) {
				throw new IoxException(e);
			}
		}
	}
	
	private boolean enumerationContainsTargetName(String targetName, String names) {
		String[] parts = names.split(Pattern.quote(";"));
		if(Arrays.asList(parts).contains(targetName)) {
			return true;
		}
		return false;
	}
	
	private void close(FileWriter writer) throws IOException {
		writer.flush();
		writer.close();
	}
}