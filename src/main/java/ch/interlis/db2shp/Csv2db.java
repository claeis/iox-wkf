package ch.interlis.db2shp;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.ObjectEvent;

public class Csv2db {
	
	/** import csvData to database.
	 * @param file
	 * @param db
	 * @param config
	 * @throws SQLException
	 * @throws IoxException
	 */
	public static void importData(File file,Connection db,Settings config) throws SQLException, IoxException {
		Map<String, String> attributes=new HashMap<String, String>();
		CsvReader csvReader=new CsvReader(file);
		String definedHeader=config.getValue(Config.HEADER);
		String definedIliDirs=config.getValue(Config.SETTING_ILIDIRS);
		String definedModelNames=config.getValue(Config.SETTING_MODELNAMES);
		String definedDelimiter=config.getValue(Config.DELIMITER);
		String definedRecordDelimiter=config.getValue(Config.RECORD_DELIMITER);
		String definedSchemaName=config.getValue(Config.DBSCHEMA);
		String definedTableName=config.getValue(Config.TABLE);
		TransferDescription td=null;
		
		// validity of file
		if(file==null) {
			throw new IoxException("expected csv file");
		}
		if(!(file.exists())) {
			throw new IoxException("csv file: "+file.getAbsolutePath()+" not found");
		}
		if(!(file.canRead())) {
			throw new IoxException("csv file: "+file.getAbsolutePath()+" not readable");
		}
		
		// validity of connection
		if(db==null) {
			throw new IoxException("expected url, username and password");
		}else if(!(db.isValid(0))) {
			throw new IoxException("connection to: "+db+" failed");
		}
		
		// header validity
		if(definedHeader==null || !definedHeader.equals(Config.HEADERPRESENT)) {
			definedHeader=Config.HEADERABSENT;
		}
		
		// model directory validity
		List<String> dirList=new ArrayList<String>();
		if(definedIliDirs!=null) {
			String[] dirs=definedIliDirs.split(";");
			for(String dir:dirs) {
				dirList.add(dir);
			}
		}
		
		// models validity
		if(definedModelNames!=null) {
			List<String> modelNames=getSpecifiedModelNames(definedModelNames);
			String filePath=null;
			if(definedIliDirs==null) {
				filePath=new java.io.File(file.getPath().toString()).getAbsoluteFile().getParentFile().getAbsolutePath();
			}else {
				filePath=new java.io.File(dirList.get(0).toString()).getAbsoluteFile().getAbsolutePath();
			}
			td=compileIli(modelNames, null,filePath,getAppHome(),config);
			if(td==null){
				throw new IoxException("models "+modelNames.toString()+" not found");
			}
		}
		
		// delimiter validity
		if(definedDelimiter==null) {
			definedDelimiter=Config.DEFAULT_DELIMITER;
		}
		
		// record delimiter validity
		if(definedRecordDelimiter==null) {
			definedRecordDelimiter=Config.DEFAULT_RECORD_DELIMITER;
		}
		
		// build csvReader
		if(definedHeader.equals(Config.HEADERPRESENT)) {
			csvReader.setHeader(Config.HEADERPRESENT);
		}else {
			csvReader.setHeader(Config.HEADERABSENT);
		}
		CsvReader.setDelimiter(definedDelimiter);
		csvReader.setRecordDelimiter(definedRecordDelimiter);
		if(td!=null) {
			csvReader.setModel(td);
		}
		
		// read IoxEvents
		IoxEvent event=csvReader.read();
		while(event instanceof IoxEvent){
			if(event instanceof ObjectEvent) {
				IomObject iomObj=((ObjectEvent)event).getIomObject();
				
				// schema validity
				if(definedSchemaName!=null) {
					if(!(schemaExists(definedSchemaName, db))){
						throw new IoxException("schema "+definedSchemaName+" not found");
					}
				}
				
				// table validity
				List<String> attrNames=new ArrayList<String>();
				if(config.getValue(Config.TABLE)!=null){
					if(definedSchemaName==null) {
						// default schema
						definedSchemaName=findSchemaName(definedTableName, db);
					}
					if(dbTableExists(definedSchemaName, definedTableName, db)) {
						// attribute names of database table
						attrNames=getAttrNamesOfTable(definedSchemaName, definedTableName, db);
					}else {
						throw new IoxException("table "+definedTableName+" not found");
					}
				}else {
					throw new IoxException("expected tablename");
				}
				
				// build attributes
				attributes.clear();
				for(int i=0;i<iomObj.getattrcount();i++) {
					if(attrNames.contains(iomObj.getattrname(i))) {
						attributes.put(iomObj.getattrname(i), iomObj.getattrvalue(iomObj.getattrname(i)));
					}
				}
				if(attributes.size()==0) {
					throw new IoxException("data base attribute names: "+attrNames.toString()+" not found in "+file.getAbsolutePath());
				}
				
				// insert attributes to database
				insertIntoTable(definedSchemaName, definedTableName, attributes, db, iomObj);
				event=csvReader.read();
			}else {
				// next IoxEvent
				event=csvReader.read();
			}
		}
		
		// close csvReader
		if(csvReader!=null) {
			csvReader.close();
			csvReader=null;
		}
		event=null;
	}

	/** Gets main folder of program.
	 * @return folder Main folder of program.
	 */
	private static String getAppHome(){
	  String classpath = System.getProperty("java.class.path");
	  int index = classpath.toLowerCase().indexOf(Config.APP_JAR);
	  int start = classpath.lastIndexOf(java.io.File.pathSeparator,index) + 1;
	  if(index > start)
	  {
		  return classpath.substring(start,index - 1);
	  }
	  return null;
	}

	/** get user defined model names and return them in a list of strings.
	 * @param modelNames
	 * @return separated model names in list
	 */
	private static List<String> getSpecifiedModelNames(String modelNames) {
		List<String> ret=new ArrayList<String>();
		if(modelNames!=null){
			String[] modelNameList = modelNames.split(";");
			for(String modelName:modelNameList){
				ret.add(modelName);
			}
		}
		return ret;
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
	 */
	private static void insertIntoTable(String schemaName, String tableName, Map<String, String> attributes,Connection db, IomObject iomObj) throws IoxException, SQLException {
		String comma1="";
		String comma2="";
		StringBuilder attrNames=new StringBuilder();
		StringBuilder attrValues=new StringBuilder();
		
		for(Entry<String,String> attribute:attributes.entrySet()){
			// attribute names
			attrNames.append(comma1);
			attrNames.append(attribute.getKey());
			comma1=",";
			
			// attribute values
			attrValues.append(comma2);
			attrValues.append("'"+attribute.getValue()+"'");
			comma2=",";
		}
		Statement stmt = db.createStatement();
		if(schemaName!=null) {
			try {
				int result=stmt.executeUpdate("INSERT INTO "+schemaName+"."+tableName+"("+attrNames.toString()+") VALUES ("+attrValues.toString()+")");
			}catch(SQLException e) {
				throw new IoxException("import of "+iomObj.toString()+" to "+schemaName+"."+tableName+" failed",e);
			}
		}else {
			try {
				int result=stmt.executeUpdate("INSERT INTO "+tableName+"("+attrNames.toString()+") VALUES ("+attrValues.toString()+")");
			}catch(SQLException e) {
				throw new IoxException("import of "+iomObj.toString()+" to "+tableName+" failed",e);
			}
		}
	}

	/** create selection to table inside schema and get the attribute names on data base table.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return attribute names
	 * @throws IoxException
	 */
	private static List<String> getAttrNamesOfTable(String schemaName, String tableName, Connection db) throws IoxException {
		List<String> dbAttrs = new ArrayList<String>();
		ResultSet rs =null;
		try {
			Statement stmt = db.createStatement();
			rs = stmt.executeQuery("SELECT column_name FROM information_schema.columns WHERE table_schema ='"+schemaName+"' AND table_name = '"+tableName+"'");
			if(rs==null) {
				throw new IoxException("table "+schemaName+"."+tableName+" not found");
			}
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			if(columnCount>1) {
				throw new IoxException("tablename multiple times defined in data base");
			}
			while(rs.next()){
				for(int i=1;i<=columnCount;i++){
					dbAttrs.add(rs.getString(i));
				}
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		return dbAttrs;
	}

	/** find schema which contain defined table
	 * @param tableName
	 * @param db
	 * @return schema name
	 * @throws SQLException
	 * @throws IoxException
	 */
	private static String findSchemaName(String tableName, Connection db) throws SQLException, IoxException {
		// find out schemaName of table
		Statement stmt = db.createStatement();
		ResultSet rs=null;
		rs = stmt.executeQuery("SELECT tables.table_schema FROM information_schema.tables WHERE tables.table_name = '"+tableName+"'");
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		if(columnCount>1) {
			throw new IoxException("tablename in more than one schema defined");
		}
		while(rs.next()){
			for(int i=1;i<=columnCount;i++){
				String result=(rs.getString(i));
				return result;
			}
		}
		return null;
	}

	/** check if table inside schema exists in data base.
	 * depends on schema exist.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return true if table exist, false if table not exist
	 * @throws SQLException
	 */
	private static boolean dbTableExists(String schemaName, String tableName, Connection db) throws SQLException {
		Statement stmt=db.createStatement();
		ResultSet rs =stmt.executeQuery("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '"+schemaName+"' AND table_name='"+tableName+"');");
		stmt.close();
		if(rs!=null) {
			return true;
		}
		return false;
	}

	/** check if schema exists in data base.
	 * @param schemaName
	 * @param db
	 * @return true if schema exist, false if schema not exist
	 * @throws SQLException
	 */
	private static boolean schemaExists(String schemaName, Connection db) throws SQLException {
		Statement stmt=db.createStatement();
		ResultSet rs =stmt.executeQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+schemaName+"';");
		stmt.close();
		if(rs!=null) {
			return true;
		}
		return false;
	}

	public static void exportData(File file,Connection db,Settings config) {
		
	}
	
	/** Compiles the required Interlis models.
	 * @param aclass Interlis qualified class name of a required class.
	 * @param ilifile Interlis model file to read. null if not known.
	 * @param modelDir Folder with Interlis model files or null.
	 * @param appHome Folder of program. Function will check in ilimodels sub-folder for Interlis models.
	 * @param config Configuration of program.
	 * @return root object of java representation of Interlis model.
	 * @throws IoxException 
	 * @see #SETTING_ILIDIRS
	 */
	public static TransferDescription compileIli(List<String> modelNames,File ilifile,String modelDir,String appHome,Settings config) throws IoxException {
		ArrayList modeldirv=new ArrayList();
		String ilidirs=config.getValue(Config.SETTING_ILIDIRS);
		if(ilidirs==null){
			ilidirs=Config.SETTING_DEFAULT_ILIDIRS;
		}else {
			ilidirs=modelDir;
		}
		EhiLogger.logState("ilidirs <"+ilidirs+">");
		String modeldirs[]=ilidirs.split(";");
		HashSet ilifiledirs=new HashSet();
		for(int modeli=0;modeli<modeldirs.length;modeli++){
			String m=modeldirs[modeli];
			if(m.contains(Config.CSV_DIR)){
				m=m.replace(Config.CSV_DIR, modelDir);
				if(m!=null && m.length()>0){
					if(!modeldirv.contains(m)){
						modeldirv.add(m);
					}
				}
			}else if(m.contains(Config.JAR_DIR)){
				if(appHome!=null){
					m=m.replace(Config.JAR_DIR,appHome);
				}
				if(m!=null){
					m=new java.io.File(m).getAbsolutePath();
				}
				if(m!=null && m.length()>0){
					modeldirv.add(m);				
				}
			}else{
				if(m!=null && m.length()>0){
					modeldirv.add(m);				
				}
			}
		}		
		ch.interlis.ili2c.Main.setHttpProxySystemProperties(config);
		TransferDescription td=null;
		ch.interlis.ili2c.config.Configuration ili2cConfig=null;
		if(ilifile!=null){
			try {
				ch.interlis.ilirepository.IliManager modelManager=new ch.interlis.ilirepository.IliManager();
				modelManager.setRepositories((String[])modeldirv.toArray(new String[]{}));
				ArrayList<String> ilifiles=new ArrayList<String>();
				ilifiles.add(ilifile.getPath());
				ili2cConfig=modelManager.getConfigWithFiles(ilifiles);
				ili2cConfig.setGenerateWarnings(false);
			} catch (Ili2cException ex) {
				EhiLogger.logError(ex);
				return null;
			}
		}else{
			ArrayList<String> modelv=new ArrayList<String>();
			if(modelNames!=null){
				modelv.addAll(modelNames);
			}
			try {
				ch.interlis.ilirepository.IliManager modelManager=new ch.interlis.ilirepository.IliManager();
				modelManager.setRepositories((String[])modeldirv.toArray(new String[]{}));
				ili2cConfig=modelManager.getConfig(modelv, 0.0);
				ili2cConfig.setGenerateWarnings(false);
			} catch (Ili2cException ex) {
				EhiLogger.logError(ex);
				return null;
			}
			
		}
		try {
			ch.interlis.ili2c.Ili2c.logIliFiles(ili2cConfig);
			td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		} catch (Ili2cFailure ex) {
			EhiLogger.logError(ex);
			return null;
		}
		return td;
	}
}