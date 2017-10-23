package ch.interlis.dbimport;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.configuration.Config;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxException;

public abstract class Import2db {
	
	public Import2db() {};
	
	/** import Data to database.
	 * @param file
	 * @param db
	 * @param config
	 * @throws IoxException 
	 * @throws SQLException 
	 */
	public abstract void importData(File file,Connection db,Settings config) throws SQLException, IoxException;

	/** get user defined model names and return them in a list of strings.
	 * @param modelNames
	 * @return separated model names in list
	 */
	protected final static List<String> getSpecifiedModelNames(String modelNames) {
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
	protected final static void insertIntoTable(String schemaName, String tableName, Map<String, String> attributes,Connection db, IomObject iomObj) throws IoxException, SQLException {
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
	protected final static List<String> getAttrNamesOfTable(String schemaName, String tableName, Connection db) throws IoxException {
		List<String> dbAttrs = new ArrayList<String>();
		ResultSet rs =null;
		try {
			Statement stmt = db.createStatement();
			if(schemaName!=null) {
				rs = stmt.executeQuery("SELECT column_name FROM information_schema.columns WHERE table_schema ='"+schemaName+"' AND table_name = '"+tableName+"'");
			}else {
				rs = stmt.executeQuery("SELECT column_name FROM information_schema.columns WHERE table_name = '"+tableName+"'");
			}
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

	/** check if table inside schema exists in data base.
	 * depends on schema exist.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return true if table exist, false if table not exist
	 * @throws SQLException
	 */
	protected final static boolean dbTableExists(String schemaName, String tableName, Connection db) throws SQLException {
		Statement stmt=db.createStatement();
		ResultSet rs =null;
		if(schemaName!=null) {
			rs =stmt.executeQuery("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '"+schemaName+"' AND table_name='"+tableName+"');");
		}else {
			rs =stmt.executeQuery("SELECT EXISTS (SELECT * FROM "+tableName+");");
		}
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
	protected final static boolean schemaExists(String schemaName, Connection db) throws SQLException {
		Statement stmt=db.createStatement();
		ResultSet rs =stmt.executeQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+schemaName+"';");
		stmt.close();
		if(rs!=null) {
			return true;
		}
		return false;
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
	public static TransferDescription compileIli(List<String> modelNames,File ilifile,String itfDir,String appHome,Settings settings) {
		ArrayList modeldirv=new ArrayList();
		String ilidirs=settings.getValue(Config.SETTING_ILIDIRS);
		if(ilidirs==null){
			ilidirs=Config.SETTING_DEFAULT_ILIDIRS;
		}
	
		EhiLogger.logState("ilidirs <"+ilidirs+">");
		String modeldirs[]=ilidirs.split(";");
		HashSet ilifiledirs=new HashSet();
		for(int modeli=0;modeli<modeldirs.length;modeli++){
			String m=modeldirs[modeli];
			if(m.contains(Config.FILE_DIR)){
				m=m.replace(Config.FILE_DIR, itfDir);
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
		ch.interlis.ili2c.Main.setHttpProxySystemProperties(settings);
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