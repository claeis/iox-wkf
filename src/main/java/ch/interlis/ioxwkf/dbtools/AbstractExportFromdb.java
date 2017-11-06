package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;

public abstract class AbstractExportFromdb {
	
	private int objectCount=0;
	private IoxFactoryCollection factory;
	
	public AbstractExportFromdb() {};
	
	/** export Data from database.
	 * @param file
	 * @param db
	 * @param config
	 * @throws IoxException 
	 * @throws SQLException 
	 */
	public abstract void exportData(File file,Connection db,Settings config) throws SQLException, IoxException;

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
	
	/** get all data of schemaname.tablename
	 * @param schemaTableName
	 * @param db
	 * @return the whole table of schemaname.tablename.
	 * @throws IoxException
	 * @throws SQLException
	 */
	protected final static ResultSet openTableInDb(String schemaName, String tableName,Connection db) throws SQLException {
		Statement stmt = db.createStatement();
		ResultSet rs =null;
		if(schemaName!=null) {
			rs = stmt.executeQuery("SELECT * FROM "+schemaName+"."+tableName);
		}else {
			rs = stmt.executeQuery("SELECT * FROM "+tableName);
		}
		return rs;
	}
	
	public IomObject createIomObject(String type)throws IoxException{
    	factory=new ch.interlis.iox_j.DefaultIoxFactoryCollection();
    	objectCount+=1;
		String oid="o"+objectCount;
		return factory.createIomObject(type, oid);
	}

	/** get default schema name.
	 * @param db
	 * @return default schema name.
	 * @throws SQLException
	 */
	protected final static String getDefaultSchemaName(Connection db) throws SQLException {
		ResultSet rs=null;
		Statement stmt = db.createStatement();
		rs = stmt.executeQuery("SELECT current_schema;");
	    if (rs.next()) {
	    	String schema=rs.getString("current_schema");
	      return schema;
	    }
	    return null;	
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
		EhiLogger.logState("look for models");
		ArrayList modeldirv=new ArrayList();
		String ilidirs=settings.getValue(Config.SETTING_ILIDIRS);
		if(ilidirs==null){
			ilidirs=Config.SET_ILIDIRS_DEFAULT_VALUE;
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

	/** get attribute name list of db
	 * @param rsmd
	 * @return list of attibute names.
	 * @throws SQLException
	 */
	public static List<String> getAttributeNames(ResultSetMetaData rsmd) throws SQLException {
		List<String> attributeNames=new ArrayList<String>();
		int columnCount = rsmd.getColumnCount();
		for(int i=1;i<=columnCount;i++){
			String attrName = rsmd.getColumnName(i);
			attributeNames.add(attrName);
		}
		return attributeNames;
	}
}