package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Main;
import ch.interlis.ili2c.metamodel.DataModel;
import ch.interlis.ili2c.metamodel.Element;
import ch.interlis.ili2c.metamodel.Model;
import ch.interlis.ili2c.metamodel.Topic;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.csv.CsvWriter;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;

public class Db2Csv extends AbstractExportFromdb {
	
	private String modelName=null;
	private String topicName=null;
	private String className=null;
	private TransferDescription td=null;
	private HashMap<Viewable, Topic> iliTopics=null;
	private List<HashMap<Viewable, Model>> listOfIliClasses=null;
	private HashMap<Viewable, Model> iliClasses=null;

	/** export from data base table to csv file.
	 * @param file
	 * @param db
	 * @param config
	 * @throws SQLException
	 * @throws IoxException
	 */
	@Override
	public void exportData(File file,Connection db,Settings config) throws SQLException, IoxException {
		CsvWriter csvWriter=new CsvWriter(file);
		String definedIliDirs=config.getValue(Config.SETTING_ILIDIRS);
		String definedModelNames=config.getValue(Config.SETTING_MODELNAMES);
		String definedDelimiter=config.getValue(Config.SETTING_QUOTATIONMARK);
		String definedRecordDelimiter=config.getValue(Config.SETTING_VALUEDELIMITER);
		String definedSchemaName=config.getValue(Config.SETTING_DBSCHEMA);
		String definedTableName=config.getValue(Config.SETTING_DBTABLE);
		List<String> modelNames=null;
		
		EhiLogger.logState("dataFile <"+file.getAbsolutePath()+">");
		if(definedModelNames!=null){
			EhiLogger.logState("modelNames <"+definedModelNames+">");
		}
		if(definedIliDirs!=null){
			EhiLogger.logState("ilidirs <"+definedIliDirs+">");
		}
		if(definedDelimiter!=null){
			EhiLogger.logState("delimiter <"+definedDelimiter+">");
		}
		if(definedRecordDelimiter!=null){
			EhiLogger.logState("record delimiter <"+definedRecordDelimiter+">");
		}
		
		if(definedTableName!=null){
			EhiLogger.logState("tablename <"+definedTableName+">");
		}else {
			throw new IoxException("expected tablename");
		}
		
		// validity of connection
		if(db==null) {
			throw new IoxException("connection==null");
		}else if(!(db.isValid(0))) {
			throw new IoxException("connection to: "+db+" failed");
		}else {
			EhiLogger.logState("connection to <"+db+"> successful");
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
			modelNames=getSpecifiedModelNames(definedModelNames);
			String filePath=null;
			if(definedIliDirs==null) {
				filePath=new java.io.File(file.getPath()).getAbsoluteFile().getParentFile().getAbsolutePath();
			}else {
				filePath=new java.io.File(dirList.get(0)).getAbsoluteFile().getAbsolutePath();
			}
			td=compileIli(modelNames,null,filePath,Main.getIli2cHome(),config);
			if(td==null){
				throw new IoxException("models "+modelNames.toString()+" not found");
			}
		}else {
			// set modelname
			modelName=file.getName();
			topicName="Topic1";
			className="Class1";
		}
		
		// delimiter validity
		if(definedDelimiter==null) {
			definedDelimiter=Config.SET_QUOTATIONMARK;
			EhiLogger.logState("delimiter <"+definedDelimiter+">");
		}
		
		// record delimiter validity
		if(definedRecordDelimiter==null) {
			definedRecordDelimiter=Config.SET_DEFAULT_VALUEDELIMITER;
			EhiLogger.logState("record delimiter <"+definedRecordDelimiter+">");
		}
		
		// build csvWriter
		csvWriter.setHeader(Config.SET_FIRSTLINE_AS_VALUE);
		EhiLogger.logState("create header");
		csvWriter.setDelimiter(definedDelimiter);
		csvWriter.setRecordDelimiter(definedRecordDelimiter);
		if(td!=null) {
			csvWriter.setModel(td);
		}
		List<IomObject> objectList = new ArrayList<IomObject>();
		// get db table content
		ResultSet tableInDb=null;
		try {
			tableInDb=AbstractExportFromdb.openTableInDb(definedSchemaName, definedTableName, db);
		}catch(Exception e) {
			if(definedSchemaName!=null) {
				throw new IoxException("table "+definedSchemaName+"."+definedTableName+" not found");
			}else {
				throw new IoxException("table "+definedTableName+" not found");
			}
		}
		ResultSetMetaData rsmd = tableInDb.getMetaData();
		
		List<String> attributeNames=AbstractExportFromdb.getAttributeNames(rsmd);
		if(attributeNames.size()!=0) {
			if(td!=null) {
				// compare columnNames with models and get model with same attribute names.
				Viewable modelClass=getAppropriateClassOfModel(td, attributeNames);
				if(modelClass==null) {
					throw new IoxException("class attribute names "+attributeNames.toString()+" not found in given models "+modelNames.toString());
				}else {
					className=modelClass.getName();
				}
			}
		}else {
			throw new IoxException("no attributes found on table "+definedTableName);
		}
		
		int columnCount = rsmd.getColumnCount();
		while(tableInDb.next()){
			IomObject iomObj=createIomObject(modelName+"."+topicName+"."+className);
			for(int i=1;i<=columnCount;i++){
				String attrName = rsmd.getColumnName(i);
				String attrValue=null;
				// db value null, set empty String="".
				if(tableInDb.getObject(i)==null) {
					attrValue="";
				}else {
					attrValue=(String) tableInDb.getObject(i);
				}
				iomObj.setattrvalue(attrName, attrValue);
			}
			objectList.add(iomObj);
		}
		if(objectList.size()==0) {
			throw new IoxException("no attributes found in data base table");
		}
		
		// write IoxEvents
		csvWriter.write(new StartTransferEvent());
		csvWriter.write(new StartBasketEvent(modelName+"."+topicName,"b1"));
		
		for(IomObject iomObj:objectList) {
			csvWriter.write(new ch.interlis.iox_j.ObjectEvent(iomObj));
		}
		
		csvWriter.write(new EndBasketEvent());
		csvWriter.write(new EndTransferEvent());
		
		// close csvReader
		if(csvWriter!=null) {
			csvWriter.close();
			csvWriter=null;
			modelName=null;
			topicName=null;
			className=null;
			td=null;
			EhiLogger.logState("export to <"+file.getAbsolutePath()+"> successful");
		}
	}
	
    /** Iterate through ili file
	 */
    private void setupNameMapping(){
    	iliTopics=new HashMap<Viewable, Topic>();
    	listOfIliClasses=new ArrayList<HashMap<Viewable, Model>>();
		Iterator tdIterator = td.iterator();
		while(tdIterator.hasNext()){
			iliClasses=new HashMap<Viewable, Model>();
			Object modelObj = tdIterator.next();
			if(!(modelObj instanceof DataModel)){
				continue;
			}
			// iliModel
			DataModel model = (DataModel) modelObj;
			modelName=model.getName();
			Iterator modelIterator = model.iterator();
			while(modelIterator.hasNext()){
				Object topicObj = modelIterator.next();
				if(!(topicObj instanceof Topic)){
					continue;
				}
				// iliTopic
				Topic topic = (Topic) topicObj;
				topicName=topic.getName();
				// iliClass
				Iterator classIter=topic.iterator();
		    	while(classIter.hasNext()){
		    		Object classObj=classIter.next();
		    		if(!(classObj instanceof Viewable)){
    					continue;
    				}
		    		Viewable viewable = (Viewable) classObj;
	    			iliClasses.put(viewable, model);
	    			iliTopics.put(viewable, topic);
		    	}
			}
			listOfIliClasses.add(iliClasses);
		}
    }
    
	private Viewable getAppropriateClassOfModel(TransferDescription td, List<String> attrNames) throws IoxException {
		List<String> foundClasses=null;
    	Viewable viewable=null;
    	if(iliClasses==null){
    		setupNameMapping();
    	}
    	foundClasses=new ArrayList<String>();
    	// first last model file.
    	for(HashMap<Viewable, Model> mapIliClasses : listOfIliClasses){
    		for(Viewable iliViewable : mapIliClasses.keySet()){
    			List<String> iliAttrs=new ArrayList<String>();
    			Iterator attrIter=iliViewable.getAttributes();
    			while(attrIter.hasNext()){
    				Element attribute=(Element) attrIter.next();
    				iliAttrs.add(attribute.getName());
    			}
    			if(iliAttrs.equals(attrNames)){
					viewable=iliViewable;
					modelName=mapIliClasses.get(iliViewable).getName();
					foundClasses.add(viewable.getScopedName());
				}
    		}
    	}
    	if(foundClasses.size()>1) {
    		throw new IoxException("several possible classes were found: "+foundClasses.toString());
    	}else if(foundClasses.size()==1){
    		return viewable;
    	}
    	return null;
	}
}