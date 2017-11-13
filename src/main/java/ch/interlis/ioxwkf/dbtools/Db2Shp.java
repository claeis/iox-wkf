package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.ioxwkf.shp.ShapeWriter;

public class Db2Shp extends AbstractExportFromdb {
	
	/** default model content.
	 */
	private static final String MODELNAME="model";
	
	/** default topic content.
	 */
	private static final String TOPICNAME="topic";
	
	/** export from data base table to shp file.
	 * @param file
	 * @param db
	 * @param config
	 * @throws SQLException
	 * @throws IoxException
	 */
	@Override
	public void exportData(File file,Connection db,Settings config) throws SQLException, IoxException {
		/** mandatory: set target shp file to write to.
		 */
		if(file!=null) {
			EhiLogger.logState("shp file to write to: <"+file.getAbsolutePath()+">");
		}else {
			throw new IoxException("shp-file==null.");
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
		
		/** data base connection has not to be null.
		 */
		if(db==null) {
			throw new IoxException("connection==null");
		}else if(!(db.isValid(0))) {
			throw new IoxException("connection to database: <failed>.");
		}
		EhiLogger.logState("connection to database: <success>.");
		
		/** create and set settings to shpWriter.
		 */
		ShapeWriter shpWriter=new ShapeWriter(file);

		/** create selection to get information about attributes of target data base table.
		 */
		ResultSet dbTable=null;
		try {
			dbTable=openTableInDb(definedSchemaName, definedTableName, db);
			if(definedSchemaName!=null) {
				EhiLogger.logState("db table <"+definedTableName+"> inside db schema <"+definedSchemaName+">: exist.");
			}else {
				EhiLogger.logState("db table <"+definedTableName+"> inside default db schema: exist.");
			}
		}catch(Exception e) {
			if(definedSchemaName!=null) {
				throw new IoxException("db table <"+definedTableName+"> inside db schema <"+definedSchemaName+">: not found.",e);
			}else{
				throw new IoxException("db table "+definedTableName+" inside default db schema: not found.",e);
			}
		}
		
		/** set attribute data to target attribute, to create wrapper selection.
		 */
		ResultSetMetaData metadataDbTable=dbTable.getMetaData();
		for(int k=1;k<metadataDbTable.getColumnCount()+1;k++) {
			// columnName
			String columnName=metadataDbTable.getColumnName(k);
			// columnType
			int columnType=metadataDbTable.getColumnType(k);
			// columnTypeName
			String columnTypeName=metadataDbTable.getColumnTypeName(k);
			
			// set PG Attribute Object data.
			AttributeDescriptor attr=new AttributeDescriptor();
			attr.setAttributeName(columnName);
			attr.setAttributeType(columnType);
			attr.setAttributeTypeName(columnTypeName);
			
			// put attribute to attribute descriptor list.
			attrs.add(attr);
		}
		if(attrs.size()==0) {
			if(definedSchemaName!=null) {
				throw new IoxException("no attributes found in db table: <"+definedTableName+"> inside db schema: <"+definedSchemaName+">.");
			}else {
				throw new IoxException("no attributes found in db table: <"+definedTableName+"> inside default db schema.");
			}
		}
		
		/** create selection for appropriate datatypes.
		 *  geometry datatypes are wrapped from pg to ili.
		 */
		ResultSet pg2IliConvertedTable=null;
		try {
			pg2IliConvertedTable=openPgToIliConvertedTableInDb(definedSchemaName, definedTableName, db);
		}catch(IoxException e) {
			throw new IoxException(e);
		}
		
		/** The final IomObjects will be send in ioxObjectEvents and written as individual records to the given shp file.
		 */
		EhiLogger.logState("start transfer to shp file.");
		shpWriter.write(new StartTransferEvent());
		shpWriter.write(new StartBasketEvent(MODELNAME+"."+TOPICNAME,"b1"));
		EhiLogger.logState("start to write records.");
		
		IomObject iomObject=null;
		// add attribute value, converted in appropriate type, to map of PG attribute objects.
		while(pg2IliConvertedTable.next()) {
			/** Objects within the object list will be written to shp file as a records.
			 */
			// create iomObjects
			try {
				iomObject=getRecordsAsIomObjects(definedSchemaName,definedTableName, MODELNAME, TOPICNAME, pg2IliConvertedTable, db);
			} catch (IoxException e) {
				throw new IoxException(e);
			}
			if(iomObject.getattrcount()==0) {
				throw new IoxException("no data found to export to shp file.");
			}
			try {
				shpWriter.write(new ch.interlis.iox_j.ObjectEvent(iomObject));
			}catch(Exception e) {
				throw new IoxException("export of: <"+iomObject.getobjecttag()+"> to shp file: <"+file.getAbsolutePath()+"> failed.",e);
			}
		}
		EhiLogger.logState("conversion of attributes: <successful>.");
		
		EhiLogger.logState("all records are written.");
		shpWriter.write(new EndBasketEvent());
		shpWriter.write(new EndTransferEvent());
		EhiLogger.logState("end transfer to shp file.");
		EhiLogger.logState("export: <successful>.");
		
		/** close, clear and delete all dependencies to used elements.
		 */
		if(shpWriter!=null) {
			shpWriter.close();
			shpWriter=null;
		}
	}
}