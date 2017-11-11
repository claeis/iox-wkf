package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.converter.ConverterException;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.ObjectEvent;

public class Csv2db extends AbstractImport2db {
	
	/** import csvData to database.
	 * @param file
	 * @param db
	 * @param config
	 * @throws SQLException
	 * @throws IoxException
	 */
	@Override
	public void importData(File file,Connection db,Settings config) throws SQLException, IoxException {
		Map<String, PgAttributeObject> attrsPool=new HashMap<String, PgAttributeObject>();
		Set notFoundAttrs=new HashSet();
		
		if(!(file.exists())) {
			throw new IoxException("csv file: "+file.getAbsolutePath()+" not found");
		}else if(!(file.canRead())) {
			throw new IoxException("csv file: "+file.getAbsolutePath()+" not readable");
		}else {
			EhiLogger.logState("dataFile <"+file.getAbsolutePath()+">");
		}
		
		/** mandatory: set csv file, which contains data to import.
		 */
		CsvReader csvReader=new CsvReader(file);
		boolean firstLineIsHeader=false;
		{
			String val=config.getValue(Config.SETTING_FIRSTLINE);
			if(Config.SETTING_FIRSTLINE_AS_HEADER.equals(val)) {
				firstLineIsHeader=true;
			}
		}
		char valueDelimiter=Config.SETTING_VALUEDELIMITER_DEFAULT;
		{
			String val=config.getValue(Config.SETTING_VALUEDELIMITER);
			if(val!=null) {
				valueDelimiter=val.charAt(0);
			}
		}
		char valueSeparator=Config.SETTING_VALUESEPARATOR_DEFAULT;
		{
			String val=config.getValue(Config.SETTING_VALUESEPARATOR);
			if(val!=null) {
				valueSeparator=val.charAt(0);
			}
		}
		/** optional: set database schema, if table is not in default schema.
		 */
		String definedSchemaName=config.getValue(Config.SETTING_DBSCHEMA);
		/** mandatory: set database table to insert data into.
		 */
		String definedTableName=config.getValue(Config.SETTING_DBTABLE);
		
		// validity of connection
		if(db==null) {
			throw new IoxException("connection==null");
		}else if(!(db.isValid(0))) {
			throw new IoxException("connection to: "+db+" failed");
		}
				
		// build csvReader
		csvReader.setFirstLineIsHeader(firstLineIsHeader);
		csvReader.setValueDelimiter(valueDelimiter);
		csvReader.setValueSeparator(valueSeparator);
		
		// read IoxEvents
		IoxEvent event=csvReader.read();
		while(event instanceof IoxEvent){
			if(event instanceof ObjectEvent) {
				IomObject iomObj=((ObjectEvent)event).getIomObject();
				
				// table validity
				ResultSet tableInDb=null;
				if(config.getValue(Config.SETTING_DBTABLE)!=null){
					// attribute names of database table
					try {
						tableInDb=openTableInDb(definedSchemaName, definedTableName, db);
					}catch(Exception e) {
						throw new IoxException("table "+definedTableName+" not found");
					}
				}else {
					throw new IoxException("expected tablename");
				}
				
				attrsPool.clear();
				notFoundAttrs.clear();
				// build attributes
				attrsPool.clear();
				ResultSetMetaData rsmd=tableInDb.getMetaData();
				for(int k=1;k<rsmd.getColumnCount()+1;k++) {
					String columnName=rsmd.getColumnName(k);
					int columnType=rsmd.getColumnType(k);
					String columnTypeName=rsmd.getColumnTypeName(k);
					
					for(int i=0;i<iomObj.getattrcount();i++) {
						if(columnName.equals(iomObj.getattrname(i))){
							String attrValue=iomObj.getattrvalue(iomObj.getattrname(i));
							if(attrValue==null) {
								attrValue=iomObj.getattrobj(iomObj.getattrname(i), 0).toString();
							}
							if(attrValue!=null) {
								PgAttributeObject attrData=new PgAttributeObject();
								attrData.setAttributeName(iomObj.getattrname(i));
								attrData.setAttributeType(columnType);
								attrData.setAttributeTypeName(columnTypeName);
								attrsPool.put(iomObj.getattrname(i), attrData);
							}
						}else {
							notFoundAttrs.add(iomObj.getattrname(i));
						}
					}
				}
				if(attrsPool.size()==0) {
					throw new IoxException("data base attribute names: "+notFoundAttrs.toString()+" not found in "+file.getName());
				}

				// insert attributes to database
				try {
					insertIntoTable(definedSchemaName, definedTableName, attrsPool, db,iomObj);
				} catch (ConverterException e) {
					throw new IoxException("import failed"+e);
				}
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
}