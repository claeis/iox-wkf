package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.io.IOException;
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
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.ObjectEvent;
import ch.interlis.ioxwkf.shp.ShapeReader;

public class Shp2db extends AbstractImport2db {
	
	/** import shpData to database.
	 * @param file
	 * @param db
	 * @param config
	 * @throws SQLException
	 * @throws IoxException
	 * @throws IOException 
	 */
	@Override
	public void importData(File file,Connection db,Settings config) throws SQLException, IoxException {
		Map<String, AttributeDescriptor> attrsPool=new HashMap<String, AttributeDescriptor>();
		ShapeReader shpReader;
		Set notFoundAttrs=new HashSet();
		
		if(!(file.exists())) {
			throw new IoxException("shp file: "+file.getAbsolutePath()+" not found");
		}else if(!(file.canRead())) {
			throw new IoxException("shp file: "+file.getAbsolutePath()+" not readable");
		}else {
			EhiLogger.logState("dataFile <"+file.getAbsolutePath()+">");
		}
		
		try {
			shpReader = new ShapeReader(file);
		} catch (IOException e) {
			throw new IoxException(e);
		}
		
		/** optional: set database schema, if table is not in default schema.
		 */
		String definedSchemaName=config.getValue(IoxWkfConfig.SETTING_DBSCHEMA);
		/** mandatory: set database table to insert data into.
		 */
		String definedTableName=config.getValue(IoxWkfConfig.SETTING_DBTABLE);
		
		// validity of connection
		if(db==null) {
			throw new IoxException("connection==null");
		}else if(!(db.isValid(0))) {
			throw new IoxException("connection to: "+db+" failed");
		}
		
		// read IoxEvents
		IoxEvent event=shpReader.read();
		while(event instanceof IoxEvent){
			if(event instanceof ObjectEvent) {
				attrsPool.clear();
				IomObject iomObj=((ObjectEvent)event).getIomObject();
				
				// table validity
				ResultSet tableInDb=null;
				if(config.getValue(IoxWkfConfig.SETTING_DBTABLE)!=null){
					// get data of geometry inside table-columns.
					try {
						tableInDb=openTableInDb(definedSchemaName, definedTableName, db);
					}catch(Exception e) {
						throw new IoxException("table "+definedTableName+" not found");
					}
				}else {
					throw new IoxException("expected tablename");
				}
				// build attributes				
				ResultSetMetaData rsmd=tableInDb.getMetaData();
				
				notFoundAttrs.clear();
				
				for(int k=1;k<rsmd.getColumnCount()+1;k++) {
					String columnName=rsmd.getColumnName(k);
					int columnType=rsmd.getColumnType(k);
					String columnTypeName=rsmd.getColumnTypeName(k);
					for(int i=0;i<iomObj.getattrcount();i++) {
						AttributeDescriptor attrData=null;
						if(columnName.equals(iomObj.getattrname(i))){
							String attrValue=iomObj.getattrvalue(iomObj.getattrname(i));
							if(attrValue==null) {
								attrValue=iomObj.getattrobj(iomObj.getattrname(i), 0).toString();
							}
							if(attrValue!=null) {
								attrData=new AttributeDescriptor();
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
					throw new IoxException("data base attribute names: "+notFoundAttrs.toString()+" not found in "+file.getAbsolutePath());
				}
				// insert attributes to database
				try {
					insertIntoTable(definedSchemaName, definedTableName, attrsPool, db, iomObj);
				} catch (Exception e) {
					throw new IoxException(e);
				}
				event=shpReader.read();
			}else {
				// next IoxEvent
				event=shpReader.read();
			}
		}
		// close shpReader
		if(shpReader!=null) {
			shpReader.close();
			shpReader=null;
		}
		event=null;
	}
}