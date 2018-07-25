package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Types;
import java.util.ArrayList;

import com.vividsolutions.jts.geom.Point;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;
import ch.interlis.ioxwkf.gpkg.GeoPackageWriter;

/**<b>Db2Shp</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * write data of data base with any IoxWriter, converted from PostGIS to Interlis data types and export converted data to GPKG file.<br>
 * <p>
 * 
 * <b>Create a new Db2Gpkg</b><br>
 * <li>Create an Db2Gpkg object. Db2Gpkg extends AbstractExport2db class.</li>
 * <p>
 * 
 * <b>AttributeDescriptor possibilities</b><br>
 * {@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}<br>
 * <p>
 * 
 * <b>Setting possibilities</b><br>
 * {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}<br>
 * <p>
 * 
 * <b>Attachement</b><br>
 * <li><a href="http://www.geopackage.org/spec/">GeoPackage specification</a></li>
 * <li><a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
 */
public class Db2Gpkg extends AbstractExportFromdb {
	/** Create the GpkgWriter and return created IoxWriter.<br>
	 * <p>
	 * Set file (mandatory) and table name (mandatory)<br>
	 * <p>
	 * 
	 * Path has to exist.<br>
	 * Db2Gpkg gpkgExport= new Db2Gpkg();<br>
	 * gpkgExport.exportData("/path/to/file;tablename","Connection", config);
	 * <p>
	 * 
	 * Setting possibilities:<br>
	 * <li>Setting possibilities<br>
	 *	   {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
	 * </li>
	 * @param obj
	 * @param config
	 * @param dbColumns[]
	 * @exception IoxException
	 * @return IoxWriter
	 */

	@Override
	protected IoxWriter createWriter(Object obj, Settings config, AttributeDescriptor[] dbColumns) throws IoxException {
        File file = null;
        String tableName = null;
        if (obj != null) {
            String[] splits = ((String)obj).split(";");
            file = new File(splits[0]);
            tableName = splits[1];
            if (file!=null && tableName != null) {
            	EhiLogger.logState("file to write to: <"+file.getName()+">");
            	EhiLogger.logState("table name: <"+tableName+">");
            } else {
    			throw new IoxException("file or table name == null.");
    		}
        }  else {
            throw new IoxException("obj==null.");
        }
        
        // Create and return gpkg writer.
        GeoPackageWriter writer = new GeoPackageWriter(file, tableName, config);
        AttributeDescriptor attrDescs[] = new AttributeDescriptor[dbColumns.length];
    	System.out.println("ççççççççççççççççççççççççççç");

        for(int i=0;i<dbColumns.length;i++) {
        	String attrName=dbColumns[i].getIomAttributeName();
        	int dbColType = dbColumns[i].getDbColumnType();
        	
        	AttributeDescriptor attrDesc = new AttributeDescriptor();
        	attrDesc.setIomAttributeName(dbColumns[i].getIomAttributeName());
        	attrDesc.setDbColumnName(dbColumns[i].getDbColumnName().toLowerCase());

        	System.out.println("**** PostgreSQL");
        	System.out.println("getIomAttributeName: " + dbColumns[i].getIomAttributeName());
        	System.out.println("getDbColumnName: " + dbColumns[i].getDbColumnName());
        	System.out.println("getDbColumnTypeName: " + dbColumns[i].getDbColumnTypeName());
        	System.out.println("getDbColumnType: " + dbColumns[i].getDbColumnType());
        	System.out.println("getDbColumnGeomTypeName: " + dbColumns[i].getDbColumnGeomTypeName());
        	System.out.println("getCoordDimension: " + dbColumns[i].getCoordDimension());
        	System.out.println("getSrId: " +  dbColumns[i].getSrId());
        	System.out.println("****");
        	        	
        	if(dbColumns[i].isGeometry()) {
        		attrDesc.setCoordDimension(dbColumns[i].getCoordDimension());
        		attrDesc.setSrId(dbColumns[i].getSrId());

        		String geoColumnTypeName=dbColumns[i].getDbColumnGeomTypeName();
        		if (geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
					attrDesc.setDbColumnGeomTypeName("POINT");
				} else if (geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOINT)) {
					attrDesc.setDbColumnGeomTypeName("MULTIPOINT");
				} else if (geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
					attrDesc.setDbColumnGeomTypeName("LINESTRING");
				} else if (geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTILINESTRING)) {
					attrDesc.setDbColumnGeomTypeName("MULTILINESTRING");
				} else if (geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
					attrDesc.setDbColumnGeomTypeName("POLYGON");
				} else if (geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)) {
					attrDesc.setDbColumnGeomTypeName("MULTIPOLYGON");
				} else {
					throw new IllegalStateException("unexpected geometry type "+geoColumnTypeName);
				}
        	} else if (dbColType==Types.SMALLINT) {
        		attrDesc.setDbColumnTypeName("INTEGER");
        	} else if (dbColType==Types.TINYINT) {
        		attrDesc.setDbColumnTypeName("INTEGER");
        	} else if (dbColType==Types.INTEGER) {
        		attrDesc.setDbColumnTypeName("INTEGER");
        	} else if (dbColType==Types.BIGINT) {
        		attrDesc.setDbColumnTypeName("INTEGER");
        	} else if (dbColType==Types.NUMERIC) {
        		attrDesc.setDbColumnTypeName("REAL");
        	} else if (dbColType==Types.FLOAT) {
        		attrDesc.setDbColumnTypeName("REAL");
        	} else if (dbColType==Types.DOUBLE) {
        		attrDesc.setDbColumnTypeName("REAL");
        	} else if (dbColType==Types.DATE) {
        		attrDesc.setDbColumnTypeName("DATE");
        	} else if (dbColType==Types.TIMESTAMP) {
        		attrDesc.setDbColumnTypeName("DATETIME");
        	} else if (dbColType==Types.BOOLEAN || dbColumns[i].getDbColumnTypeName().equals(AttributeDescriptor.DBCOLUMN_TYPENAME_BOOL)) {
        		attrDesc.setDbColumnTypeName("BOOLEAN");
        	} else {
        		attrDesc.setDbColumnTypeName("TEXT");
        	}
        	attrDescs[i]= attrDesc;
        }
        writer.setAttributeDescriptors(attrDescs);
		return writer;
	}

}
