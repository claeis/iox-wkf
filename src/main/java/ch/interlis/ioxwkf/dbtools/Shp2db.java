package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;
import ch.interlis.ioxwkf.shp.ShapeReader;

/**<b>Shp2db</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * read data of files with any IoxReader, converted from Interlis to PostGis dataTypes and import converted data to database.<br>
 * <p>
 * 
 * <b>Create a new Shp2db</b><br>
 * <li>Create an Shp2db object. Shp2db extends AbstractImport2db class.</li>
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
 * <li><a href="https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf">Shapespecification</a></li>
 * <li><a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
 */
public class Shp2db extends AbstractImport2db {
	/** Create the IoxReader.<br>
	 * There are 2 parameter to define:<br>
	 * <li>the file to read from.</li>
	 * <li>the config.</li>
	 * <p>
	 * 
	 * Set File (Mandatory)<br>
	 * The file to read from.
	 * <p>
	 * 
	 * File has to exist and has to be readable.<br>
	 * File file=new File("C:\file.shp");<br>
	 * Shp2db shpImport= new Shp2db();<br>
	 * shpImport.importData(file,"Connection", config);
	 * <p>
	 * 
	 * Setting possibilities:<br>
	 * <li>Setting possibilities<br>
	 *	   {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
	 * </li>
	 * @param file to read from
	 * @param config defined settings
	 * @return IoxReader
	 */
	@Override
	protected IoxReader createReader(Object obj, Settings config) throws IoxException {
		// mandatory: file to reader has not to be null.
	    File file = (File) obj;
		if(file!=null) {
			if(file.exists()) {
				EhiLogger.logState("file to read from: <"+file.getName()+">");
			}else {
				throw new IoxException("file "+file.getName()+" not found.");
			}
		}else {
			throw new IoxException("file==null.");
		}
		
		// create and return a shape reader.
		return new ShapeReader(file,config);
	}
	
	@Override
	protected  List<AttributeDescriptor> assignIomAttr2DbColumn(IoxReader ioxReader, List<AttributeDescriptor> attrDescriptors,List<String> missingAttributes) {
        List<AttributeDescriptor> ret=new ArrayList<AttributeDescriptor>();
		ShapeReader reader=(ShapeReader)ioxReader;
		HashMap<String,AttributeDescriptor> attrs=new HashMap<String,AttributeDescriptor>();
		AttributeDescriptor geomAttr=null;
		for(AttributeDescriptor attrDesc:attrDescriptors) {
			if(attrDesc.getDbColumnGeomTypeName()!=null) {
				geomAttr=attrDesc;
			}
			attrs.put(attrDesc.getDbColumnName().toLowerCase(), attrDesc);
		}
		String [] shpAttrs=reader.getAttributes();
		String shpGeomAttr=reader.getGeomAttr();
		for(String  shpAttr:shpAttrs) {
			AttributeDescriptor attrDesc=null;
			if(shpAttr.equals(shpGeomAttr)) {
				attrDesc=geomAttr;
			}else {
				attrDesc=attrs.get(shpAttr.toLowerCase());
			}
			if(attrDesc!=null) {
				attrDesc.setIomAttributeName(shpAttr);			
				ret.add(attrDesc);
			}else {
				missingAttributes.add(shpAttr);
			}
		}
		return ret;
	}
}