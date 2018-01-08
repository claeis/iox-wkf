package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;
import ch.interlis.ioxwkf.shp.ShapeReader;

/** set iom attribute names to attribute descriptor, create shapeReader and return the created shapeReader.
 */
public class Shp2db extends AbstractImport2db {
	/** create the ShpReader and return created ShpReader.
	 * @param file to read from
	 * @param config defined settings
	 * @return IoxReader
	 */
	@Override
	protected IoxReader createReader(File file, Settings config) throws IoxException {
		// mandatory: file to reader has not to be null.
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
	/** set iom attribute names to attribute descriptor
	 * @param ioxReader
	 * @param attrDescriptors
	 * @param missingAttributes
	 */
	@Override
	protected void setIomAttrNames(IoxReader ioxReader, List<AttributeDescriptor> attrDescriptors,List<String> missingAttributes) {
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
			}else {
				missingAttributes.add(shpAttr);
			}
		}
	}
}