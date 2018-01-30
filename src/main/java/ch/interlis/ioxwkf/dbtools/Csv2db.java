package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;

/** read data of a CSV file into a database.
 * {@link CsvReader#ENCODING}
 * {@link IoxWkfConfig#SETTING_VALUEDELIMITER}
 * {@link IoxWkfConfig#SETTING_VALUESEPARATOR}
 * {@link IoxWkfConfig#SETTING_FIRSTLINE}
 */
public class Csv2db extends AbstractImport2db {
	
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
		
		// create csv reader.
		CsvReader reader=new CsvReader(file,config);
		
		// optional char: value delimiter.
		String valueDelimiter=config.getValue(IoxWkfConfig.SETTING_VALUEDELIMITER);
		if(valueDelimiter!=null) {
			reader.setValueDelimiter(valueDelimiter.charAt(0));
			EhiLogger.traceState("valueDelimiter <"+valueDelimiter+">.");
		}
		
		// optional char: value separator.
		String valueSeparator=config.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR);
		if(valueSeparator!=null) {
			reader.setValueSeparator(valueSeparator.charAt(0));
			EhiLogger.traceState("valueSeparator <"+valueSeparator+">.");
		}
		
		// optional boolean: first line is set as header or as data.
		boolean firstLineIsHeader=false;
		if(config.getValue(IoxWkfConfig.SETTING_FIRSTLINE)!=null) {
			firstLineIsHeader=config.getValue(IoxWkfConfig.SETTING_FIRSTLINE).equals(IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
		}
		reader.setFirstLineIsHeader(firstLineIsHeader);
		EhiLogger.traceState("first line is "+(firstLineIsHeader?"header":"data"));
		
		return reader;
	}

	
	@Override
	protected List<AttributeDescriptor> assignIomAttr2DbColumn(IoxReader ioxReader, List<AttributeDescriptor> attrDescriptors,List<String> missingAttributes) {
	    List<AttributeDescriptor> ret=new ArrayList<AttributeDescriptor>();
		CsvReader reader=(CsvReader)ioxReader;
		HashMap<String,AttributeDescriptor> attrs=new HashMap<String,AttributeDescriptor>();
		for(AttributeDescriptor attrDesc:attrDescriptors) {
			attrs.put(attrDesc.getDbColumnName().toLowerCase(), attrDesc);
		}
		String [] csvAttrs=reader.getAttributes();
		for(String  csvAttr:csvAttrs) {
			AttributeDescriptor attrDesc=attrs.get(csvAttr.toLowerCase());
			if(attrDesc!=null) {
				attrDesc.setIomAttributeName(csvAttr);			
				ret.add(attrDesc);
			}else {
				missingAttributes.add(csvAttr);
			}
		}
		return ret;
	}
}