package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;

/**<b>Csv2db</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * read data of files with any IoxReader, converted from Interlis to PostGis dataTypes and import converted data to database.<br>
 * <p>
 * 
 * <b>Create a new Csv2db</b><br>
 * <li>Create an Csv2db object. Csv2db extends AbstractImport2db class.</li>
 * <p>
 * 
 * <b>Csv2db Settings</b><br>
 * <table border="1">
 * <tr>
 *   <th>Setting Name</th>
 *   <th>Description</th>
 *   <th>Example</th>
 * </tr>
 * <tr>
 *   <td>SETTING_VALUEDELIMITER</td>
 *   <td>CSV import/export<br>
 * 		 The value delimiter.<p>
 * 		 decide between:<br>
 * 		 <li>default value: SETTING_VALUEDELIMITER_DEFAULT</li>
 * 		 <li>user defined value delimiter</li></td>
 *   <td>
 *   Settings config=new Settings();<br>
 *	 Csv2db csvImport= new Csv2db();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, IoxWkfConfig.SETTING_VALUEDELIMITER_DEFAULT);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, ",");<br>
 *	 csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_VALUEDELIMITER_DEFAULT</td>
 *   <td>CSV import/export<br>
 *		 the default value delimiter value: quotation mark (").</td>
 *   <td>See: SETTING_VALUEDELIMITER</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_VALUESEPARATOR</td>
 *   <td>CSV import/export<br>
 * 		 The value separator.<p>
 * 		 decide between:<br>
 * 		 <li>default value: SETTING_VALUESEPARATOR_DEFAULT</li>
 * 		 <li>user defined value separator</li></td>
 *   <td>
 *   Settings config=new Settings();<br>
 *	 Csv2db csvImport= new Csv2db();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, IoxWkfConfig.SETTING_VALUESEPARATOR_DEFAULT);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, ":");<br>
 *	 csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_VALUESEPARATOR_DEFAULT</td>
 *   <td>CSV import/export<br>
 *		 the default value separator.</td>
 *   <td>See: SETTING_VALUESEPARATOR</td>
 * </tr>
 * <tr>
 *   <td>SETTING_FIRSTLINE</td>
 *   <td> CSV import/export<br>
 *		the first line of CSV file.<p>
 *		decide between:<br>
 *		<li>SETTING_FIRSTLINE_AS_HEADER</li>
 * 		<li>SETTING_FIRSTLINE_AS_VALUE</li></td>
 *   <td>
 *   Settings config=new Settings();<br>
 *	 Csv2db csvImport= new Csv2db();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_VALUE);<br>
 *	 csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 * </table>
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
public class Csv2db extends AbstractImport2db {
	
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
	 * File file=new File("C:\file.csv");<br>
	 * Csv2db csvImport= new Csv2db();<br>
	 * csvImport.importData(file,"Connection", config);
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

	
	/** setIomAttrNames<br>
	 * set attribute names to attribute descriptor.<br>
	 * <p>
	 * 
	 * IoxReader:<br>
	 * {@link ch.interlis.ioxwkf.dbtools.AbstractImport2db#createReader()}
	 * <p>
	 * 
	 * AttributeDescriptor possibilities<br>
	 * {@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}<br>
	 * <p>
	 * 
	 * Missing Attributes:<br>
	 * Set iom attribute names to attribute descriptor.
	 * 
	 * @param ioxReader
	 * @param attrDescriptors
	 * @param missingAttributes
	 */
	@Override
	protected void setIomAttrNames(IoxReader ioxReader, List<AttributeDescriptor> attrDescriptors,List<String> missingAttributes) {
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
			}else {
				missingAttributes.add(csvAttr);
			}
		}
	}
}