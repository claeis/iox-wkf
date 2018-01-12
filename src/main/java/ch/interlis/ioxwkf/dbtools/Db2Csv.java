package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.List;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvWriter;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;

/**<b>Db2Csv</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * write data of data base with any IoxWriter, converted from PostGis to Interlis dataTypes and export converted data to CSV file.<br>
 * <p>
 * 
 * <b>Create a new Db2Csv</b><br>
 * <li>Create an Db2Csv object. Db2Csv extends AbstractExport2db class.</li>
 * <p>
 * 
 * <b>Db2Csv Settings</b><br>
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
 *	 Db2Csv csvExport= new Db2Csv();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, IoxWkfConfig.SETTING_VALUEDELIMITER_DEFAULT);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, ",");<br>
 *	 csvExport.exportData(new File("file"),"Connection", config);
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
 *	 Db2Csv csvExport= new Db2Csv();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, IoxWkfConfig.SETTING_VALUESEPARATOR_DEFAULT);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, ":");<br>
 *	 csvExport.exportData(new File("file"),"Connection", config);
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
 *	 Db2Csv csvExport= new Db2Csv();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_VALUE);<br>
 *	 csvExport.exportData(new File("file"),"Connection", config);
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
public class Db2Csv extends AbstractExportFromdb {
	private String attributes[]=null;
	
	/** validate the settings, create the CsvWriter and return created IoxWriter.
	 * <p>
	 * 
	 * Set File (Mandatory)<br>
	 * The file to write to.
	 * <p>
	 * 
	 * Path has to exist.<br>
	 * File file=new File("C:\file.csv");<br>
	 * Db2Csv csvExport= new Db2Csv();<br>
	 * csvExport.exportData(file,"Connection", config);
	 * <p>
	 * 
	 * Setting possibilities:<br>
	 * {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
	 * <p>
	 * 
	 * AttributeDescriptor possibilities<br>
	 * {@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}
	 * <p>
	 * 
	 * <table border="1">
	 *  <tr>
	 *   <td>SETTING_VALUEDELIMITER</td>
	 *   <td>CSV import/export<br>
	 * 		 The value delimiter.<p>
	 * 		 decide between:<br>
	 * 		 <li>default value: SETTING_VALUEDELIMITER_DEFAULT</li>
	 * 		 <li>user defined value delimiter</li></td>
	 *   <td>
	 *   Settings config=new Settings();<br>
	 *	 Db2Csv csvExport= new Db2Csv();<br>
	 *	 config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, IoxWkfConfig.SETTING_VALUEDELIMITER_DEFAULT);<br>
	 *	 config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, ",");<br>
	 *	 csvExport.exportData(new File("file"),"Connection", config);
	 *   </td>
	 *  </tr>
	 *  <tr>
	 *   <td>SETTING_VALUEDELIMITER_DEFAULT</td>
	 *   <td>CSV import/export<br>
	 *		 the default value delimiter value: quotation mark (").</td>
	 *   <td>See: SETTING_VALUEDELIMITER</td>
	 *  </tr>
	 *  <tr>
	 *   <td>SETTING_VALUESEPARATOR</td>
	 *   <td>CSV import/export<br>
	 * 		 The value separator.<p>
	 * 		 decide between:<br>
	 * 		 <li>default value: SETTING_VALUESEPARATOR_DEFAULT</li>
	 * 		 <li>user defined value separator</li></td>
	 *   <td>
	 *   Settings config=new Settings();<br>
	 *	 Db2Csv csvExport= new Db2Csv();<br>
	 *	 config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, IoxWkfConfig.SETTING_VALUESEPARATOR_DEFAULT);<br>
	 *	 config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, ":");<br>
	 *	 csvExport.exportData(new File("file"),"Connection", config);
	 *   </td>
	 *  </tr>
	 *  <tr>
	 *   <td>SETTING_VALUESEPARATOR_DEFAULT</td>
	 *   <td>CSV import/export<br>
	 *		 the default value separator.</td>
	 *   <td>See: SETTING_VALUESEPARATOR</td>
	 *  </tr>
	 *  <tr>
	 *   <td>SETTING_FIRSTLINE</td>
	 *   <td> CSV import/export<br>
	 *		the first line of CSV file.<p>
	 *		decide between:<br>
	 *		<li>SETTING_FIRSTLINE_AS_HEADER</li>
	 * 		<li>SETTING_FIRSTLINE_AS_VALUE</li></td>
	 *   <td>
	 *   Settings config=new Settings();<br>
	 *	 Db2Csv csvExport= new Db2Csv();<br>
	 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);<br>
	 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_VALUE);<br>
	 *	 csvExport.exportData(new File("file"),"Connection", config);
	 *   </td>
	 *  </tr>
	 * </table>
	 * <p>
	 * 
	 * @param file
	 * @param config defined settings
	 * @param dbColumns[]
	 * @exception IoxException
	 * @return IoxWriter
	 */
	@Override
	protected IoxWriter createWriter(File file, Settings config, AttributeDescriptor dbColumns[]) throws IoxException {
		// mandatory: file to reader has not to be null.
		if(file!=null) {
				EhiLogger.logState("file to write to: <"+file.getName()+">");
		}else {
			throw new IoxException("file==null.");
		}
		
		// create csv writer.
		CsvWriter writer=new CsvWriter(file,config);
		if(attributes!=null) {
			writer.setAttributes(attributes);
		}
		
		// optional char: value delimiter.
		String valueDelimiter=config.getValue(IoxWkfConfig.SETTING_VALUEDELIMITER);
		if(valueDelimiter!=null) {
			writer.setValueDelimiter(valueDelimiter.charAt(0));
			EhiLogger.traceState("valueDelimiter <"+valueDelimiter+">.");
		}
		
		// optional char: value separator.
		String valueSeparator=config.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR);
		if(valueSeparator!=null) {
			writer.setValueSeparator(valueSeparator.charAt(0));
			EhiLogger.traceState("valueSeparator <"+valueSeparator+">.");
		}
		
		// optional boolean: first line is set as header or as data.
		String firstLine=config.getValue(IoxWkfConfig.SETTING_FIRSTLINE);
		if(firstLine!=null) {
			boolean firstLineIsHeader=firstLine.equals(IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
			if(!firstLineIsHeader) {
				writer.setWriteHeader(firstLineIsHeader);
				EhiLogger.traceState("first line is "+(firstLineIsHeader?"header":"data"));
			}
		}
		return writer;
	}

	/** get attributes
	 * @return String[]
	 */
	public String[] getAttributes() {
		return attributes;
	}

	/** set attributes
	 * @param attributes[]
	 */
	public void setAttributes(String attributes[]) {
		this.attributes = attributes;
	}
}