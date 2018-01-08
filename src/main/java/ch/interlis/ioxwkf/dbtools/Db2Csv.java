package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.List;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvWriter;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;

/** create a CSV writer.
 */
public class Db2Csv extends AbstractExportFromdb {
	private String attributes[]=null;
	
	/** validate the settings, create the CsvWriter and return created IoxWriter.
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