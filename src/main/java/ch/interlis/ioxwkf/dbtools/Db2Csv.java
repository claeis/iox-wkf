package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.List;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iom_j.csv.CsvWriter;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;

/** writes data from a database to a CSV file.
 * Use {@link #setAttributes(String[])} to limit the export to some columns, otherwise all columns from the given table are exported.
 * {@link CsvReader#ENCODING}
 * {@link IoxWkfConfig#SETTING_VALUEDELIMITER}
 * {@link IoxWkfConfig#SETTING_VALUESEPARATOR}
 * {@link IoxWkfConfig#SETTING_FIRSTLINE}
 */
public class Db2Csv extends AbstractExportFromdb {
	private String attributes[]=null;
	
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

	public String[] getAttributes() {
		return attributes;
	}

	/** set attributes/column names to export.
	 * @param attributes[]
	 */
	public void setAttributes(String attributes[]) {
		this.attributes = attributes;
	}
}