package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;

public class Csv2db extends AbstractImport2db {
	@Override
	protected IoxReader createReader(File file, Settings config) throws IoxException {
		/** mandatory: file to read has not to be null.
		 */
		if(file!=null) {
			EhiLogger.logState("file to write to: <"+file.getAbsolutePath()+">");
		}else {
			throw new IoxException("file==null.");
		}
		
		/** create csv reader.
		 */
		CsvReader reader=new CsvReader(file);
		
		/** optional char: value delimiter.
		 */
		String valueDelimiter=config.getValue(IoxWkfConfig.SETTING_VALUEDELIMITER);
		if(valueDelimiter!=null) {
			reader.setValueDelimiter(valueDelimiter.charAt(0));
			EhiLogger.traceState("valueDelimiter <"+valueDelimiter+">.");
		}
		
		/** optional char: value separator.
		 */
		String valueSeparator=config.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR);
		if(valueSeparator!=null) {
			reader.setValueSeparator(valueSeparator.charAt(0));
			EhiLogger.traceState("valueSeparator <"+valueSeparator+">.");
		}
		
		/** optional boolean: first line is set as header or as data.
		 */
		boolean firstLineIsHeader=false;
		if(config.getValue(IoxWkfConfig.SETTING_FIRSTLINE)!=null) {
			firstLineIsHeader=config.getValue(IoxWkfConfig.SETTING_FIRSTLINE).equals(IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
		}
		reader.setFirstLineIsHeader(firstLineIsHeader);
		EhiLogger.traceState("first line is "+(firstLineIsHeader?"header":"data"));
		
		return reader;
	}
}