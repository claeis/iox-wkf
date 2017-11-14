package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvWriter;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;

public class Db2Csv extends AbstractExportFromdb {
	@Override
	protected IoxWriter createWriter(File file, Settings config) throws IoxException {
		/** mandatory: file to write to has not to be null.
		 */
		if(file!=null) {
			EhiLogger.logState("file to write to: <"+file.getAbsolutePath()+">");
		}else {
			throw new IoxException("file==null.");
		}
		
		/** create csv writer.
		 */
		CsvWriter writer=new CsvWriter(file);
		
		/** optional char: value delimiter.
		 */
		String valueDelimiter=config.getValue(IoxWkfConfig.SETTING_VALUEDELIMITER);
		if(valueDelimiter!=null) {
			writer.setValueDelimiter(valueDelimiter.charAt(0));
			EhiLogger.traceState("valueDelimiter <"+valueDelimiter+">.");
		}
		
		/** optional char: value separator.
		 */
		String valueSeparator=config.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR);
		if(valueSeparator!=null) {
			writer.setValueSeparator(valueSeparator.charAt(0));
			EhiLogger.traceState("valueSeparator <"+valueSeparator+">.");
		}
		
		/** optional boolean: first line is set as header or as data.
		 */
		boolean firstLineIsHeader=config.getValue(IoxWkfConfig.SETTING_FIRSTLINE).equals(IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
		writer.setWriteHeader(firstLineIsHeader);
		EhiLogger.traceState("first line is "+(firstLineIsHeader?"header":"data"));
		
		return writer;
	}
}