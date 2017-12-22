package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;
import ch.interlis.ioxwkf.shp.ShapeWriter;

public class Db2Shp extends AbstractExportFromdb {
	@Override
	protected IoxWriter createWriter(File file, Settings config) throws IoxException {
		/** mandatory: file to reader has not to be null.
		 */
		if(file!=null) {
			EhiLogger.logState("file to write to: <"+file.getName()+">");
		}else {
			throw new IoxException("file==null.");
		}
		
		/** create and return a shape writer.
		 */
		return new ShapeWriter(file,config);
	}
}