package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;
import ch.interlis.ioxwkf.shp.ShapeReader;

public class Shp2db extends AbstractImport2db {
	@Override
	protected IoxReader createReader(File file, Settings config) throws IoxException {
		/** mandatory: file to reader has not to be null.
		 */
		if(file!=null) {
			if(file.exists()) {
				EhiLogger.logState("file to read to: <"+file.getName()+">");
			}else {
				throw new IoxException("file "+file.getName()+" not found.");
			}
		}else {
			throw new IoxException("file==null.");
		}
		
		/** create and return a shape reader.
		 */
		return new ShapeReader(file,config);
	}
}