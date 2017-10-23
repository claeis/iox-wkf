package ch.interlis.configuration;

public class Config {
	private Config() {}
	public final static String IMPORT_PREFIX="ch.interlis.dbimport";
	public final static String DBSCHEMA=IMPORT_PREFIX+".dbschema";
	public final static String TABLE=IMPORT_PREFIX+".dbtable";
	public final static String SETTING_MODELNAMES=IMPORT_PREFIX+".settingModelNames";
	public final static String SETTING_ILIDIRS=IMPORT_PREFIX+".settingIliDirs";
	public final static String DELIMITER=IMPORT_PREFIX+".delimiter";
	public final static String RECORD_DELIMITER=IMPORT_PREFIX+".recordDelimiter";
	public final static String HEADER=IMPORT_PREFIX+".header";
	public final static String APP_JAR="iox-wkf.jar";
	public final static String HEADERPRESENT="present";
	public final static String HEADERABSENT="absent";
	public final static String DEFAULT_DELIMITER="\"";
	public final static String DEFAULT_RECORD_DELIMITER=",";
	public final static String FILE_DIR="%CSV_DIR";
	public final static String JAR_DIR="%JAR_DIR";
	public final static String SETTING_DEFAULT_ILIDIRS=FILE_DIR+";http://models.interlis.ch/;"+JAR_DIR+"/ilimodels";
}