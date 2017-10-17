package ch.interlis.db2shp;

public class Config {
	private Config() {}
	// prefix
	public final static String PREFIX="ch.interlis.db2shp";
	// key
	public final static String DBSCHEMA=PREFIX+".dbschema";
	public final static String TABLE=PREFIX+".dbtable";
	public final static String SETTING_MODELNAMES=PREFIX+".settingModelNames";
	public final static String SETTING_ILIDIRS=PREFIX+".settingIliDirs";
	public final static String DELIMITER=PREFIX+".delimiter";
	public final static String RECORD_DELIMITER=PREFIX+".recordDelimiter";
	public final static String HEADER=PREFIX+".header";
	// default value
	public static final String APP_JAR="iox-wkf.jar";
	public final static String HEADERPRESENT="present";
	public final static String HEADERABSENT="absent";
	public final static String DEFAULT_DELIMITER="\"";
	public final static String DEFAULT_RECORD_DELIMITER=",";
	public final static String CSV_DIR="%CSV_DIR";
	public final static String JAR_DIR="%JAR_DIR";
	public final static String SETTING_DEFAULT_ILIDIRS=CSV_DIR+";http://models.interlis.ch/;"+JAR_DIR+"/ilimodels";
}
