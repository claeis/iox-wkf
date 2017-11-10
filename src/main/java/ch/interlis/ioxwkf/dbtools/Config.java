package ch.interlis.ioxwkf.dbtools;

public class Config {
	private Config() {}
	public final static String APP_JAR="iox-wkf.jar";
	public final static String FILE_DIR="%CSV_DIR";
	public final static String JAR_DIR="%JAR_DIR";
	// import
	public final static String IMPORT_PREFIX="ch.interlis.dbimport";
	// schema
	public final static String SETTING_DBSCHEMA=IMPORT_PREFIX+".settingDBSchema";
	// table
	public final static String SETTING_DBTABLE=IMPORT_PREFIX+".settingDBTable";
	// models
	public final static String SETTING_MODELNAMES=IMPORT_PREFIX+".settingModelNames";
	public final static String SETTING_ILIDIRS=IMPORT_PREFIX+".settingIliDirs";
	public final static String SET_ILIDIRS_DEFAULT_VALUE=FILE_DIR+";http://models.interlis.ch/;"+JAR_DIR+"/ilimodels";
	// firstline
	public final static String SETTING_FIRSTLINE=IMPORT_PREFIX+".settingFirstline";
	public final static String SET_FIRSTLINE_AS_HEADER="present";
	public final static String SET_FIRSTLINE_AS_VALUE="absent";
	// quotationMark
	public final static String SETTING_QUOTATIONMARK=IMPORT_PREFIX+".settingQuotationMark";
	public final static String SET_QUOTATIONMARK="\"";
	// value delimiter
	public final static String SETTING_VALUEDELIMITER=IMPORT_PREFIX+".settingValueDelimiter";
	public final static String SET_DEFAULT_VALUEDELIMITER=",";
	// epsg/srs code
	public final static String SETTING_SRSCODE=IMPORT_PREFIX+".settingSrsCode";
	public final static Integer SET_DEFAULT_SRSCODE=2056;
	// geometry types
	public final static String SET_GEOMETRY="geometry";
	public final static String SET_GEOMETRY_POINT="POINT";
	public final static String SET_GEOMETRY_MULTIPOINT="MULTIPOINT";
	public final static String SET_GEOMETRY_LINESTRING="LINESTRING";
	public final static String SET_GEOMETRY_MULTILINESTRING="MULTILINESTRING";
	public final static String SET_GEOMETRY_POLYGON="POLYGON";
	public final static String SET_GEOMETRY_MULTIPOLYGON="MULTIPOLYGON";
	// uuid type
	public final static String SET_UUID="uuid";
	// xml type
	public final static String SET_XML="xml";
}