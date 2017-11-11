package ch.interlis.ioxwkf.dbtools;

public class Config {
	private Config() {}
	public final static String APP_JAR="iox-wkf.jar";
	public final static String FILE_DIR="%CSV_DIR";
	public final static String JAR_DIR="%JAR_DIR";
	// import
	public final static String IMPORT_PREFIX="ch.interlis.dbimport";
	// schema
	public final static String SETTING_DBSCHEMA=IMPORT_PREFIX+".dbSchema";
	// table
	public final static String SETTING_DBTABLE=IMPORT_PREFIX+".dbTable";
	// models
	public final static String SETTING_MODELNAMES=IMPORT_PREFIX+".modelNames";
	public final static String SETTING_ILIDIRS=IMPORT_PREFIX+".settingIliDirs";
	public final static String SETTING_ILIDIRS_DEFAULT=FILE_DIR+";http://models.interlis.ch/;"+JAR_DIR+"/ilimodels";
	// firstline
	public final static String SETTING_FIRSTLINE=IMPORT_PREFIX+".firstLine";
	public final static String SETTING_FIRSTLINE_AS_HEADER="header";
	public final static String SETTING_FIRSTLINE_AS_VALUE="data";
	// quotationMark
	public final static String SETTING_VALUEDELIMITER=IMPORT_PREFIX+".valueDelimiter";
	public final static char SETTING_VALUEDELIMITER_DEFAULT='\"';
	// value delimiter
	public final static String SETTING_VALUESEPARATOR=IMPORT_PREFIX+".valueSeparator";
	public final static char SETTING_VALUESEPARATOR_DEFAULT=',';
	// epsg/srs code
	public final static String SETTING_SRSCODE=IMPORT_PREFIX+".settingSrsCode";
	public final static int SET_DEFAULT_SRSCODE=2056;
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