package ch.interlis.ioxwkf.dbtools;

/** IoxWkfConfig contains configuration possibilities inside DB Tools.
 */
public class IoxWkfConfig {
	private IoxWkfConfig() {}
	/** the application jar of project iox-wkf.
	 */
	public final static String APP_JAR="iox-wkf.jar";
	/** defines a file directory of CSV file.
	 */
	public final static String FILE_DIR="%CSV_DIR";
	/** defines a file directory of JAR file.
	 */
	public final static String JAR_DIR="%JAR_DIR";
	// import
	/** set the data base import prefix.
	 */
	public final static String IMPORT_PREFIX="ch.interlis.dbimport";
	// schema
	/** set data base schema.<p>
	 * decide between:<br>
	 * <li>set dbschema and import data into dbtable inside defined dbschema.</li>
	 * <li>set no dbschema and import data into default dbschema.</li>
	 */
	public final static String SETTING_DBSCHEMA=IMPORT_PREFIX+".dbSchema";
	// table
	/** set data base table.<br>
	 * import/export data to this table inside defined or default dbschema.
	 */
	public final static String SETTING_DBTABLE=IMPORT_PREFIX+".dbTable";
	// models
	/** set model name/names.
	 */
	public final static String SETTING_MODELNAMES=IMPORT_PREFIX+".modelNames";
	/** set directory of model files.
	 * decide between:<br>
	 * <li>set default model directory: SETTING_ILIDIRS_DEFAULT.</li>
	 * <li>set user defined model directory.</li>
	 */
	public final static String SETTING_ILIDIRS=IMPORT_PREFIX+".settingIliDirs";
	/** set default model directory.
	 */
	public final static String SETTING_ILIDIRS_DEFAULT=FILE_DIR+";http://models.interlis.ch/;"+JAR_DIR+"/ilimodels";
	// firstline
	/** CSV import/export<br>
	 * set the first line of CSV file.<p>
	 * decide between:<br>
	 * <li>SETTING_FIRSTLINE_AS_HEADER</li>
	 * <li>SETTING_FIRSTLINE_AS_VALUE</li>
	 */
	public final static String SETTING_FIRSTLINE=IMPORT_PREFIX+".firstLine";
	/** CSV import/export<br>
	 * set a header in first line of CSV file.
	 */
	public final static String SETTING_FIRSTLINE_AS_HEADER="header";
	/** CSV import/export<br>
	 * CSV file only contain values.
	 */
	public final static String SETTING_FIRSTLINE_AS_VALUE="data";
	// quotationMark
	/** CSV import/export<br>
	 * set the value delimiter.<p>
	 * decide between:<br>
	 * <li>default value: SETTING_VALUEDELIMITER_DEFAULT</li>
	 * <li>user defined value delimiter</li>
	 */
	public final static String SETTING_VALUEDELIMITER=IMPORT_PREFIX+".valueDelimiter";
	/** CSV import/export<br>
	 * set default value: quotation mark (").
	 */
	public final static char SETTING_VALUEDELIMITER_DEFAULT='\"';
	// value delimiter
	/** CSV import/export<br>
	 * set the value separator.<p>
	 * decide between:<br>
	 * <li>default value: SETTING_VALUESEPARATOR_DEFAULT</li>
	 * <li>user defined value separator</li>
	 */
	public final static String SETTING_VALUESEPARATOR=IMPORT_PREFIX+".valueSeparator";
	/** CSV import/export<br>
	 * set default value: comma (,).
	 */
	public final static char SETTING_VALUESEPARATOR_DEFAULT=',';
	// epsg/srs code
	/** set the epsg/srs.<br>
	 * epsg/srs is a code that uniquely identifies the Spatial Referencing System (SRS) within the database.<p>
	 * decide between:<br>
	 * <li>the default epsg/srs code: 2056 --> SETTING_SRSCODE_DEFAULT.</li>
	 * <li>user defined epsg/srs code.</li>
	 */
	public final static String SETTING_SRSCODE=IMPORT_PREFIX+".settingSrsCode";
	/** set the default epsg/srs code.
	 */
	public final static int SETTING_SRSCODE_DEFAULT=2056;
	// dateFormat date,time,timeStamp
	/** set the date format.<p>
	 * decide between:<br>
	 * <li>the default date format pattern: yyyy-MM-dd --> SETTING_DEFAULTFORMAT_DATE.</li>
	 * <li>user defined date format pattern.</li>
	 */
	public final static String SETTING_DATEFORMAT=IMPORT_PREFIX+".dateFormat";
	/** set the time format.<p>
	 * decide between:<br>
	 * <li>the default time format pattern: HH:mm:ss --> SETTING_DEFAULTFORMAT_TIME.</li>
	 * <li>user defined time format pattern.</li>
	 */
	public final static String SETTING_TIMEFORMAT=IMPORT_PREFIX+".timeFormat";
	/** set the time stamp format.<p>
	 * decide between:<br>
	 * <li>the default time stamp format pattern: yyyy-MM-dd'T'HH:mm:ss.SSS --> SETTING_DEFAULTFORMAT_TIMESTAMP.</li>
	 * <li>user defined time stamp format pattern.</li>
	 */
	public final static String SETTING_TIMESTAMPFORMAT=IMPORT_PREFIX+".timeStampFormat";
	// dateFormat date,time,timeStamp default patterns
	/** set the default date format pattern.
	 */
	public final static String SETTING_DEFAULTFORMAT_DATE="yyyy-MM-dd";
	/** set the default time format pattern.
	 */
	public final static String SETTING_DEFAULTFORMAT_TIME="HH:mm:ss";
	/** set the default time stamp format pattern.
	 */
	public final static String SETTING_DEFAULTFORMAT_TIMESTAMP="yyyy-MM-dd'T'HH:mm:ss.SSS";
}