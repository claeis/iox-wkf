package ch.interlis.ioxwkf.dbtools;

/** settings used by this package.
 */
public class IoxWkfConfig {
	private IoxWkfConfig() {}
	/** the app jar of project iox-wkf.
	 */
	public final static String APP_JAR="iox-wkf.jar";
	/** placeholder, that will be replaced by the folder of the current to be validated csv file.
	 */
	public final static String FILE_DIR="%CSV_DIR";
	/** placeholder, that will be replaced by the folder of the iox-wkf program.
	 */
	public final static String JAR_DIR="%JAR_DIR";
	private final static String PREFIX="ch.interlis.ioxwkf.dbtools";
	// schema
	/** the data base schema name.
	 */
	public final static String SETTING_DBSCHEMA=PREFIX+".dbSchema";
	// table
	/** the data base table name.
	 */
	public final static String SETTING_DBTABLE=PREFIX+".dbTable";
	/** only the listed tables from the database should be exported to the interlis file.
	 * multiple tables are separated by semicolon (';').
	 */
	public final static String SETTING_INCLUDETABLES=PREFIX+".includeTables";
	/** the listed tables should not be exported from the database to the interlis file.
	 * multiple tables are separated by semicolon (';').
	 */
	public final static String SETTING_EXCLUDETABLES=PREFIX+".excludeTables";
	/** model names. Multiple model names are separated by semicolon (';'). 
	 */
	public final static String SETTING_MODELNAMES=PREFIX+".modelNames";
	/** Path with folders of Interlis model files.
	 * decide between:<br>
	 * <li>set default model directory: SETTING_ILIDIRS_DEFAULT.</li>
	 * <li>set user defined model directory.</li>
	 * <p>
	 * Multiple entries are separated by semicolon (';').
	 * Might contain "http:" URLs which should contain model repositories.
	 * Might include placeholders FILE_DIR or JAR_DIR.
	 * @see #FILE_DIR
	 * @see #JAR_DIR
	 */
	public final static String SETTING_ILIDIRS=PREFIX+".settingIliDirs";
	/** Default path with folders of Interlis model files.
	 * @see #SETTING_ILIDIRS
	 */
	public final static String SETTING_ILIDIRS_DEFAULT=FILE_DIR+";http://models.interlis.ch/;"+JAR_DIR+"/ilimodels";
	// firstline
	/** Indicates the purpose of the first line of the CSV file.
	 * <li>SETTING_FIRSTLINE_AS_HEADER</li>
	 * <li>SETTING_FIRSTLINE_AS_VALUE</li>
	 */
	public final static String SETTING_FIRSTLINE=PREFIX+".firstLine";
	/** The first line of the CSV file is a header.
	 */
	public final static String SETTING_FIRSTLINE_AS_HEADER="header";
	/** The first line of the CSV file is data.
	 */
	public final static String SETTING_FIRSTLINE_AS_VALUE="data";
	// quotationMark
	/** CSV import/export<br>
	 * the value delimiter.<p>
	 * decide between:<br>
	 * <li>default value: SETTING_VALUEDELIMITER_DEFAULT</li>
	 * <li>user defined value delimiter</li>
	 */
	public final static String SETTING_VALUEDELIMITER=PREFIX+".valueDelimiter";
	/** CSV import/export<br>
	 * the default value delimiter value: quotation mark (").
	 */
	public final static char SETTING_VALUEDELIMITER_DEFAULT='\"';
	// value delimiter
	/** CSV import/export<br>
	 * the value separator.<p>
	 * decide between:<br>
	 * <li>default value: SETTING_VALUESEPARATOR_DEFAULT</li>
	 * <li>user defined value separator</li>
	 */
	public final static String SETTING_VALUESEPARATOR=PREFIX+".valueSeparator";
	/** CSV import/export<br>
	 * the default value separator.
	 */
	public final static char SETTING_VALUESEPARATOR_DEFAULT=',';
	// epsg/srs code
	/** epsg/srs is a code that uniquely identifies the Spatial Referencing System (SRS) within the database.
	 */
	public final static String SETTING_SRSCODE=PREFIX+".settingSrsCode";
	/** an epsg/srs code.
	 */
	public final static int SETTING_SRSCODE_DEFAULT=2056;
	// dateFormat date,time,timeStamp
	/** the date format.<p>
	 * decide between:<br>
	 * <li>the default date format pattern: yyyy-MM-dd SETTING_DEFAULTFORMAT_DATE.</li>
	 * <li>user defined date format pattern.</li>
	 * <p>
	 * @see <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a>
	 */
	public final static String SETTING_DATEFORMAT=PREFIX+".dateFormat";
	/** the time format.<p>
	 * decide between:<br>
	 * <li>the default time format pattern: HH:mm:ss SETTING_DEFAULTFORMAT_TIME.</li>
	 * <li>user defined time format pattern.</li>
	 *  <p>
	 * @see <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a>
	 */
	public final static String SETTING_TIMEFORMAT=PREFIX+".timeFormat";
	/** the time stamp format.<p>
	 * decide between:<br>
	 * <li>the default time stamp format pattern: yyyy-MM-dd'T'HH:mm:ss.SSS SETTING_DEFAULTFORMAT_TIMESTAMP.</li>
	 * <li>user defined time stamp format pattern.</li>
	 * <p>
	 * @see <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a>
	 */
	public final static String SETTING_TIMESTAMPFORMAT=PREFIX+".timeStampFormat";
	// dateFormat date,time,timeStamp default patterns
	/** the default date format pattern.
	 */
	public final static String SETTING_DEFAULTFORMAT_DATE="yyyy-MM-dd";
	/** the default time format pattern.
	 */
	public final static String SETTING_DEFAULTFORMAT_TIME="HH:mm:ss";
	/** the default time stamp format pattern.
	 */
	public final static String SETTING_DEFAULTFORMAT_TIMESTAMP="yyyy-MM-dd'T'HH:mm:ss.SSS";
}