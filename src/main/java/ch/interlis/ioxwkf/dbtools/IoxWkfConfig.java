package ch.interlis.ioxwkf.dbtools;

/**<b>IoxWkfConfig</b>
 * <p>
 * <b>The main task</b><br>
 * <br>
 * IoxWkfConfig contains configuration possibilities inside DB Tools.
 * <p>
 * 
 * <b>get data of IoxWkfConfig:</b><br>
 * <li>IoxWkfConfig.SETTING_ILIDIRS_DEFAULT</li>
 * <p>
 * 
 * <b>(Optional) Setting possibilities</b><br>
 * <table border="1">
 * <tr>
 *   <th>Setting Name</th>
 *   <th>Description</th>
 *   <th>Example</th>
 * </tr>
 * <tr>
 *   <td>APP_JAR</td>
 *   <td>The app jar of project iox-wkf.</td>
 *   <td>See: SETTING_ILIDIRS</td>
 * </tr>
 * <tr>
 *   <td>FILE_DIR</td>
 *   <td>Placeholder, that will be replaced by the folder of the current,
 *   to be validated, csv file.</td>
 *   <td>See: SETTING_ILIDIRS</td>
 * </tr>
 *  <tr>
 *   <td>JAR_DIR</td>
 *   <td>Placeholder, that will be replaced by the folder of the iox-wkf program.</td>
 *   <td>See: SETTING_ILIDIRS</td>
 * </tr>
 *  <tr>
 *   <td>IMPORT_PREFIX</td>
 *   <td>The data base import prefix.</td>
 *   <td>-</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DBSCHEMA</td>
 *   <td>The data base schema.<p>
 * 		Decide between:<br>
 * 		<li>Set dbschema and import data into dbtable inside defined dbschema.</li>
 * 		<li>Set no dbschema and import data into default dbschema.</li></td>
 *   <td>Settings config=new Settings();<br>
 *		 config.setValue(IoxWkfConfig.SETTING_DBSCHEMA,"schemaName");<br>
 *		 Csv2db csvImport= new Csv2db();<br>
 *		 csvImport.importData(new File("file"),"Connection", config);
 *	 </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DBTABLE (Mandatory)</td>
 *   <td>The data base table.<br>
 *   	 Import/Export data to this table inside defined or default dbschema.</td>
 *   <td>Settings config=new Settings();<br>
 *		 config.setValue(IoxWkfConfig.SETTING_DBTABLE,"tableName");<br>
 *		 Csv2db csvImport= new Csv2db();<br>
 *		 csvImport.importData(new File("file"),"Connection", config);
 *	 </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_MODELNAMES</td>
 *   <td>model names. Multiple model names are separated by semicolon (';'). </td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *		Csv2db csvImport= new Csv2db();<br>
 *		config.setValue(IoxWkfConfig.SETTING_MODELNAMES, "model1;model2;model3");
 *		csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_ILIDIRS</td>
 *   <td>Path with folders of Interlis model files.
 * 		decide between:<br>
 * 		<li>set default model directory: SETTING_ILIDIRS_DEFAULT.</li>
 *		<li>set user defined model directory.</li>
 * 		<p>
 * 		Multiple entries are separated by semicolon (';').
 * 		Might contain "http:" URLs which should contain model repositories.
 * 		Might include placeholders FILE_DIR or JAR_DIR.
 * 		<p>
 * 		See: FILE_DIR<br>
 * 		See: JAR_DIR</td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *		Csv2db csvImport= new Csv2db();<br>
 *   	config.setValue(IoxWkfConfig.SETTING_ILIDIRS, "http://models.interlis.ch/;"+JAR_DIR+"/ilimodels;"+FILE_DIR+"/ilimodels");<br>
 *   	config.setValue(IoxWkfConfig.SETTING_ILIDIRS, IoxWkfConfig.SETTING_ILIDIRS_DEFAULT);<br>
 *   	csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_ILIDIRS_DEFAULT</td>
 *   <td> Default path with folders of Interlis model files.</td>
 *   <td>See: SETTING_ILIDIRS</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_FIRSTLINE</td>
 *   <td> CSV import/export<br>
 *		the first line of CSV file.<p>
 *		decide between:<br>
 *		<li>SETTING_FIRSTLINE_AS_HEADER</li>
 * 		<li>SETTING_FIRSTLINE_AS_VALUE</li></td>
 *   <td>
 *   Settings config=new Settings();<br>
 *	 Csv2db csvImport= new Csv2db();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_VALUE);<br>
 *	 csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_FIRSTLINE_AS_HEADER</td>
 *   <td>CSV import/export<br>
 * 		 the first line of CSV file is the header.</td>
 *   <td>See: SETTING_FIRSTLINE</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_FIRSTLINE_AS_VALUE</td>
 *   <td>CSV import/export<br>
 * 		 CSV file only contain values, no header.</td>
 *   <td>See: SETTING_FIRSTLINE</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_VALUEDELIMITER</td>
 *   <td>CSV import/export<br>
 * 		 The value delimiter.<p>
 * 		 decide between:<br>
 * 		 <li>default value: SETTING_VALUEDELIMITER_DEFAULT</li>
 * 		 <li>user defined value delimiter</li></td>
 *   <td>
 *   Settings config=new Settings();<br>
 *	 Csv2db csvImport= new Csv2db();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, IoxWkfConfig.SETTING_VALUEDELIMITER_DEFAULT);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, ",");<br>
 *	 csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_VALUEDELIMITER_DEFAULT</td>
 *   <td>CSV import/export<br>
 *		 the default value delimiter value: quotation mark (").</td>
 *   <td>See: SETTING_VALUEDELIMITER</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_VALUESEPARATOR</td>
 *   <td>CSV import/export<br>
 * 		 The value separator.<p>
 * 		 decide between:<br>
 * 		 <li>default value: SETTING_VALUESEPARATOR_DEFAULT</li>
 * 		 <li>user defined value separator</li></td>
 *   <td>
 *   Settings config=new Settings();<br>
 *	 Csv2db csvImport= new Csv2db();<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, IoxWkfConfig.SETTING_VALUESEPARATOR_DEFAULT);<br>
 *	 config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, ":");<br>
 *	 csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_VALUESEPARATOR_DEFAULT</td>
 *   <td>CSV import/export<br>
 *		 the default value separator.</td>
 *   <td>See: SETTING_VALUESEPARATOR</td>
 * </tr>
 * <tr>
 *   <td>SETTING_SRSCODE</td>
 *   <td>epsg/srs is a code that uniquely identifies the
 *       Spatial Referencing System (SRS) within the database.<p></td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *	 	Csv2db csvImport= new Csv2db();<br>
 *	 	config.setValue(IoxWkfConfig.SETTING_SRSCODE, "2056");<br>
 *	 	csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DATEFORMAT</td>
 *   <td>The date format.<p>
 * 		 decide between:<br>
 * 		 <li>the default date format pattern: yyyy-MM-dd SETTING_DEFAULTFORMAT_DATE.</li>
 * 		 <li>user defined date format pattern.</li>
 * 		 <p>
 * 		 See: <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a></td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *	 	Csv2db csvImport= new Csv2db();<br>
 *	 	config.setValue(IoxWkfConfig.SETTING_DATEFORMAT, IoxWkfConfig.SETTING_DEFAULTFORMAT_DATE);<br>
 *		config.setValue(IoxWkfConfig.SETTING_DATEFORMAT, "yyyy-MM-dd");<br>
 *	 	csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_TIMEFORMAT</td>
 *   <td>The time format.<p>
 * 		 decide between:<br>
 * 		 <li>the default time format pattern: HH:mm:ss SETTING_DEFAULTFORMAT_TIME.</li>
 * 		 <li>user defined time format pattern.</li>
 *  	 <p>
 * 		 See: <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a></td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *	 	Csv2db csvImport= new Csv2db();<br>
 *	 	config.setValue(IoxWkfConfig.SETTING_TIMEFORMAT, IoxWkfConfig.SETTING_DEFAULTFORMAT_TIME);<br>
 *		config.setValue(IoxWkfConfig.SETTING_TIMEFORMAT, "HH:mm:ss");<br>
 *	 	csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_TIMESTAMPFORMAT</td>
 *   <td>The time stamp format.<p>
 *		 decide between:<br>
 *		 <li>the default time stamp format pattern: yyyy-MM-dd'T'HH:mm:ss.SSS SETTING_DEFAULTFORMAT_TIMESTAMP.</li>
 *		 <li>user defined time stamp format pattern.</li>
 *		 <p>
 *		 See: <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a></td>
 *   <td>
 *   	Settings config=new Settings();<br>
 *	 	Csv2db csvImport= new Csv2db();<br>
 *	 	config.setValue(IoxWkfConfig.SETTING_TIMESTAMPFORMAT, IoxWkfConfig.SETTING_DEFAULTFORMAT_TIMESTAMP);<br>
 *		config.setValue(IoxWkfConfig.SETTING_TIMESTAMPFORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSS");<br>
 *	 	csvImport.importData(new File("file"),"Connection", config);
 *   </td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DEFAULTFORMAT_DATE</td>
 *   <td>the default date format pattern.</td>
 *   <td>See: SETTING_DATEFORMAT</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DEFAULTFORMAT_TIME</td>
 *   <td>the default time format pattern.</td>
 *   <td>See: SETTING_TIMEFORMAT</td>
 * </tr>
 *  <tr>
 *   <td>SETTING_DEFAULTFORMAT_TIMESTAMP</td>
 *   <td>the default time stamp format pattern.</td>
 *   <td>See: SETTING_TIMESTAMPFORMAT</td>
 * </tr>
 * </table>
 * <p>
 * 
 * <b>Attachement</b><br>
 * <li><a href="https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf">Shapespecification</a></li>
 * <li><a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
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
	// import
	/** the data base import prefix.
	 */
	public final static String IMPORT_PREFIX="ch.interlis.dbimport";
	// schema
	/** the data base schema.<p>
	 * decide between:<br>
	 * <li>set dbschema and import data into dbtable inside defined dbschema.</li>
	 * <li>set no dbschema and import data into default dbschema.</li>
	 */
	public final static String SETTING_DBSCHEMA=IMPORT_PREFIX+".dbSchema";
	// table
	/** the data base table. (Mandatory)<br>
	 * import/export data to this table inside defined or default dbschema.
	 */
	public final static String SETTING_DBTABLE=IMPORT_PREFIX+".dbTable";
	// models
	/** model names. Multiple model names are separated by semicolon (';'). 
	 */
	public final static String SETTING_MODELNAMES=IMPORT_PREFIX+".modelNames";
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
	public final static String SETTING_ILIDIRS=IMPORT_PREFIX+".settingIliDirs";
	/** Default path with folders of Interlis model files.
	 * @see #SETTING_ILIDIRS
	 */
	public final static String SETTING_ILIDIRS_DEFAULT=FILE_DIR+";http://models.interlis.ch/;"+JAR_DIR+"/ilimodels";
	// firstline
	/** CSV import/export<br>
	 * the first line of CSV file.<p>
	 * decide between:<br>
	 * <li>SETTING_FIRSTLINE_AS_HEADER</li>
	 * <li>SETTING_FIRSTLINE_AS_VALUE</li>
	 */
	public final static String SETTING_FIRSTLINE=IMPORT_PREFIX+".firstLine";
	/** CSV import/export<br>
	 * the first line of CSV file is the header.
	 */
	public final static String SETTING_FIRSTLINE_AS_HEADER="header";
	/** CSV import/export<br>
	 * CSV file only contain values, no header.
	 */
	public final static String SETTING_FIRSTLINE_AS_VALUE="data";
	// quotationMark
	/** CSV import/export<br>
	 * the value delimiter.<p>
	 * decide between:<br>
	 * <li>default value: SETTING_VALUEDELIMITER_DEFAULT</li>
	 * <li>user defined value delimiter</li>
	 */
	public final static String SETTING_VALUEDELIMITER=IMPORT_PREFIX+".valueDelimiter";
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
	public final static String SETTING_VALUESEPARATOR=IMPORT_PREFIX+".valueSeparator";
	/** CSV import/export<br>
	 * the default value separator.
	 */
	public final static char SETTING_VALUESEPARATOR_DEFAULT=',';
	// epsg/srs code
	/** epsg/srs is a code that uniquely identifies the Spatial Referencing System (SRS) within the database.
	 */
	public final static String SETTING_SRSCODE=IMPORT_PREFIX+".settingSrsCode";
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
	public final static String SETTING_DATEFORMAT=IMPORT_PREFIX+".dateFormat";
	/** the time format.<p>
	 * decide between:<br>
	 * <li>the default time format pattern: HH:mm:ss SETTING_DEFAULTFORMAT_TIME.</li>
	 * <li>user defined time format pattern.</li>
	 *  <p>
	 * @see <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a>
	 */
	public final static String SETTING_TIMEFORMAT=IMPORT_PREFIX+".timeFormat";
	/** the time stamp format.<p>
	 * decide between:<br>
	 * <li>the default time stamp format pattern: yyyy-MM-dd'T'HH:mm:ss.SSS SETTING_DEFAULTFORMAT_TIMESTAMP.</li>
	 * <li>user defined time stamp format pattern.</li>
	 * <p>
	 * @see <a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">valid patterns</a>
	 */
	public final static String SETTING_TIMESTAMPFORMAT=IMPORT_PREFIX+".timeStampFormat";
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