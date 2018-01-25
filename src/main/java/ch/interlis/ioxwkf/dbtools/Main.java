package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;

/** Main program and command line interface of db2ili.
 */
public class Main {
	/** name of application as shown to user.
	 */
	private static final String APP_NAME="db2ili";
	/** name of jar file.
	 */
	private static final String APP_JAR="db2ili.jar";
	/** version of application.
	 */
	private static String version=null;
	/** main program entry.
	 * @param args command line arguments.
	 * @throws IoxException 
	 */
	public static void main(String args[]) throws IoxException{
		Settings settings=new Settings();
		String dbhost=null;
		String dbport=null;
		String dbname=null;
		String dbusr=null;
		String dbpwd=null;
		// arguments on export
		if(args.length==0){
			return;
		}
		int argi=0;
		for(;argi<args.length;argi++){
			String arg=args[argi];
			if(arg.equals("--trace")){
				EhiLogger.getInstance().setTraceFilter(false);
			}else if(arg.equals("--dbhost")) {
				argi++;
				dbhost=args[argi];
			}else if(arg.equals("--dbport")) {
				argi++;
				dbport=args[argi];
			}else if(arg.equals("--dbdatabase")) {
				argi++;
				dbname=args[argi];
			}else if(arg.equals("--dbusr")) {
				argi++;
				dbusr=args[argi];
			}else if(arg.equals("--dbpwd")) {
				argi++;
				dbpwd=args[argi];
			}else if(arg.equals("--dbschema")) {
				argi++;
				settings.setValue(IoxWkfConfig.SETTING_DBSCHEMA, args[argi]);
			}else if(arg.equals("--version")){
				printVersion();
				return;
			}else if(arg.equals("--help")){
				System.err.println();
				printDescription ();
				System.err.println();
				printUsage ();
				System.err.println();
				printConnectOptions();
				System.err.println();
				System.err.println("OPTIONS");
				System.err.println("--trace            	enable trace messages.");
				System.err.println("--dbschema schema       the name of the schema in the database. Defaults to not set.");
				System.err.println("--help            	display this help text.");
				System.err.println("--version               display the version of "+APP_NAME+".");
				System.err.println();
				return;
			}else if(arg.startsWith("-")){
				EhiLogger.logAdaption(arg+": unknown option; ignored");
			}else{
				break;
			}
		}
		int dataFileCount=args.length-argi;
		if(dataFileCount>0) {
			Connection conn=null;
			File iliFile = new File(args[argi]);
			try {
				// check connection
				conn=connect(makeUrl(dbhost,dbport,dbname),dbusr,dbpwd);
			} catch (SQLException e) {
				EhiLogger.logError(APP_NAME+": connection to data base failed");
				System.exit(1);
			}
			Db2Ili db2Ili=new Db2Ili();
			try {
				db2Ili.exportData(iliFile, conn, settings);
				EhiLogger.logState(APP_NAME+": export ok");
			} catch (SQLException e) {
				EhiLogger.logError(APP_NAME+": export failed");
				System.exit(1);
			}
		}else{
			EhiLogger.logError(APP_NAME+": wrong number of arguments");
			System.exit(2);
		}
	}
	
	private static Connection connect(String dburl,String dbusr,String dbpwd) throws SQLException {
		Connection conn=null;
		EhiLogger.logState("dburl <" + dburl + ">");
		EhiLogger.logState("dbusr <" + dbusr + ">");
		// connection test
		conn=DriverManager.getConnection(dburl, dbusr, dbpwd);
		return conn;
	}
		
	private static String makeUrl(String dbhost,String dbport, String dbname) {
		if(dbname!=null){
			if(dbhost!=null){
				if(dbport!=null){
					return "jdbc:postgresql://"+dbhost+":"+dbport+"/"+dbname;
				}
				return "jdbc:postgresql://"+dbhost+"/"+dbname;
			}
			return "jdbc:postgresql:"+dbname;
		}
		return null;
	}
	
	private static void printDescription()	{
		System.err.println("DESCRIPTION");
		System.err.println("  Exports an INTERLIS model from db tables.");
	}

	private static void printUsage() {
		System.err.println ("USAGE");
		System.err.println("  java -jar "+APP_JAR+" [Options] in.ili");
	}
	
	private static void printVersion() {
		System.err.println(APP_NAME+", Version "+getVersion());
		System.err.println("  Developed by Eisenhut Informatik AG, CH-3400 Burgdorf");
	}
	
	private static void printConnectOptions() {
		System.err.println ("CONNECTION OPTIONS");
		System.err.println("--dbhost  host          the host name of the server. Defaults to localhost.");
		System.err.println("--dbport  port          the port number the server is listening on. Defaults to 5432.");
		System.err.println("--dbdatabase database   the database name.");
		System.err.println("--dbusr  username       user name to access database.");
		System.err.println("--dbpwd  password       password of user used to access database.");
	}
	
	private static String getVersion() {
		if(version==null){
			java.util.ResourceBundle resVersion = java.util.ResourceBundle.getBundle(ch.ehi.basics.i18n.ResourceBundle.class2qpackageName(Main.class)+".Version");
			StringBuffer ret=new StringBuffer(20);
			ret.append(resVersion.getString("version"));
			ret.append('-');
			ret.append(resVersion.getString("versionCommit"));
			version=ret.toString();
		}
		return version;
	}
}
