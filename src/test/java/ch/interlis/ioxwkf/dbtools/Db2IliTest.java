package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;
import org.junit.BeforeClass;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iox.IoxException;

public class Db2IliTest {

	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private static final String TEST_OUT="src/test/data/Db2Ili/";
	
	@BeforeClass
	public static void setup() throws Ili2cFailure
	{
		new File(TEST_OUT).mkdirs();
	}
	
	// Alle Tabellen innerhalb dem definierten Schema, sollen gefunden werden.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Alle Tabellen innerhalb dem definierten Schema werden gefunden.
	@Test
	public void export_FindAllTablesInDefinedSchema_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoilischema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtoilischema");
	        	preStmt.execute("CREATE TABLE dbtoilischema.table1();");
	        	preStmt.execute("CREATE TABLE dbtoilischema.table2();");
	        	preStmt.execute("CREATE TABLE dbtoilischema.table3();");
	        	preStmt.execute("CREATE TABLE dbtoilischema.table4();");
	        	preStmt.execute("CREATE TABLE dbtoilischema.table5();");
	        	preStmt.close();
	        }
			File data=new File(TEST_OUT+"export_FindAllTablesInDefinedSchema_Ok.ili");
			if(data.exists()) {
				data.delete();
			}
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoilischema");
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(data, jdbcConnection, config);
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		Scanner scanner=null;
		try{
			scanner=new Scanner(new File(TEST_OUT+"export_FindAllTablesInDefinedSchema_Ok.ili"));
			if(scanner!=null) {
				// \Z read to the end of the file.
				String fileContent = scanner.useDelimiter("\\Z").next();
				assertTrue(fileContent.contains("table1"));
				assertTrue(fileContent.contains("table2"));
				assertTrue(fileContent.contains("table3"));
				assertTrue(fileContent.contains("table4"));
				assertTrue(fileContent.contains("table5"));
				assertFalse(fileContent.contains("table6"));
			}
			// model compile test
			String iliFile=TEST_OUT+"export_FindAllTablesInDefinedSchema_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFile);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			if(td==null){
				throw new IoxException("ili-compiler failed");
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(scanner!=null) {
				scanner.close();
			}
		}
	}
	
	// Es wird 1 Tabelle innerhalb des definierten Schemas erstellt.
	// Innerhalb dieser Tabellen sollen alle AttributeTypen erstellt werden.
	// Die Tabelle wird mit den DatenTypen in die ili Datei exportiert.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Alle AttributeTypen sollen erkannt werden.
	@Test
	public void export_RecognizeAllDataTypesInClass_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoilischema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoilischema CASCADE");
	        	// create dbtoilischema
	        	preStmt.execute("CREATE SCHEMA dbtoilischema");
	        	// create table in dbtoilischema
	        	try {
		        	preStmt.execute("CREATE TABLE dbtoilischema.table1("
		        			+ "attr1 smallint,"
		        			+ "attr2 integer,"
		        			+ "attr3 bigint,"
		        			+ "attr4 decimal,"
		        			+ "attr5 numeric,"
		        			+ "attr6 real,"
		        			+ "attr9 character,"
		        			+ "attr10 character(5),"
		        			+ "attr11 char,"
		        			+ "attr12 char(2),"
		        			+ "attr13 varchar,"
		        			+ "attr14 varchar(5),"
		        			+ "attr15 character varying,"
		        			+ "attr16 character varying(10),"
		        			+ "attr17 date,"
		        			+ "attr18 time,"
		        			+ "attr19 time with time zone,"
		        			+ "attr20 timestamp,"
		        			+ "attr21 timestamp with time zone,"
							+ "attr22 boolean,"
							+ "attr23 bit(3),"
							+ "attr24 bit(5),"
							+ "attr25 uuid,"
							+ "attr26 xml,"
							+ "attr27 geometry(POINT,2056),"
							+ "attr29 geometry(LINESTRING,2056),"
							+ "attr31 geometry(POLYGON,2056)"
							+ ")WITH (OIDS=FALSE);");
		        	preStmt.close();
	        	}catch(Exception e) {
	        		throw new IoxException(e);
	        	}
	        }
	        {
				File data=new File(TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoilischema");
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(data, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		Scanner scanner=null;
		try{
			scanner=new Scanner(new File(TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili"));
			if(scanner!=null) {
				// \Z read to the end of the file.
				String fileContent = scanner.useDelimiter("\\Z").next();
				assertTrue(fileContent.contains(""));
				assertTrue(fileContent.contains(""));
				assertTrue(fileContent.contains(""));
			}
			// model compile test
			String iliFile=TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFile);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			if(td==null){
				throw new IoxException("ili-compiler failed");
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(scanner!=null) {
				scanner.close();
			}
		}
	}
	
	// Die ili-Datei wird fuer den Export nicht angegeben.
	// Eine Fehlermeldung muss ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: file==null.
	@Test
	public void export_FileNotDefined_Fail() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoilischema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtoilischema");
	        	preStmt.execute("CREATE TABLE dbtoilischema.table1();");
	        	preStmt.close();
	        }
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoilischema");
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(null, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertEquals("file==null", e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Die Connection wird dem Export nicht mitgegeben.
	// Eine Fehlermeldung muss ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: connection==null.
	@Test
	public void export_ConnectionFailed_Fail() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoilischema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtoilischema");
	        	preStmt.execute("CREATE TABLE dbtoilischema.table1();");
	        	preStmt.close();
	        }
	        File data=new File(TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoilischema");
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(data, null, config);
			fail();
		}catch(IoxException e) {
			assertEquals("connection==null", e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Das DB-Schema wird dem Export nicht mitgegeben.
	// Eine Fehlermeldung muss ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: db schema name==null.
	@Test
	public void export_DbSchemaNotDefined_Fail() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoilischema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtoilischema");
	        	preStmt.execute("CREATE TABLE dbtoilischema.table1();");
	        	preStmt.close();
	        }
	        File data=new File(TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(data, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertEquals("db schema name==null", e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}