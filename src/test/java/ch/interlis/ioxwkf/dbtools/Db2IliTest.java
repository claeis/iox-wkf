package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import org.junit.BeforeClass;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.Container;
import ch.interlis.ili2c.metamodel.Element;
import ch.interlis.ili2c.metamodel.Model;
import ch.interlis.ili2c.metamodel.Table;
import ch.interlis.ili2c.metamodel.Topic;
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
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		final String TABLE2="table2";
		final String TABLE3="table3";
		final String TABLE4="table4";
		final String TABLE5="table5";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE2+"();");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE3+"();");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE4+"();");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE5+"();");
	        	preStmt.close();
	        }
			File data=new File(TEST_OUT+"export_FindAllTablesInDefinedSchema_Ok.ili");
			if(data.exists()) {
				data.delete();
			}
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(data, jdbcConnection, config);
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFile=TEST_OUT+"export_FindAllTablesInDefinedSchema_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFile);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			assertNotNull(td);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			Table table2=(Table) topic.getElement(Table.class, TABLE2);
			assertNotNull(table2);
			Table table3=(Table) topic.getElement(Table.class, TABLE3);
			assertNotNull(table3);
			Table table4=(Table) topic.getElement(Table.class, TABLE4);
			assertNotNull(table4);
			Table table5=(Table) topic.getElement(Table.class, TABLE5);
			assertNotNull(table5);
		}catch(Exception e) {
			throw new IoxException(e);
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
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoilischema
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	// create dbtoilischema
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	// create table in dbtoilischema
	        	try {
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"("
		        			+ "attr1 smallint NOT NULL,"
		        			+ "attr2 integer,"
		        			+ "attr3 bigint,"
		        			+ "attr4 decimal(10),"
		        			+ "attr5 numeric(20),"
		        			+ "attr6 real,"
		        			+ "attr7 float,"
		        			+ "attr8 double precision,"
		        			+ "attr9 character,"
		        			+ "attr10 character(5) NOT NULL,"
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
							+ "attr27 geometry(POINT,2056) NOT NULL,"
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
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(data, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFile=TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFile);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			assertNotNull(td);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			AttributeDef formAttr=(AttributeDef) table1.getElement(AttributeDef.class, "attr1");
			assertNotNull("attr1 : MANDATORY -32768 .. 32767;", formAttr);
			AttributeDef formAttr2=(AttributeDef) table1.getElement(AttributeDef.class, "attr6");
			assertNotNull("attr6 : -340282346638528860000000000000000000000.000000 .. 340282346638528860000000000000000000000.000000;", formAttr2);
			AttributeDef formAttr3=(AttributeDef) table1.getElement(AttributeDef.class, "attr10");
			assertNotNull("attr10 : MANDATORY TEXT*5;", formAttr3);
			AttributeDef formAttr4=(AttributeDef) table1.getElement(AttributeDef.class, "attr13");
			assertNotNull("attr13 : TEXT*2147483647;", formAttr4);
			AttributeDef formAttr5=(AttributeDef) table1.getElement(AttributeDef.class, "attr25");
			assertNotNull("attr25 : INTERLIS.UUIDOID;", formAttr5);
			AttributeDef formAttr6=(AttributeDef) table1.getElement(AttributeDef.class, "attr16");
			assertNotNull("attr16 : TEXT*10;", formAttr6);
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}
	
	// Es wird 1 Tabelle innerhalb des definierten Schemas erstellt.
	// Innerhalb dieser Tabellen werden Koordinaten, Linien und Oberflaechen mit 3 verschiedenen EPSG Code's erstellt.
	// Die Tabelle wird mit den DatenTypen in die ili Datei exportiert.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Alle AttributeTypen sollen erkannt und geschrieben werden.
	@Test
	public void export_export_DifferentEPSGCoords_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoilischema
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	// create dbtoilischema
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	// create table in dbtoilischema
	        	try {
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"("
							+ "attr1 geometry(POINT,2056) NOT NULL,"
							+ "attr2 geometry(LINESTRING,2056),"
							+ "attr3 geometry(POLYGON,2056),"
							+ "attr4 geometry(POINT,21781) NOT NULL,"
							+ "attr5 geometry(LINESTRING,21781),"
							+ "attr6 geometry(POLYGON,21781),"
							+ "attr7 geometry(POINT,3333) NOT NULL,"
							+ "attr8 geometry(LINESTRING,3333),"
							+ "attr9 geometry(POLYGON,3333)"
							+ ")WITH (OIDS=FALSE);");
		        	preStmt.close();
	        	}catch(Exception e) {
	        		throw new IoxException(e);
	        	}
	        }
	        {
				File data=new File(TEST_OUT+"export_DifferentEPSGCoords_Ok.ili");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(data, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFile=TEST_OUT+"export_DifferentEPSGCoords_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFile);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			AttributeDef formAttr=(AttributeDef) table1.getElement(AttributeDef.class, "attr1");
			assertNotNull("attr2 : POLYLINE WITH (STRAIGHTS) VERTEX lcoord2056 WITHOUT OVERLAPS > 0.01;", formAttr);
			AttributeDef formAttr2=(AttributeDef) table1.getElement(AttributeDef.class, "attr4");
			assertNotNull("attr5 : POLYLINE WITH (STRAIGHTS) VERTEX lcoord21781 WITHOUT OVERLAPS > 0.01;", formAttr);
			AttributeDef formAttr3=(AttributeDef) table1.getElement(AttributeDef.class, "attr7");
			assertNotNull("attr8 : POLYLINE WITH (STRAIGHTS) VERTEX lcoord3333 WITHOUT OVERLAPS > 0.01;", formAttr);
		}catch(Exception e) {
			throw new IoxException(e);
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
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
	        	preStmt.close();
	        }
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
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
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
	        	preStmt.close();
	        }
	        File data=new File(TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
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
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
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