package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.ioxwkf.dbtools.AbstractImport2db;
import ch.interlis.ioxwkf.dbtools.IoxWkfConfig;
import ch.interlis.ioxwkf.dbtools.Csv2db;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Csv2dbTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private Statement stmt=null;
	private Map<String, List<String>> rows=null;
	private static final String TEST_IN="src/test/data/Csv2DB/";
	
	// Bei diesem Test darf keine Fehlermeldung ausgegeben werden.
	// Hier werden alle moeglichen Parameter gesetzt.
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportwithheadernopk(idname character varying,abbreviation character varying,state character varying) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "AttributesHeader.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportwithheadernopk");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM csvtodbschema.csvimportwithheadernopk;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,abbreviation,state FROM csvtodbschema.csvimportwithheadernopk;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(3, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("20", rs.getObject(1));
				  	assertEquals("AU", rs.getObject(2));
				  	assertEquals("Deutschland", rs.getObject(3));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Hier darf keine Fehlermeldung ausgegeben werden.
	// Der Header ist nicht gesetzt, somit wird die erste Zeile als Werte Zeile gelesen.
	// Das Datenbankschema ist nicht gesetzt, somit wird innerhalb des Default-Schemas gesucht.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: header-present
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SchemaNotSet_HeaderNotSet_Ok() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop table
	        	preStmt.execute("DROP TABLE IF EXISTS csvimportnopkcolumn CASCADE");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvimportnopkcolumn(attr1 character varying) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "AttributesHeaderAbsent.csv");
				// HEADER: HEADERPRESENT, HEADERABSENT not set
				// DBSCHEMA: "csvtodbschema" not set
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportnopkcolumn");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM csvimportnopkcolumn;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr1 FROM csvimportnopkcolumn;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(1, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("Holland", rs.getObject(1));
				}
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
			
		}
	}
	
	// Hier darf keine Fehlermeldung ausgegeben werden.
	// Das Datenbankschema ist nicht gesetzt, somit wird innerhalb des Default-Schemas gesucht.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-present
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SchemaNotSet_Ok() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop table
	        	preStmt.execute("DROP TABLE IF EXISTS csvimportwithheader CASCADE");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvimportwithheader(idname character varying,abbreviation character varying,state character varying) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "AttributesHeader.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				// DBSCHEMA: not set "csvtodbschema"
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportwithheader");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM csvimportwithheader;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,abbreviation,state FROM csvimportwithheader;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(3, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("20", rs.getObject(1));
				  	assertEquals("AU", rs.getObject(2));
				  	assertEquals("Deutschland", rs.getObject(3));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bigint
	@Test
	public void import_Datatype_BigInt_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr bigint) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeBigint.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("9223372036854775807"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=boolean
	@Test
	public void import_Datatype_Boolean_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr boolean) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeBoolean.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("t"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bit
	@Test
	public void import_Datatype_Bit_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr boolean) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeBit.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("t"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=char
	@Test
	public void import_Datatype_Char_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr character) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeChar.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("a"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=varchar
	@Test
	public void import_Datatype_VarChar_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr character varying) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeVarchar.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("aaaaaaaaa"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=date
	@Test
	public void import_Datatype_Date_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr date) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeDate.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("2017-02-02"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=integer
	@Test
	public void import_Datatype_Integer_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr integer) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeInteger.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("123"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=numeric
	@Test
	public void import_Datatype_Numeric_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr numeric) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeNumeric.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("12345"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=text
	@Test
	public void import_Datatype_Text_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr character varying) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeText.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("testtext"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=time
	@Test
	public void import_Datatype_Time_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr time without time zone) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeTime.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("10:10:59"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=smallint
	@Test
	public void import_Datatype_Smallint_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr smallint) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeSmallint.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("12"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}	
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=timestamp
	@Test
	public void import_Datatype_Timestamp_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr timestamp without time zone) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeTimestamp.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("2014-05-15 12:30:30.555"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=uuid	
	@Test
	public void import_Datatype_Uuid_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr uuid) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeUuid.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("123e4567-e89b-12d3-a456-426655440000"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=xml
	@Test
	public void import_Datatype_Xml_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportdatatype(attr xml) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "DataTypeXml.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportdatatype");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attrValue=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportdatatype.attr FROM "+config.getValue(IoxWkfConfig.SETTING_DBSCHEMA)+".csvimportdatatype");
				while(rs.next()){
					attrValue = rs.getString(1);
					assertTrue(attrValue.equals("<attrText>text</attrText>"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}	
	
	// Testet ob der richtige Fehler ausgegeben wird, wenn die
	// Attribute der Tabelle innerhalb der CSV-Datei nicht gefunden werden kann.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-->present
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: data base attribute names ... not found.
	@Test
	public void import_AttrNamesOfDbNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportwithheader(id character varying NOT NULL,abbreviation character varying,state character varying,CONSTRAINT csvimportwithheader_pkey PRIMARY KEY (id)) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_IN, "AttributesHeader3.csv");
			config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportwithheader");
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("data base attribute names:"));
			assertTrue(e.getMessage().contains("planet"));
			assertTrue(e.getMessage().contains("name"));
			assertTrue(e.getMessage().contains("lastname"));
			assertTrue(e.getMessage().contains("not found in AttributesHeader3.csv"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Wenn die Verbindung zu Datenbank: null ist,
	// muss die Fehlermeldung: connection=null ausgegeben werden.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: Csv-file
	// - NOT SET: connection content.
	// --
	// Erwartung: FEHLER: connection=null.
	@Test
	public void import_ConnectionFailed_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        jdbcConnection = null;
			// csv
			File data=new File(TEST_IN, "AttributesHeader.csv");
			Csv2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertEquals("connection==null.",e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Wenn alle Parameter nicht gesetzt werden, muss die Fehlermeldung
	// table not found ausgegeben werden.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: header not set
	// - NOT SET: database-schema not set
	// - NOT SET: database-table not set
	// --
	// Erwartung: FEHLER: table ... not found.
	@Test
	public void import_AllNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	preStmt.close();
	        }
	        // csv
			File data=new File(TEST_IN, "AttributesHeader.csv");
			// HEADER: HEADERPRESENT, HEADERABSENT not set
			// DBSCHEMA: "csvtodbschema" not set
			// TABLE: "csvimportwithheader" not set
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("database table==null."));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Wenn der Parameter database-table nicht gesetzt wird, muss die Fehlermeldung
	// table not found ausgegeben werden.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header
	// - set: database-schema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: table ... not found.
	@Test
	public void import_TableNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_IN, "AttributesHeader.csv");
			config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
			// TABLE: "csvimportwithheader" not set
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("database table==null."));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Wenn die CSV-Datei nicht gefunden wird, muss die Fehlermeldung
	// csv file not found ausgegeben werden.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: csv file not found.
	@Test
	public void import_CsvFileNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS csvtodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA csvtodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportwithheader(idname character varying NOT NULL,abbreviation character varying,state character varying) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        // csv
			File data=new File("src/test/data/NotExist/AttributesHeader.csv");
			config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "csvtodbschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimportwithheader");
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("file: D:\\GIT\\iox-wkf\\iox-wkf\\src\\test\\data\\NotExist\\AttributesHeader.csv not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Wenn die id als primary key definiert ist und die Daten bereits in der Datenbank-Tabelle existieren,
	// muss die Fehlermeldung import fehlgeschlagen ausgegeben werden.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: import .. failed.
	@Test
	public void import_UniqueConstraint_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop table
	        	preStmt.execute("DROP TABLE IF EXISTS csvimport CASCADE");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE csvimport(attr1 character varying NOT NULL, CONSTRAINT csvimport_pkey PRIMARY KEY (attr1))WITH (OIDS=FALSE)");
	        	preStmt.executeUpdate("INSERT INTO csvimport(attr1)VALUES('Holland')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_IN, "AttributesHeaderAbsent.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_VALUE);
				// DBSCHEMA: "csvtodbschema" not set
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvimport");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
				fail();
			}
		}catch(IoxException e) {
			assertTrue(e.getCause() instanceof SQLException);
			// unique violation.
			assertEquals("23505",((SQLException) e.getCause()).getSQLState());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}