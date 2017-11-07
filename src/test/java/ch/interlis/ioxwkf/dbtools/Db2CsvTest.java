package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.ioxwkf.dbtools.Config;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Db2CsvTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private static final String TEST_OUT="src/test/data/DB2Csv/";
	private static final String ATTR_ID="idname";
	private static final String ATTR_ABBREVIATION="abbreviation";
	private static final String ATTR_STATE="state";
	private Statement stmt=null;
	private static final String ROW="row";
	private Map<String, List<String>> rows=null;
	// Es soll keine Fehlermeldung ausgegeben werden, 1 Reihe der Tabelle in eine Csv-Datei geschrieben wird.
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void export_SingleRow_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"exportSingleRow_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"exportSingleRow_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("10", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("Schweiz", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, wenn 10 Tabellenspalten aus einer Tabelle in eine Csv-Datei exportiert werden.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void export_10ColumnNames_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk10(attr11 text, attr12 text, attr13 text, attr14 text, attr15 text, attr16 text, attr17 text, attr18 text, attr19 text, attr20 text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk10 (attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19, attr20) VALUES ('a_11', 'a_12', 'a_13', 'a_14', 'a_15', 'a_16', 'a_17', 'a_18', 'a_19', 'a_20')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_10ColumnNames_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "csvexportnopk10");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_10ColumnNames_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(10, iomObj.getattrcount());
		        	assertEquals("a_11", iomObj.getattrvalue("attr11"));
		        	assertEquals("a_12", iomObj.getattrvalue("attr12"));
		        	assertEquals("a_13", iomObj.getattrvalue("attr13"));
		        	assertEquals("a_14", iomObj.getattrvalue("attr14"));
		        	assertEquals("a_15", iomObj.getattrvalue("attr15"));
		        	assertEquals("a_16", iomObj.getattrvalue("attr16"));
		        	assertEquals("a_17", iomObj.getattrvalue("attr17"));
		        	assertEquals("a_18", iomObj.getattrvalue("attr18"));
		        	assertEquals("a_19", iomObj.getattrvalue("attr19"));
		        	assertEquals("a_20", iomObj.getattrvalue("attr20"));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, wenn der Delimiter vom Benutzer aus gesetzt wird.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// - set: delimiter
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void export_SetDelimiter_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_SetDelimiter_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_QUOTATIONMARK, "|");
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SetDelimiter_Ok.csv"));
				
				CsvReader.setDelimiter("|");
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("10", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("Schweiz", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, wenn der Record-Delimiter vom Benutzer aus gesetzt wird.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// - set: record delimiter
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void export_SetRecordDelimiter_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_SetRecordDelimiter_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_VALUEDELIMITER, "|");
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SetRecordDelimiter_Ok.csv"));
				
				reader.setRecordDelimiter("|");
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("10", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("Schweiz", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, wenn der Delimiter und der Record-Delimiter vom Benutzer gesetzt wird.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// - set: delimiter
	// - set: record delimiter
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void export_SetDelimiterAndRecordDelimiter_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_SetDelimiterAndRecordDelimiter_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_QUOTATIONMARK, "|");
				config.setValue(Config.SETTING_VALUEDELIMITER, ":");
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SetDelimiterAndRecordDelimiter_Ok.csv"));
				
				CsvReader.setDelimiter("|");
				reader.setRecordDelimiter(":");
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("10", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("Schweiz", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, da beim Setzen des Headers immer present gesetzt wird.
	// - set: database-dbtocsvschema
	// - set: database-table
	// - set: header --> absent
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void export_HeaderAbsent_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_HeaderAbsent_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_VALUE);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_HeaderAbsent_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("10", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("Schweiz", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll keine Fehlermeldung ausgegeben werden, wenn mehrere Reihen innerhalb der Datenbank Tabelle definiert sind.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void export_MultipleRows_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('11', 'DE', 'Deutschland')");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('12', 'FR', 'Frankreich')");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('13', 'IT', 'Italien')");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('14', 'ES', 'Spanien')");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('15', 'AT', 'Oesterreich')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"exportMultipleRows_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"exportMultipleRows_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				int objectCount=0;
				while(event instanceof ObjectEvent){
					objectCount+=1;
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	if(iomObj.getattrvalue(ATTR_ID).equals("10")) {
		        		assertEquals(3, iomObj.getattrcount());
		        		assertEquals("CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        		assertEquals("Schweiz", iomObj.getattrvalue(ATTR_STATE));
		        	}else if(iomObj.getattrvalue(ATTR_ID).equals("11")) {
		        		assertEquals(3, iomObj.getattrcount());
		        		assertEquals("DE", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        		assertEquals("Deutschland", iomObj.getattrvalue(ATTR_STATE));
		        	}else if(iomObj.getattrvalue(ATTR_ID).equals("12")) {
		        		assertEquals(3, iomObj.getattrcount());
		        		assertEquals("FR", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        		assertEquals("Frankreich", iomObj.getattrvalue(ATTR_STATE));
		        	}else if(iomObj.getattrvalue(ATTR_ID).equals("13")) {
		        		assertEquals(3, iomObj.getattrcount());
		        		assertEquals("IT", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        		assertEquals("Italien", iomObj.getattrvalue(ATTR_STATE));
		        	}else if(iomObj.getattrvalue(ATTR_ID).equals("14")) {
		        		assertEquals(3, iomObj.getattrcount());
		        		assertEquals("ES", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        		assertEquals("Spanien", iomObj.getattrvalue(ATTR_STATE));
		        	}else if(iomObj.getattrvalue(ATTR_ID).equals("15")) {
		        		assertEquals(3, iomObj.getattrcount());
		        		assertEquals("AT", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        		assertEquals("Oesterreich", iomObj.getattrvalue(ATTR_STATE));
		        	}
		        	event=reader.read();
				}
				assertTrue(objectCount==6);
				assertTrue(event instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil auch ohne die Angabe des Datenbank Schemas, die Tabelle gefunden werden kann.
	// - NOT SET: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void export_SchemaNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO defaultcsvexportnopk (idname, abbreviation, state) VALUES ('S_10', 'S_CH', 'S_Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_SchemaNotSet_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				// DBSCHEMA: "dbtocsvschema" not set
				config.setValue(Config.SETTING_DBTABLE, "defaultcsvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SchemaNotSet_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("S_10", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("S_CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("S_Schweiz", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil die Tabelle im gesetzten Schema zwar gefunden wird, jedoch keine Daten --> "NULL" beinhaltet.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: Success: Die Daten sollen als leeren String in das IomObject eingeschrieben werden.
	//@Test
	public void export_DataContainsNullInTableInSchema_Ok() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES (NULL, NULL, NULL)");
	        	// import data to table
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_DataContainsNullInTableInSchema_Ok.csv");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_DataContainsNullInTableInSchema_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil die Tabelle innerhalb des Default-Schemas zwar gefunden wird, jedoch keine Daten --> "NULL" beinhaltet.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: Success: Die Daten sollen als leeren String in das IomObject eingeschrieben werden.
	//@Test
	public void export_DataContainsNullInTable_Ok() throws Exception
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
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO defaultcsvexportnopk (idname, abbreviation, state) VALUES (NULL, NULL, NULL)");
	        	// import data to table
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_DataContainsNullInTable_Ok.csv");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			// DBSCHEMA: "dbtocsvschema" not set
			config.setValue(Config.SETTING_DBTABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_DataContainsNullInTable_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil 2 Tabellen mit dem selben Namen in unterschiedlichen Schemen existieren.
	// Dabei wird die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-table
	// --
	// Erwartung: Es sollte nur diese Tabelle gefunden werden, welche sich innerhalb des gesetzten Schemas befindet.
	//@Test
	public void export_FindTableInDefinedSchema_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	preStmt.execute("CREATE TABLE dbtocsvschema.defaultcsvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO defaultcsvexportnopk (idname, abbreviation, state) VALUES ('D_10', 'D_CH', 'D_Schweiz')");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.defaultcsvexportnopk (idname, abbreviation, state) VALUES ('S_10', 'S_CH', 'S_Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_FindTableInDefinedSchema_Ok.csv");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
			config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.SETTING_DBTABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_FindTableInDefinedSchema_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("S_10", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("S_CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("S_Schweiz", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil 2 Tabellen mit dem selben Namen in unterschiedlichen Schemen existieren.
	// Dabei wird die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-table
	// --
	// Erwartung: Es sollte nur diese Tabelle gefunden werden, welche sich innerhalb des default Schemas befindet.
	//@Test
	public void export_FindTableInDefaultSchema_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	preStmt.execute("CREATE TABLE dbtocsvschema.defaultcsvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO defaultcsvexportnopk (idname, abbreviation, state) VALUES ('D_10', 'D_CH', 'D_Schweiz')");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.defaultcsvexportnopk (idname, abbreviation, state) VALUES ('S_10', 'S_CH', 'S_Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_FindTableInDefaultSchema_Ok.csv");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
			// DBSCHEMA: "dbtocsvschema" not set
			config.setValue(Config.SETTING_DBTABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_FindTableInDefaultSchema_Ok.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(3, iomObj.getattrcount());
		        	assertEquals("D_10", iomObj.getattrvalue(ATTR_ID));
		        	assertEquals("D_CH", iomObj.getattrvalue(ATTR_ABBREVIATION));
		        	assertEquals("D_Schweiz", iomObj.getattrvalue(ATTR_STATE));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bigint
	//@Test
	public void export_Datatype_BigInt_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr bigint) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('9223372036854775807')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeBigint.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeBigint.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("9223372036854775807", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=boolean
	//@Test
	public void export_Datatype_Boolean_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr boolean) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('true')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeBoolean.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeBoolean.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("true", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bit
	//@Test
	public void export_Datatype_Bit_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr bit) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('1')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeBit.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeBit.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("1", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=char
	//@Test
	public void export_Datatype_Char_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr character) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('a')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeChar.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeChar.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("a", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=varchar
	//@Test
	public void export_Datatype_VarChar_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr character varying) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('abc')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeVarchar.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeVarchar.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("abc", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=date
	//@Test
	public void export_Datatype_Date_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr date) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('2017-02-02')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeDate.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeDate.csv"));
				
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("2017-02-02", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=integer
	//@Test
	public void export_Datatype_Integer_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr integer) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('12')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeInteger.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeInteger.csv"));
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("12", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=numeric
	//@Test
	public void export_Datatype_Numeric_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr numeric) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('123')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeNumeric.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeNumeric.csv"));
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("123", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=text
	//@Test
	public void export_Datatype_Text_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr text) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('text')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeText.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeText.csv"));
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("text", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=time
	//@Test
	public void export_Datatype_Time_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr time without time zone) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('10:10:59')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeTime.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeTime.csv"));
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("10:10:59", iomObj.getattrvalue("attr"));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=smallint
	//@Test
	public void export_Datatype_Smallint_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr smallint) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('5')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeSmallint.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeSmallint.csv"));
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("5", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}	
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=timestamp
	//@Test
	public void export_Datatype_Timestamp_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr timestamp without time zone) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('2014-05-15 12:30:30.555')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeTimestamp.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeTimestamp.csv"));
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("2014-05-15T12:30:30.555", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=uuid	
	//@Test
	public void export_Datatype_Uuid_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr uuid) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('123e4567-e89b-12d3-a456-426655440000')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeUuid.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeUuid.csv"));
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("123e4567-e89b-12d3-a456-426655440000", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die CSV Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=xml
	//@Test
	public void export_Datatype_Xml_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr xml) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('<attrText>text</attrText>')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"DataTypeXml.csv");
				config.setValue(Config.SETTING_FIRSTLINE, Config.SET_FIRSTLINE_AS_HEADER);
				config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"DataTypeXml.csv"));
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertEquals(1, iomObj.getattrcount());
		        	assertEquals("<attrText>text</attrText>", iomObj.getattrvalue("attr"));

				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
				reader.close();
				reader=null;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet, ob connection=null zu einer IoxException fuehrt 
	//@Test
	public void export_ConnectionFailed_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = null;
			// csv
			File data=new File(TEST_OUT+"export_ConnectionFailed_Fail.csv");
			config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
			Db2Csv db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertEquals(IoxException.class,e.getClass());
			assertEquals("connection==null",e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil das Schema zwar gesetzt wird, jedoch in der Datenbank nicht existiert.
	// Daraus folgt dass die Datenbank Tabelle nicht gefunden werden kann.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: FEHLER: table dbtocsvschema.table ... not found
	//@Test
	public void export_TableInSchemaNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_SchemaNotFound_Fail.csv");
			config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema99999");
			config.setValue(Config.SETTING_DBTABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("table"));
			assertTrue(e.getMessage().contains("dbtocsvschema99999.csvexportnopk"));
			assertTrue(e.getMessage().contains("not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Tabelle zwar gesetzt wird, jedoch in der Datenbank nicht existiert.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: FEHLER: table ... not found
	//@Test
	public void export_TableNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_TableNotFound_Fail.csv");
			config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.SETTING_DBTABLE, "csvexportnopk99999");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("table"));
			assertTrue(e.getMessage().contains("dbtocsvschema.csvexportnopk99999"));
			assertTrue(e.getMessage().contains("not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, da nichts in den Settings gesetzt wird.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: database-dbtocsvschema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	//@Test
	public void export_AllNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        // csv
			File data=new File(TEST_OUT+"export_AllNotSet_Fail.csv");
			// DBSCHEMA: "dbtocsvschema" not set
			// TABLE: "csvimportwithheader" not set
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("expected tablename"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, da die Tabelle nicht gesetzt wird.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	//@Test
	public void export_TableNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_TableNotSet_Fail.csv");
			config.setValue(Config.SETTING_DBSCHEMA, "dbtocsvschema");
			// TABLE: "csvimportwithheader" not set
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("expected tablename"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}