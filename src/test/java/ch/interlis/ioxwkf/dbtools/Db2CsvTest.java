package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.TransferDescription;
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
	private TransferDescription td=null;
	private static final String ATTR_ID="idname";
	private static final String ATTR_ABBREVIATION="abbreviation";
	private static final String ATTR_STATE="state";
	
	// Es soll keine Fehlermeldung ausgegeben werden, 1 Reihe der Tabelle in eine Csv-Datei geschrieben wird.
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SingleRow_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
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
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"exportSingleRow_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_10ColumnNames_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
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
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport10Columns");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk10");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport10Columns.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_10ColumnNames_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// - set: delimiter
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SetDelimiter_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
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
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				config.setValue(Config.DELIMITER, "|");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SetDelimiter_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// - set: record delimiter
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SetRecordDelimiter_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
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
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				config.setValue(Config.RECORD_DELIMITER, "|");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SetRecordDelimiter_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// - set: delimiter
	// - set: record delimiter
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SetDelimiterAndRecordDelimiter_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
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
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				config.setValue(Config.DELIMITER, "|");
				config.setValue(Config.RECORD_DELIMITER, ":");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SetDelimiterAndRecordDelimiter_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// - set: header --> absent
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_HeaderAbsent_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
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
				config.setValue(Config.HEADER, Config.HEADERABSENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_HeaderAbsent_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, wenn nur das Model nicht gesetzt wird.
	// - NOT SET: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_ModelsNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_ModelsNotSet_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				// SETTING_MODELNAMES: "modelExport" not set
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_ModelsNotSet_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_MultipleRows_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
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
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"exportMultipleRows_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, wenn mehrere Modelle gesetzt werden.
	// und sich die Tabelle mit den richtigen Attributen an der Ersten Stelle befindet.
	// - set: multiple models
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_MultipleModels_FirstModel_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_MultipleModels_FirstModel_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport;modelExport2;modelExport3");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// ili-datei lesen
				TransferDescription tdM=null;
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntryConditionClass=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE); // first input model
				ili2cConfig.addFileEntry(fileEntryConditionClass);
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport2.ili", FileEntryKind.ILIMODELFILE); // second input model
				ili2cConfig.addFileEntry(fileEntry);
				FileEntry fileEntry2=new FileEntry(TEST_OUT+"modelExport3.ili", FileEntryKind.ILIMODELFILE); // third input model
				ili2cConfig.addFileEntry(fileEntry2);
				tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(tdM);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_MultipleModels_FirstModel_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, wenn mehrere Modelle gesetzt werden.
	// und sich die Tabelle mit den richtigen Attributen an der Letzten Stelle befindet.
	// - set: multiple models
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_MultipleModels_LastModel_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_MultipleModels_LastModel_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport3;modelExport2;modelExport");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// ili-datei lesen
				TransferDescription tdM=null;
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntryConditionClass=new FileEntry(TEST_OUT+"modelExport3.ili", FileEntryKind.ILIMODELFILE); // first input model
				ili2cConfig.addFileEntry(fileEntryConditionClass);
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport2.ili", FileEntryKind.ILIMODELFILE); // second input model
				ili2cConfig.addFileEntry(fileEntry);
				FileEntry fileEntry2=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE); // third input model
				ili2cConfig.addFileEntry(fileEntry2);
				tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(tdM);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_MultipleModels_LastModel_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil auch ohne die Angabe des Datenbank Schemas, die Tabelle gefunden werden kann.
	// - set: model
	// - set: model-path
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SchemaNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
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
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				// DBSCHEMA: "dbtocsvschema" not set
				config.setValue(Config.TABLE, "defaultcsvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SchemaNotSet_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil das Schema und das Model nicht angegeben werden muessen.
	// - NOT SET: model
	// - set: model-path
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SchemaAndModelNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO defaultcsvexportnopk (idname, abbreviation, state) VALUES ('S_10', 'S_CH', 'S_Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_SchemaAndModelNotSet_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				// SETTING_MODELNAMES: "modelExport" not set
				config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
				// DBSCHEMA: "dbtocsvschema" not set
				config.setValue(Config.TABLE, "defaultcsvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SchemaAndModelNotSet_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil ohne den Modelpfad das Model gefunden werden kann und
	// die Datenbank Tabelle ohne das Schema gefunden werden kann.
	// - set: model
	// - NOT SET: model-path
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SchemaAndPathNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO defaultcsvexportnopk (idname, abbreviation, state) VALUES ('S_10', 'S_CH', 'S_Schweiz');");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_SchemaAndPathNotSet_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				// SETTING_ILIDIRS: TEST_OUT not set
				// DBSCHEMA: "dbtocsvschema" not set
				config.setValue(Config.TABLE, "defaultcsvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_SchemaAndPathNotSet_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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

	
	// Es soll keine Fehlermeldung ausgegeben werden, nur der Pfad zu den Modellen nicht definiert wird.
	// - set: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_PathNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_PathNotSet_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "modelExport");
				// SETTING_ILIDIRS: TEST_OUT not set
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_PathNotSet_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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

	
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_ModelAndPathNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_PathAndModelNotSet_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				// SETTING_MODELNAMES: "modelExport" not set
				// SETTING_ILIDIRS: TEST_OUT not set
				config.setValue(Config.DBSCHEMA, "dbtocsvschema");
				config.setValue(Config.TABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_PathAndModelNotSet_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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

	
	// Es soll keine Fehlermeldung ausgegeben werden, weil die Tabelle im gesetzten Schema zwar gefunden wird, jedoch keine Daten --> "NULL" beinhaltet.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: Success: Die Daten sollen als leeren String in das IomObject eingeschrieben werden.
	@Test
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
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
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
			config.setValue(Config.SETTING_MODELNAMES, "modelExport");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_DataContainsNullInTableInSchema_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: Success: Die Daten sollen als leeren String in das IomObject eingeschrieben werden.
	@Test
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
			config.setValue(Config.SETTING_MODELNAMES, "modelExport");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			// DBSCHEMA: "dbtocsvschema" not set
			config.setValue(Config.TABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_DataContainsNullInTable_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - set: database-table
	// --
	// Erwartung: Es sollte nur diese Tabelle gefunden werden, welche sich innerhalb des gesetzten Schemas befindet.
	@Test
	public void export_FindTableInDefinedSchema_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
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
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "modelExport");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_FindTableInDefinedSchema_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	// - set: model
	// - set: model-path
	// - set: database-table
	// --
	// Erwartung: Es sollte nur diese Tabelle gefunden werden, welche sich innerhalb des default Schemas befindet.
	@Test
	public void export_FindTableInDefaultSchema_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
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
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "modelExport");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			// DBSCHEMA: "dbtocsvschema" not set
			config.setValue(Config.TABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				// compile model
				Configuration ili2cConfig=new Configuration();
				FileEntry fileEntry=new FileEntry(TEST_OUT+"modelExport.ili", FileEntryKind.ILIMODELFILE);
				ili2cConfig.addFileEntry(fileEntry);
				td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
				assertNotNull(td);
				
				// test with reader created csv-file.
				CsvReader reader=new CsvReader(new File(TEST_OUT,"export_FindTableInDefaultSchema_Ok.csv"));
				reader.setModel(td);
				reader.setHeader(Config.HEADERPRESENT);
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
	
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Spalten-Namen (Attribute-Namen) der gesuchten Datenbank-Tabelle,
	// welche sich innerhalb des gesuchten Datenbank-Schemas befinden,
	// weder in den gesetzten Modellen, noch in der Csv-Datei gefunden werden koennen.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: data base attribute names ... not found.
	@Test
	public void export_AttrNamesOfDbNotFoundInCsvAndModels_Fail() throws Exception
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_AttrNamesOfDbNotFoundInCsvAndModels_Fail.csv");
			config.setValue(Config.SETTING_MODELNAMES, "model3");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("class attribute names"));
			assertTrue(e.getMessage().contains("idname"));
			assertTrue(e.getMessage().contains("state"));
			assertTrue(e.getMessage().contains("abbreviation"));
			assertTrue(e.getMessage().contains("not found in given models"));
			assertTrue(e.getMessage().contains("model3"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil das Passwort bei der Verbindung zur Datenbank nicht stimmt.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FATAL: Passwort-Authentifizierung fehlgeschlagen
	@Test
	public void export_ConnectionFailed_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, "12345");
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_ConnectionFailed_Fail.csv");
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("FATAL: Passwort-Authentifizierung"));
			assertTrue(e.getMessage().contains("fehlgeschlagen"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Attribute der Datenbank-Tabelle in keinem Modell gefunden werden koennen.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FATAL: data base attributes not found in models
	@Test
	public void export_AttrNamesOfDbNotFoundInModels_Fail() throws Exception
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_AttrNamesOfDbNotFoundInModels.csv");
			config.setValue(Config.SETTING_MODELNAMES, "model");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("class attribute names"));
			assertTrue(e.getMessage().contains("not found in given models"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Attribute der Datenbank-Tabelle in mehreren models gefunden werden.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FATAL: several possible classes were found: ...
	@Test
	public void export_SerevalPossibleClassesFound_Fail() throws Exception
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk2(sonne text, mond text, pluto text, mars text, jupiter text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk2 (sonne, mond, pluto, mars, jupiter) VALUES ('1_sonne', '2_mond', '3_pluto', '4_mars', '5_jupiter')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_SerevalPossibleClassesFound_Fail.csv");
			config.setValue(Config.SETTING_MODELNAMES, "sameAttrs1;sameAttrs2;sameAttrs3");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk2");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("several possible classes were found"));
			assertTrue(e.getMessage().contains("sameAttrs2.TopicAttrs2.ClassAttrs2"));
			assertTrue(e.getMessage().contains("sameAttrs1.TopicAttrs1.ClassAttrs1"));
			assertTrue(e.getMessage().contains("sameAttrs3.TopicAttrs3.ClassAttrs3"));
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: table schema.table ... not found
	@Test
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
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_SchemaNotFound_Fail.csv");
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "dbtocsvschema99999");
			config.setValue(Config.TABLE, "csvexportnopk");
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: table ... not found
	@Test
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
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_TableNotFound_Fail.csv");
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk99999");
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
	// - NOT SET: model
	// - NOT SET: model-path
	// - NOT SET: database-schema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	@Test
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
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        // csv
			File data=new File(TEST_OUT+"export_AllNotSet_Fail.csv");
			// SETTING_MODELNAMES: "model2" not set
			// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
			// DBSCHEMA: "csvtodbschema" not set
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
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	@Test
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
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_TableNotSet_Fail.csv");
			config.setValue(Config.SETTING_MODELNAMES, "modelExport");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
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
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Attribute innerhalb des Models nicht gefunden werden koennen.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: attributes of headerrecord ... not found in iliModel ...
	@Test
	public void export_AttrNamesNotFoundInModel_Fail() throws Exception
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_AttrNamesNotFoundInModel_Fail.csv");
			config.setValue(Config.SETTING_MODELNAMES, "model3");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("class attribute names"));
			assertTrue(e.getMessage().contains("idname"));
			assertTrue(e.getMessage().contains("abbreviation"));
			assertTrue(e.getMessage().contains("state"));
			assertTrue(e.getMessage().contains("not found in given models"));
			assertTrue(e.getMessage().contains("model3"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil das gesetzte Model nicht gefunden werden kann.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: models ... not found
	@Test
	public void export_ModelNotFound_Fail() throws Exception
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname text, abbreviation text, state text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_ModelNotFound_Fail.csv");
			config.setValue(Config.SETTING_MODELNAMES, "modelNotFound");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("models [modelNotFound] not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Die Anzahl der Tabellenspalten ist 9. Im Model sind jedoch 10 Attribute-Namen definiert.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: Fail
	@Test
	public void export_MissOneColumn_Fail() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk10(attr11 text, attr12 text, attr13 text, attr14 text, attr15 text, attr16 text, attr17 text, attr18 text, attr19 text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk10 (attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19) VALUES ('a_11', 'a_12', 'a_13', 'a_14', 'a_15', 'a_16', 'a_17', 'a_18', 'a_19')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_MissOneColumn_Fail.csv");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "modelExport10Columns");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk10");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertEquals("class attribute names [attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19] not found in given models [modelExport10Columns]", e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Die Anzahl der Tabellenspalten ist 11. Im Model sind jedoch nur 10 Attribute-Namen definiert.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: Fail
	@Test
	public void export_OneColumnTooMuch_Fail() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk10(attr11 text, attr12 text, attr13 text, attr14 text, attr15 text, attr16 text, attr17 text, attr18 text, attr19 text, attr20 text, attr21 text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk10 (attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19, attr20, attr21) VALUES ('a_11', 'a_12', 'a_13', 'a_14', 'a_15', 'a_16', 'a_17', 'a_18', 'a_19', 'a_20', 'a_21')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_OneColumnTooMuch_Fail.csv");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "modelExport10Columns");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk10");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertEquals("class attribute names [attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19, attr20, attr21] not found in given models [modelExport10Columns]", e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	
	// Die Anzahl der Model-Attribute-Namen ist 9. Es sind jedoch 10 Tabellenspalten in der Datenbank definiert.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: Fail
	@Test
	public void export_ModelMissOneColumn_Fail() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk10(attr11 text, attr12 text, attr13 text, attr14 text, attr15 text, attr16 text, attr17 text, attr18 text, attr19 text, attr20 text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk10 (attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19, attr20) VALUES ('a_11', 'a_12', 'a_13', 'a_14', 'a_15', 'a_16', 'a_17', 'a_18', 'a_19', 'a_20')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_ModelMissOneColumn_Fail.csv");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "MissOneColumn");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk10");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertEquals("class attribute names [attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19, attr20] not found in given models [MissOneColumn]",e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Die Anzahl der Model-Attribute-Namen ist 11, es sind jedoch nur 10 Tabellenspalten definiert.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: Fehler: class attribute names not found in given models.
	@Test
	public void export_OneModelAttrTooMuch_Fail() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk10(attr11 text, attr12 text, attr13 text, attr14 text, attr15 text, attr16 text, attr17 text, attr18 text, attr19 text, attr20 text) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk10 (attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19, attr20) VALUES ('a_11', 'a_12', 'a_13', 'a_14', 'a_15', 'a_16', 'a_17', 'a_18', 'a_19', 'a_20')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_OneModelAttrTooMuch_Fail.csv");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "OneColumnTooMuch");
			config.setValue(Config.SETTING_ILIDIRS, TEST_OUT);
			config.setValue(Config.DBSCHEMA, "dbtocsvschema");
			config.setValue(Config.TABLE, "csvexportnopk10");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertEquals("class attribute names [attr11, attr12, attr13, attr14, attr15, attr16, attr17, attr18, attr19, attr20] not found in given models [OneColumnTooMuch]",e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}