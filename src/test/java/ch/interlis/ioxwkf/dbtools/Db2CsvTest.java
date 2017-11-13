package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.iox.IoxException;
import ch.interlis.ioxwkf.dbtools.IoxWkfConfig;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
@Ignore("reorder columns to make tests work again")
public class Db2CsvTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private static final String TEST_OUT="build/test/data/DB2Csv/";
	
	@BeforeClass
	public static void setup() throws Ili2cFailure
	{
		new File(TEST_OUT).mkdirs();
	}
	// Es soll keine Fehlermeldung ausgegeben werden, 1 Reihe der Tabelle in eine Csv-Datei geschrieben wird.
	// - set: database-dbtocsvschema
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT+"export_SingleRow_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
			{
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_SingleRow_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname,state,abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("\"10\",\"Schweiz\",\"CH\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk10(attr11 character varying, attr12 character varying, attr13 character varying, attr14 character varying, attr15 character varying, attr16 character varying, attr17 character varying, attr18 character varying, attr19 character varying, attr20 character varying) WITH (OIDS=FALSE);");
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
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk10");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_10ColumnNames_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr14,attr15,attr16,attr17,attr11,attr12,attr13,attr20,attr18,attr19", line);
					}else if(lineIndex==2) {
						assertEquals("\"a_14\",\"a_15\",\"a_16\",\"a_17\",\"a_11\",\"a_12\",\"a_13\",\"a_20\",\"a_18\",\"a_19\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, "|");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_SetDelimiter_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname,state,abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("|10|,|Schweiz|,|CH|", line);
					}else {
						fail();
					}
				}
				br.close();
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, "|");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_SetRecordDelimiter_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname|state|abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("\"10\"|\"Schweiz\"|\"CH\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, "|");
				config.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, ":");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_SetDelimiterAndRecordDelimiter_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname:state:abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("|10|:|Schweiz|:|CH|", line);
					}else {
						fail();
					}
				}
				br.close();
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_VALUE);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_HeaderAbsent_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname,state,abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("\"10\",\"Schweiz\",\"CH\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
				File data=new File(TEST_OUT+"export_MultipleRows_Ok.csv");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_MultipleRows_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname,state,abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("\"10\",\"Schweiz\",\"CH\"", line);
					}else if(lineIndex==3) {
						assertEquals("\"11\",\"Deutschland\",\"DE\"", line);
					}else if(lineIndex==4) {
						assertEquals("\"12\",\"Frankreich\",\"FR\"", line);
					}else if(lineIndex==5) {
						assertEquals("\"13\",\"Italien\",\"IT\"", line);
					}else if(lineIndex==6) {
						assertEquals("\"14\",\"Spanien\",\"ES\"", line);
					}else if(lineIndex==7) {
						assertEquals("\"15\",\"Oesterreich\",\"AT\"", line);
					}else{
						fail();
					}
				}
				br.close();
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_ILIDIRS, TEST_OUT);
				// DBSCHEMA: "dbtocsvschema" not set
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultcsvexportnopk");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_SchemaNotSet_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname,state,abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("\"S_10\",\"S_Schweiz\",\"S_CH\"", line);
					}else {
						fail();
					}
				}
				br.close();
			}
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
	        	preStmt.execute("CREATE TABLE dbtocsvschema.defaultcsvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
			config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_FindTableInDefinedSchema_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname,state,abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("\"S_10\",\"S_Schweiz\",\"S_CH\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	preStmt.execute("DROP TABLE IF EXISTS defaultcsvexportnopk CASCADE");
	        	// create table
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
	        	preStmt.execute("CREATE TABLE dbtocsvschema.defaultcsvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
			config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
			// DBSCHEMA: "dbtocsvschema" not set
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			{
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_FindTableInDefaultSchema_Ok.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("idname,state,abbreviation", line);
					}else if(lineIndex==2) {
						assertEquals("\"D_10\",\"D_Schweiz\",\"D_CH\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeBigint.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeBigint.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"9223372036854775807\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeBoolean.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeBoolean.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"true\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeBit.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeBit.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"1\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeChar.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeChar.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"a\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeVarchar.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeVarchar.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"abc\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeDate.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeDate.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"2017-02-02\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeInteger.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeInteger.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"12\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeNumeric.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeNumeric.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"123\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	// Erwartung: SUCCESS: datatype=character varying
	@Test
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
	        	preStmt.execute("CREATE TABLE dbtocsvschema.exportdatatype(attr character varying) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('character varying')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"export_DataTypeText.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeText.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"character varying\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeTime.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeTime.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"10:10:59\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeSmallint.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeSmallint.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"5\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeTimestamp.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeTimestamp.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"2014-05-15T12:30:30.555\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
				File data=new File(TEST_OUT,"export_DataTypeUuid.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeUuid.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"123e4567-e89b-12d3-a456-426655440000\"", line);
					}else {
						fail();
					}
				}
				br.close();
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
	@Test
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
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.exportdatatype (attr) VALUES ('<attrText>character varying</attrText>')");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File(TEST_OUT,"export_DataTypeXml.csv");
				config.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Csv=new Db2Csv();
				db2Csv.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
				BufferedReader br = new BufferedReader(new FileReader(new File(TEST_OUT+"export_DataTypeXml.csv")));
				String line=null;
				int lineIndex=0;
				while((line=br.readLine())!= null) {
					lineIndex+=1;
					if(lineIndex==1) {
						assertEquals("attr", line);
					}else if(lineIndex==2) {
						assertEquals("\"<attrText>character varying</attrText>\"", line);
					}else {
						fail();
					}
				}
				br.close();
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet, ob connection=null zu einer IoxException fuehrt 
	@Test
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
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_TableInSchemaNotFound_Fail.csv");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema99999");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("db table"));
			assertTrue(e.getMessage().contains("csvexportnopk"));
			assertTrue(e.getMessage().contains("inside db schema"));
			assertTrue(e.getMessage().contains("dbtocsvschema99999"));
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_TableNotFound_Fail.csv");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk99999");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) { // db table <csvexportnopk99999> inside db schema <dbtocsvschema>: not found.
			assertTrue(e.getMessage().contains("db table"));
			assertTrue(e.getMessage().contains("csvexportnopk99999"));
			assertTrue(e.getMessage().contains("inside db schema"));
			assertTrue(e.getMessage().contains("dbtocsvschema"));
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
			assertTrue(e.getMessage().contains("database table==null."));
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
	        	// drop dbtocsvschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtocsvschema CASCADE");
	        	// create dbtocsvschema
	        	preStmt.execute("CREATE SCHEMA dbtocsvschema");
	        	// create table in dbtocsvschema
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
	        	// import data to table
	        	preStmt.executeUpdate("INSERT INTO dbtocsvschema.csvexportnopk (idname, abbreviation, state) VALUES ('10', 'CH', 'Schweiz')");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_OUT+"export_TableNotSet_Fail.csv");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
			// TABLE: "csvimportwithheader" not set
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("database table==null."));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Tabelle im gesetzten Schema zwar gefunden wird, jedoch keine Daten --> "NULL" beinhaltet.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: Exception. no data found to export.
	@Test
	public void export_DataContainsNullInTableInSchema_Fail() throws Exception
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
	        	preStmt.execute("CREATE TABLE dbtocsvschema.csvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtocsvschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "csvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("export of: <model.topic.csvexportnopk> to csv file: <D:\\GIT\\iox-wkf\\iox-wkf\\src\\test\\data\\DB2Csv\\export_DataContainsNullInTableInSchema_Ok.csv> failed."));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Tabelle innerhalb des Default-Schemas zwar gefunden wird, jedoch keine Daten --> "NULL" beinhaltet.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: database-dbtocsvschema
	// - set: database-table
	// --
	// Erwartung: Success: no data found to export.
	@Test
	public void export_DataContainsNullInTable_Fail() throws Exception
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
	        	preStmt.execute("CREATE TABLE defaultcsvexportnopk(idname character varying, abbreviation character varying, state character varying) WITH (OIDS=FALSE);");
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
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultcsvexportnopk");
			AbstractExportFromdb db2Csv=new Db2Csv();
			db2Csv.exportData(data, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("export of: <model.topic.defaultcsvexportnopk> to csv file: <D:\\GIT\\iox-wkf\\iox-wkf\\src\\test\\data\\DB2Csv\\export_DataContainsNullInTable_Ok.csv> failed."));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}