package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import ch.interlis.ioxwkf.dbtools.AbstractImport2db;
import ch.interlis.ioxwkf.dbtools.Config;
import ch.interlis.ioxwkf.dbtools.Csv2db;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Csv2dbTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private Statement stmt=null;
	private static final String ROW="row";
	private Map<String, List<String>> rows=null;
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-present
	// - set: model
	// - set: model-path
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
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportwithheadernopk(id text,abbreviation text,state text) WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "model2");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportwithheadernopk");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String id=null;
				String abbreviation=null;
				String state=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportwithheadernopk.id, csvtodbschema.csvimportwithheadernopk.abbreviation, csvtodbschema.csvimportwithheadernopk.state FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("20")) {
						assertTrue(rowValue.get(1).equals("AU"));
						assertTrue(rowValue.get(2).equals("Deutschland"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: header-present
	// - set: model
	// - set: model-path
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
	        	preStmt.execute("CREATE TABLE csvimportnopkcolumn(attr1 text,attr2 text,attr3 text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				// HEADER: HEADERPRESENT, HEADERABSENT not set
				config.setValue(Config.SETTING_MODELNAMES, "modelImport");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
				// DBSCHEMA: "csvtodbschema" not set
				config.setValue(Config.TABLE, "csvimportnopkcolumn");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attr1=null;
				String attr2=null;
				String attr3=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT public.csvimportnopkcolumn.attr1, public.csvimportnopkcolumn.attr2, public.csvimportnopkcolumn.attr3 FROM public.csvimportnopkcolumn");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					attr1 = rs.getString(1);
					attr2 = rs.getString(2);
					attr3= rs.getString(3);
					row.add(attr1);
					row.add(attr2);
					row.add(attr3);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean dataFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("25") && rowValue.get(1).equals("AU") && rowValue.get(2).equals("Holland")) {
				  		dataFound=true;
				  	}
				}
				if(!dataFound) {
					dataFound=false;
					fail();
				}
				dataFound=false;
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
			
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: header-present
	// - NOT SET: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_HeaderAndPathNotSet_ModelNotSet_Ok() throws Exception
	{
		String attr1=null;
		String attr2=null;
		String attr3=null;
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
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportnopk(attr1 text,attr2 text,attr3 text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				config.setValue(Config.HEADER, Config.HEADERABSENT);
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportnopk");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportnopk.attr1, csvtodbschema.csvimportnopk.attr2, csvtodbschema.csvimportnopk.attr3 FROM "+config.getValue(Config.DBSCHEMA)+".csvimportnopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					attr1 = rs.getString(1);
					attr2 = rs.getString(2);
					attr3= rs.getString(3);
					row.add(attr1);
					row.add(attr2);
					row.add(attr3);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean attrValuesFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("25")&&rowValue.get(1).equals("AU")&&rowValue.get(2).equals("Holland")){
				  		attrValuesFound=true;
				  	}
				}
				if(!attrValuesFound) {
					attrValuesFound=false;
					fail();
				}
				attrValuesFound=false;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-present
	// - set: model
	// - set: model-path
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SchemaNotSet_Ok() throws Exception
	{
		String id=null;
		String abbreviation=null;
		String state=null;
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
	        	preStmt.execute("CREATE TABLE csvimportwithheader(id character varying NOT NULL,abbreviation character varying,state character varying,CONSTRAINT csvimportwithheader_pkey PRIMARY KEY (id)) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "model2");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
				// DBSCHEMA: not set "csvtodbschema"
				config.setValue(Config.TABLE, "csvimportwithheader");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvimportwithheader.id, csvimportwithheader.abbreviation, csvimportwithheader.state FROM csvimportwithheader");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean attrValuesFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("20")&&rowValue.get(1).equals("AU")&&rowValue.get(2).equals("Deutschland")){
				  		attrValuesFound=true;
				  	}
				}
				if(!attrValuesFound) {
					attrValuesFound=false;
					fail();
				}
				attrValuesFound=false;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-present
	// - set: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_PathNotSet_Ok() throws Exception
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
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportwithheadernopk(id text,abbreviation text,state text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "model2");
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set.
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportwithheadernopk");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String id=null;
				String abbreviation=null;
				String state=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvimportwithheadernopk.id, csvimportwithheadernopk.abbreviation, csvimportwithheadernopk.state FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("20")) {
						assertTrue(rowValue.get(1).equals("AU"));
						assertTrue(rowValue.get(2).equals("Deutschland"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-present
	// - NOT SET: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_ModelAndPathNotSet_Ok() throws Exception
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
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportwithheadernopk(id text,abbreviation text,state text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportwithheadernopk");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				String id=null;
				String abbreviation=null;
				String state=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportwithheadernopk.id, csvtodbschema.csvimportwithheadernopk.abbreviation, csvtodbschema.csvimportwithheadernopk.state FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("20")) {
						assertTrue(rowValue.get(1).equals("AU"));
						assertTrue(rowValue.get(2).equals("Deutschland"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-absent
	// - NOT SET: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_ModelAndPathNotSet_HeaderSetAbsent_Ok() throws Exception
	{
		String id=null;
		String abbreviation=null;
		String state=null;
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
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportnopk(attr1 text,attr2 text,attr3 text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				config.setValue(Config.HEADER, Config.HEADERABSENT);
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportnopk");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportnopk.attr1, csvtodbschema.csvimportnopk.attr2, csvtodbschema.csvimportnopk.attr3 FROM csvtodbschema.csvimportnopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean attrValuesFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("25")&&rowValue.get(1).equals("AU")&&rowValue.get(2).equals("Holland")){
				  		attrValuesFound=true;
				  	}
				}
				if(!attrValuesFound) {
					attrValuesFound=false;
					fail();
				}
				attrValuesFound=false;
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: header-present
	// - NOT SET: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_ModelAndPathNotSet_HeaderNotSet_Ok() throws Exception
	{
		String id=null;
		String abbreviation=null;
		String state=null;
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
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportnopk(attr1 text,attr2 text,attr3 text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				// HEADER: Config.HEADERABSENT not set
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportnopk");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportnopk.attr1, csvtodbschema.csvimportnopk.attr2, csvtodbschema.csvimportnopk.attr3 FROM csvtodbschema.csvimportnopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	assertTrue(rowValue.get(0).equals("25"));
					assertTrue(rowValue.get(1).equals("AU"));
					assertTrue(rowValue.get(2).equals("Holland"));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-present
	// - NOT SET: model
	// - NOT SET: model-path
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_ModelAndPathNotSet_HeaderNotSet_SchemaNotSet_Ok() throws Exception
	{
		String id=null;
		String abbreviation=null;
		String state=null;
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
	        	preStmt.execute("CREATE TABLE csvimportnopkcolumn(attr1 text,attr2 text,attr3 text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent2.csv");
				// HEADER: Config.HEADERABSENT not set
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				// DBSCHEMA: "csvtodbschema" not set
				config.setValue(Config.TABLE, "csvimportnopkcolumn");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT public.csvimportnopkcolumn.attr1, public.csvimportnopkcolumn.attr2, public.csvimportnopkcolumn.attr3 FROM public.csvimportnopkcolumn");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean attrValuesFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("25")&&rowValue.get(1).equals("EU")&&rowValue.get(2).equals("Europa")){
				  		attrValuesFound=true;
				  	}
				}
				if(!attrValuesFound) {
					attrValuesFound=false;
					fail();
				}
				attrValuesFound=false;
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird eine Csv-Datei gelesen, welche die folgenden Attribute beinhaltet:
	// - idname
	// - textname
	// - doublename
	// - the_geom
	// --
	// Nun werden die Attribute-Werte, nach den Attribute-Namen welche im Model definiert sind,
	// aus der Csv-Datei herausgelesen:
	// - idname
	// - textname
	// - doublename
	// - the_geom
	// --
	// Von diesen Attributen werden nun Einschraenkungen durch das Datenbank-Schema gemacht
	// und die folgenden Daten in die Datenbank-Tabelle importiert:
	// - textname
	// - the_geom
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-->present
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_LimitedSelection_Ok() throws Exception{
		{
			// reader test
			CsvReader reader=null;
			TransferDescription tdM=null;
			Configuration ili2cConfig=new Configuration();
			FileEntry fileEntryConditionClass=new FileEntry("src/test/data/Csv2DB/CsvModelAttributesLimited.ili", FileEntryKind.ILIMODELFILE);
			ili2cConfig.addFileEntry(fileEntryConditionClass);
			tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
			assertNotNull(tdM);
			try {
				reader=new CsvReader(new File("src/test/data/Csv2DB/AttributesLimited.csv"));
				reader.setHeader(Config.HEADERPRESENT);
				reader.setModel(tdM);
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertTrue(iomObj.getattrcount()==4);
		        	assertTrue(iomObj.getattrvalue("doublename").equals("54321"));
		        	assertTrue(iomObj.getattrvalue("idname").equals("1"));
		        	assertTrue(iomObj.getattrvalue("textname").equals("text1"));
		        	assertTrue(iomObj.getattrvalue("the_geom").equals("COORD {C1 -0.5332351148239034, C2 0.7382312503416462}"));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
			}finally {
				if(reader!=null) {
			    	reader.close();
					reader=null;
		    	}
			}
		}
		{
			// import test
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
		        	preStmt.execute("CREATE TABLE csvtodbschema.csvimporttablelimited(textname text,the_geom text)WITH (OIDS=FALSE)");
		        	preStmt.close();
		        }
		        {
					// shp
					File data=new File("src/test/data/Csv2DB/AttributesLimited.csv");
					config.setValue(Config.HEADER, Config.HEADERPRESENT);
					config.setValue(Config.SETTING_MODELNAMES, "CsvModelAttributesLimited");
					config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
					config.setValue(Config.DBSCHEMA, "csvtodbschema");
					config.setValue(Config.TABLE, "csvimporttablelimited");
					AbstractImport2db csv2db=new Csv2db();
					csv2db.importData(data, jdbcConnection, config);
				}
				{
					rows = new HashMap<String, List<String>>();
					stmt=jdbcConnection.createStatement();
					ResultSet rs = stmt.executeQuery("SELECT * FROM csvtodbschema.csvimporttablelimited");
					ResultSetMetaData rsmd = rs.getMetaData();
					int rowCount=rs.getRow();
					while(rs.next()){
						List<String> row = new ArrayList<String>();
						String textValue = rs.getString(1);
						String theGeomValue= rs.getString(2);
						row.add(textValue);
						row.add(theGeomValue);
						rows.put(ROW+String.valueOf(rowCount), row);
						rowCount+=1;
					}
					for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
					  	List<String> rowValue = entry.getValue();
					  	if(rowValue.get(0).equals("text1")) {
							assertTrue(rowValue.get(1).equals("COORD {C1 -0.5332351148239034, C2 0.7382312503416462}"));
							break;
					  	}
					}
				}
			}finally{
				if(jdbcConnection!=null){
					jdbcConnection.close();
				}
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Spalten-Namen (Attribute-Namen) der gesuchten Datenbank-Tabelle,
	// welche sich innerhalb des gesuchten Datenbank-Schemas befindet,
	// weder in den gesetzten Modellen, noch in der Csv-Datei gefunden werden koennen.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: header-->present
	// - set: model
	// - set: model-path
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
			File data=new File("src/test/data/Csv2DB/AttributesHeader3.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model3");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("data base attribute names: [id, abbreviation, state] not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet, ob connection=null zu einer IoxException fuehrt 
	@Test
	public void import_ConnectionFailed_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        jdbcConnection = null;
			// csv
			File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
			Csv2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
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
	
	// import test
	// - header not set
	// - model not set
	// - model not path set
	// - db schema not set
	// - db table not set
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
			File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
			// HEADER: HEADERPRESENT, HEADERABSENT not set
			// SETTING_MODELNAMES: "model2" not set
			// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
			// DBSCHEMA: "csvtodbschema" not set
			// TABLE: "csvimportwithheader" not set
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("expected tablename"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table not set
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
			File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			// TABLE: "csvimportwithheader" not set
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("expected tablename"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// attributes of ili model not found in csv file
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table set
	@Test
	public void import_AttrNamesNotFoundInModel_Fail() throws Exception
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
			File data=new File("src/test/data/Csv2DB/AttributesHeader3.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("attributes of headerrecord: [name, lastname, planet] not found in iliModel: model2"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// csv file not found
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table set
	// - csv file not found
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
	        	preStmt.execute("CREATE TABLE csvtodbschema.csvimportwithheader(id character varying NOT NULL,abbreviation character varying,state character varying,CONSTRAINT csvimportwithheader_pkey PRIMARY KEY (id)) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        // csv
			File data=new File("src/test/data/NotExist/AttributesHeader.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/NotExist");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header present
	// - model not found
	// - model path set
	// - db schema set
	// - db table set
	@Test
	public void import_ModelNotFound_Fail() throws Exception
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
			File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "modelNotFound");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			AbstractImport2db csv2db=new Csv2db();
			csv2db.importData(data, jdbcConnection, config);
	    	fail();
			}catch(IoxException e) {
				assertTrue(e.getMessage().contains("models [modelNotFound] not found"));
			}finally{
				if(jdbcConnection!=null){
					jdbcConnection.close();
				}
			}
	}
	
	// import test
	// - header not set
	// - model set
	// - model path set
	// - db schema not set
	// - db table set
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
	        	preStmt.execute("CREATE TABLE csvimport(attr1 character varying NOT NULL,attr2 character varying,attr3 character varying,CONSTRAINT csvimport_pkey PRIMARY KEY (attr1))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				// HEADER: HEADERPRESENT, HEADERABSENT not set
				config.setValue(Config.SETTING_MODELNAMES, "model2");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
				// DBSCHEMA: "csvtodbschema" not set
				config.setValue(Config.TABLE, "csvimport");
				AbstractImport2db csv2db=new Csv2db();
				csv2db.importData(data, jdbcConnection, config);
				csv2db.importData(data, jdbcConnection, config);
				fail();
			}
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("import of model2.Topic12.Class1 oid o2 {attr2 AU, attr1 25, attr3 Holland} to csvimport failed"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}