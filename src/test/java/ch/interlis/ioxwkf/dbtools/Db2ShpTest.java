package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.postgresql.util.PSQLException;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.iox.IoxException;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Db2ShpTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private static final String TEST_OUT="build/test/data/DB2Shp/";
	
	@BeforeClass
	public static void setup() throws Ili2cFailure
	{
		new File(TEST_OUT).mkdirs();
	}
	
	// Es soll keine Fehlermeldung ausgegeben werden, 1 Reihe der Tabelle in eine Shp-Datei geschrieben wird.
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.shpexport(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.shpexport(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_SingleRow_Ok.shp");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpexport");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_SingleRow_Ok.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "coord2d");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil auch ohne die Angabe des Datenbank Schemas, die Tabelle gefunden werden kann.
	// - NOT SET: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP TABLE IF EXISTS defaultshpexport CASCADE");
	        	// create table
	        	preStmt.execute("CREATE TABLE defaultshpexport(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO defaultshpexport(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_SchemaNotSet_Ok.shp");
				// delete file if already exist
				if(data.exists()) {
					data.delete();
				}
				
				config.setValue(IoxWkfConfig.SETTING_ILIDIRS, TEST_OUT);
				// DBSCHEMA: "dbtoshpschema" not set
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultshpexport");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_SchemaNotSet_Ok.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "coord2d");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	preStmt.execute("DROP TABLE IF EXISTS defaultshpexport CASCADE");
	        	// create table
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	preStmt.execute("CREATE TABLE dbtoshpschema.defaultshpexport(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.defaultshpexport(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
			// shp
			File data=new File(TEST_OUT,"export_FindTableInDefinedSchema_Ok.shp");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultshpexport");
			AbstractExportFromdb db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
			{
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_FindTableInDefinedSchema_Ok.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "coord2d");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	preStmt.execute("DROP TABLE IF EXISTS defaultshpexport CASCADE");
	        	// create table
	        	preStmt.execute("CREATE TABLE defaultshpexport(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO defaultshpexport(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
			// shp
			File data=new File(TEST_OUT,"export_FindTableInDefaultSchema_Ok.shp");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			
			// DBSCHEMA: "dbtoshpschema" not set
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultshpexport");
			AbstractExportFromdb db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
			{
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_FindTableInDefaultSchema_Ok.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "coord2d");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
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
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr bigint,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('9223372036854775807','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeBigint.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeBigint.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "9223372036854775807");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr boolean,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('true','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeBoolean.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeBoolean.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "true");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr bit,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('1','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeBit.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeBit.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "true");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp: Bit1 innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bit1
	@Test
	public void export_Datatype_Bit1_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr bit(1),the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES (B'1','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeBit1.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeBit1.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "true");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp: Bit3 innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bit3
	@Test
	public void export_Datatype_Bit3_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr bit(3),the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES (B'101','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeBit3.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeBit3.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "101");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('a','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeChar.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeChar.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "a");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('abc','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeVarchar.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeVarchar.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "abc");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr date,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('2017-02-15','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeDate.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeDate.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "2017-02-15");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob das Date auf das definierte Format geprueft wird und das Date im richtigen Format in die Shape-Datei geschrieben wird.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// - set: date-format
	// --
	// Erwartung: SUCCESS: datatype=date
	@Test
	public void export_Datatype_Date_DateFormat_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr date,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('2020-11-25','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeDate_DateFormat.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				config.setValue(IoxWkfConfig.SETTING_DATEFORMAT, "dd-MM-yyyy");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeDate_DateFormat.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "25-11-2020");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Der DatenType: attr wird mit dem Wert: NULL in der Datenbank erstellt.
	// Das SHP File darf somit keinen Wert mit dem Attribute-Namen: attr enthalten.
	// - set: database-dbtoshpschema
	// - set: database-exportdatatype
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_Datatype_Date_Null_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr date,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES (NULL,'0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeDate.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeDate.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					assertTrue(shapeObj.getAttribute("attr")==null);
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es werden 5 DatenTypen mit jeweils NULL Werte in der Datenbank erstellt.
	// Das SHP File darf somit keinen Wert enthalten, ist jedoch zulaessig.
	// - set: database-dbtoshpschema
	// - set: database-exportdatatype
	// --
	// Erwartung: SUCCESS.
	@Ignore //TODO no feature found in file.
	public void export_SomeDataTypes_NoValueDefined_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying, attr2 bit, attr3 numeric, attr4 date, the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,attr2,attr3,attr4,the_geom) VALUES (NULL,NULL,NULL,NULL,NULL)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeDateNull.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeDateNull.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		assertTrue(featureCollectionIter.hasNext());
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr integer,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('12','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeInteger.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeInteger.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "12");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr numeric,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('123','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeNumeric.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeNumeric.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "123");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('character varying','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeText.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeText.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "character varying");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr time,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('10:10:59','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeTime.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeTime.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "10:10:59");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob die Time auf das definierte Format geprueft wird und die Time im richtigen Format in die Shape-Datei geschrieben wird.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// - set: time-format
	// --
	// Erwartung: SUCCESS: datatype=time
	@Test
	public void export_Datatype_Time_TimeFormat_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr time,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('11:06:30.555','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeTime_TimeFormat.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				config.setValue(IoxWkfConfig.SETTING_TIMEFORMAT, "HH:mm:ss.SSS");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeTime_TimeFormat.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "11:06:30.555");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr smallint,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('5','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeSmallint.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeSmallint.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "5");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}	
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr timestamp,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('2014-05-15 12:30:30.555','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeTimestamp.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeTimestamp.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "2014-05-15T12:30:30.555");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der TimeStamp auf das definierte Format geprueft wird und den TimeStamp im richtigen Format in die Shape-Datei geschrieben wird.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// - set: dateTime-format
	// --
	// Erwartung: SUCCESS: datatype=timestamp
	@Test
	public void export_Datatype_Timestamp_TimeStampFormat_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr timestamp,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('1987-04-25T12:30:30','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeTimestamp_TimeStampFormat.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				config.setValue(IoxWkfConfig.SETTING_TIMESTAMPFORMAT, "dd-MM-yyyy HH:mm:ss");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeTimestamp_TimeStampFormat.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "25-04-1987 12:30:30");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr uuid,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('123e4567-e89b-12d3-a456-426655440000','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeUuid.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeUuid.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "123e4567-e89b-12d3-a456-426655440000");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr xml,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('<attrText>character varying</attrText>','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeXml.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeXml.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "<attrText>character varying</attrText>");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Tabelle im gesetzten Schema zwar gefunden wird, jedoch keine Daten --> "NULL" beinhaltet.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: Exception. no data found to export.
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(the_geom) VALUES ('0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
			// shp
			File data=new File(TEST_OUT,"export_DataContainsNullInTableInSchema_Ok.shp");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
			AbstractExportFromdb db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
			{
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataContainsNullInTableInSchema_Ok.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
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
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Tabelle innerhalb des Default-Schemas zwar gefunden wird, jedoch keine Daten --> "NULL" beinhaltet.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: Success: no data found to export.
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
	        	preStmt.execute("DROP TABLE IF EXISTS defaultshpexport CASCADE");
	        	// create table
	        	preStmt.execute("CREATE TABLE defaultshpexport(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO defaultshpexport(the_geom) VALUES ('0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
			// shp
			File data=new File(TEST_OUT,"export_DataContainsNullInTable_Ok.shp");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			// DBSCHEMA: "dbtoshpschema" not set
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultshpexport");
			AbstractExportFromdb db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
			{
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataContainsNullInTable_Ok.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
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
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=point2d
	@Test
	public void export_Datatype_Point2d_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,attr2 character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,attr2,the_geom) VALUES ('coord2d','attr2','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypePoint2d.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypePoint2d.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "coord2d");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "POINT (-0.2285714285714285 0.5688311688311687)");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=multipoint
	@Test
	public void export_Datatype_MultiPoint_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(MULTIPOINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('multicoord','0104000020080800000300000001010000001CD4411DD441CDBF0E69626CDD33E23F010100000074CFC8D2439AC8BF9E91A5873431E63F01010000006668E3AA7F40DFBFF094204F09F2D43F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeMultiPoint.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeMultiPoint.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "multicoord");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "MULTIPOINT ((-0.2285714285714285 0.5688311688311687), (-0.1922077922077922 0.6935064935064934), (-0.4883116883116884 0.3272727272727272))");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=linestring
	@Test
	public void export_Datatype_LineString_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(LINESTRING,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('linestring','010200002008080000020000001CD4411DD441CDBF0E69626CDD33E23F202A504A86DFCCBF8FFEA5F7491BE23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeLineString.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeLineString.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "linestring");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "MULTILINESTRING ((-0.2285714285714285 0.5688311688311687, -0.2255714285714285 0.5658311688311687))");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=multilinestring
	@Test
	public void export_Datatype_MultiLineString_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(MULTILINESTRING,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('multilinestring','010500002008080000020000000102000000020000001CD4411DD441CDBF0E69626CDD33E23F202A504A86DFCCBF8FFEA5F7491BE23F010200000002000000202A504A86DFCCBF8FFEA5F7491BE23FADA9EFBB6720CDBF981603D666C9E13F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeMultiLineString.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeMultiLineString.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "multilinestring");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "MULTILINESTRING ((-0.2285714285714285 0.5688311688311687, -0.2255714285714285 0.5658311688311687), (-0.2255714285714285 0.5658311688311687, -0.2275514285714285 0.5558351688311687))");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=polygon
	@Test
	public void export_Datatype_Polygon_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(POLYGON,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('polygon','01030000200808000001000000060000001CD4411DD441CDBF0E69626CDD33E23F26ABE58D114CC4BFB2D99F76B4D7E23F26ABE58D114CC4BFB2D99F76B4D7E23F26ABE58D114CC4BF0E69626CDD33E23F26ABE58D114CC4BF0E69626CDD33E23F1CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypePolygon.shp");
				
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypePolygon.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "polygon");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "MULTIPOLYGON (((-0.2285714285714285 0.5688311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5688311688311687, -0.1585714285714285 0.5688311688311687, -0.2285714285714285 0.5688311688311687)))");
	    		}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=multipolygon
	@Test
	public void export_Datatype_MultiPolygon_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(MULTIPOLYGON,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('multipolygon','0106000020080800000100000001030000000100000006000000C976BE9F1A2FCDBF931804560E2DE23FD34D62105839C4BF37894160E5D0E23FD34D62105839C4BF37894160E5D0E23FD34D62105839C4BF931804560E2DE23FD34D62105839C4BF931804560E2DE23FC976BE9F1A2FCDBF931804560E2DE23F')");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_OUT,"export_DataTypeMultiPolygon.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Shp=new Db2Shp();
				db2Shp.exportData(data, jdbcConnection, config);
			}
	        {
				//Open the file for reading
	        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"export_DataTypeMultiPolygon.shp"));
	        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
	    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
	    		if(featureCollectionIter.hasNext()) {
					// feature object
					SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
					Object attr1=shapeObj.getAttribute("attr");
					assertEquals(attr1.toString(), "multipolygon");
					Object attr2=shapeObj.getAttribute("the_geom");
					assertEquals(attr2.toString(), "MULTIPOLYGON (((-0.228 0.568, -0.158 0.588, -0.158 0.588, -0.158 0.568, -0.158 0.568, -0.228 0.568)))");
	    		}
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
			// shp
			File data=new File(TEST_OUT,"export_ConnectionFailed_Fail.shp");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpexport");
			Db2Shp db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
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
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: FEHLER: table dbtoshpschema.table .. not found
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
			// shp
			File data=new File(TEST_OUT,"export_TableInSchemaNotFound_Fail.shp");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema99999");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpexport");
			AbstractExportFromdb db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("db table"));
			assertTrue(e.getMessage().contains("shpexport"));
			assertTrue(e.getMessage().contains("inside db schema"));
			assertTrue(e.getMessage().contains("dbtoshpschema99999"));
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
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: FEHLER: table .. not found
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
			// shp
			File data=new File(TEST_OUT,"export_TableNotFound_Fail.shp");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpexport99999");
			AbstractExportFromdb db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) { // db table <shpexport99999> inside db schema <dbtoshpschema>: not found.
			assertTrue(e.getMessage().contains("db table"));
			assertTrue(e.getMessage().contains("shpexport99999"));
			assertTrue(e.getMessage().contains("inside db schema"));
			assertTrue(e.getMessage().contains("dbtoshpschema"));
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
	// - NOT SET: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        // shp
			File data=new File(TEST_OUT,"export_AllNotSet_Fail.shp");
			// DBSCHEMA: "dbtoshpschema" not set
			// TABLE: "csvimportwithheader" not set
			AbstractExportFromdb db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
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
	// - set: database-dbtoshpschema
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
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056)) WITH (OIDS=FALSE);");
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
			// shp
			File data=new File(TEST_OUT,"export_TableNotSet_Fail.shp");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoshpschema");
			// TABLE: "csvimportwithheader" not set
			AbstractExportFromdb db2Shp=new Db2Shp();
			db2Shp.exportData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("database table==null."));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Innerhalb der Datenbank wird ein bit3 erstellt. Der Wert der bit3 ist: B'10', was einer bit2 entspricht,
	// somit muss hier eine Fehlermeldung ausgegeben werden.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: ERROR: datatype=bit3
	@Test
	public void export_DatatypeBit3_ValueBit2_Fail() throws Exception {
		Settings config=new Settings();
		Connection jdbcConnection=null;
        Class driverClass = Class.forName("org.postgresql.Driver");
        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
        {
        	Statement preStmt=jdbcConnection.createStatement();
        	// drop dbtoshpschema
        	preStmt.execute("DROP SCHEMA IF EXISTS dbtoshpschema CASCADE");
        	// create dbtoshpschema
        	preStmt.execute("CREATE SCHEMA dbtoshpschema");
        	// create table in dbtoshpschema
        	preStmt.execute("CREATE TABLE dbtoshpschema.exportdatatype(attr bit(3)) WITH (OIDS=FALSE);");
	        try {
	        	preStmt.executeUpdate("INSERT INTO dbtoshpschema.exportdatatype (attr) VALUES (B'10')");
				fail();
			}catch(SQLException e) {
				assertEquals(PSQLException.class,e.getClass());
				assertEquals("22026", e.getSQLState());
			}
        	preStmt.close();
        }
	}
}