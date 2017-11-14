package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Ignore;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.ioxwkf.dbtools.AbstractImport2db;
import ch.interlis.ioxwkf.dbtools.IoxWkfConfig;
import ch.interlis.ioxwkf.dbtools.Shp2db;
import ch.interlis.ioxwkf.shp.ShapeWriter;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Shp2dbTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private Statement stmt=null;
	private static final String CREATEFILE_CLASSPATH="Test1.Topic1.Class1";
	private static final String TEST_IN="src/test/data/Shp2DB/";
	
	// Testet ob der Import eines Point's in die Datenbank funktioniert,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_Point_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname character varying,the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Point/Point.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12", rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.228571428571429 0.568831168831169)", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob der Import eines MultiPoint's in die Datenbank funktioniert,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void import_MultiPoint_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname character varying,the_geom geometry(MULTIPOINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "MultiPoint/MultiPoint.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12", rs.getObject(1));
				  	assertEquals("", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob der Import eines Linestring's in die Datenbank funktioniert,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	//@Test
	public void import_LineString_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname character varying,the_geom geometry(LINESTRING,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "LineString/LineString.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12", rs.getObject(1));
				  	assertEquals("SRID=2056;LINESTRING", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob der Import eines Multilinestring's in die Datenbank funktioniert,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_MultiLineString_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname character varying,the_geom geometry(MULTILINESTRING,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "MultiLineString/MultiLineString.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12", rs.getObject(1));
				  	assertEquals("SRID=2056;MULTILINESTRING((-0.228571428571429 0.568831168831169,-0.225571428571429 0.565831168831169),(-0.225571428571429 0.565831168831169,-0.227551428571429 0.555835168831169))", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob der Import eines Polygon's in die Datenbank funktioniert,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_Polygon_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname character varying,the_geom geometry(POLYGON,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Polygon/Polygon.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12", rs.getObject(1));
				  	assertEquals("SRID=2056;POLYGON((-0.228571428571429 0.568831168831169,-0.158571428571429 0.588831168831169,-0.158571428571429 0.588831168831169,-0.158571428571429 0.568831168831169,-0.158571428571429 0.568831168831169,-0.228571428571429 0.568831168831169))", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob der Import eines Multipolygon's in die Datenbank funktioniert,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	@Ignore("remove ch.interlis.ioxwkf.converter #10")
	public void import_MultiPolygon_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname character varying,the_geom geometry(MULTIPOLYGON,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "MultiPolygon/MultiPolygon.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(2, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12", rs.getObject(1));
				  	assertTrue(rs.getObject(2).equals("SRID=2056;MULTIPOLYGON(((-0.228 0.568,-0.158 0.588,-0.158 0.588,-0.158 0.568,-0.158 0.568,-0.228 0.568)))") ||
				  			(rs.getObject(2).equals("SRID=2056;MULTIPOLYGON(((0.228 1.3,0.158 0.5,0.158 0.5,0.158 1.568,0.158 1.568,0.228 1.3)))")));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Testet ob der Import eines Linestring's in die Datenbank funktioniert,
	// wenn das datenbank-schema nicht gesetzt wird und die Test-Konfiguration wie folgt gesetzt wird:
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SchemaNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop table
	        	preStmt.execute("DROP TABLE IF EXISTS shpimportnoschematable CASCADE");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shpimportnoschematable(idname character varying NOT NULL,the_geom geometry(POINT,2056),CONSTRAINT shpimporttable_pkey PRIMARY KEY (idname)) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Point/Point.shp");
				//config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimportnoschematable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shpimportnoschematable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT idname,st_asewkt(the_geom) FROM shpimportnoschematable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12", rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.228571428571429 0.568831168831169)", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der richtige Datentyp innerhalb der Datenbank-Tabelle importiert wurde.
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr bigint,the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Bigint/DataTypeBigInt.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals(Long.valueOf("123"), rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr boolean, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Bit/DataTypeBit.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals(true, rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr boolean, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Boolean/DataTypeBoolean.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals(true, rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr character, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Char/DataTypeChar.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("a", rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr character varying, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Varchar/DataTypeVarChar.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12345", rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr date, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Date/DataTypeDate.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("2017-10-20", rs.getString(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr integer, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Integer/DataTypeInteger.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals(12, rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr numeric, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Numeric/DataTypeNumeric.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals(java.math.BigDecimal.valueOf(Long.valueOf("12")), rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr character varying, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Text/DataTypeText.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("testtext", rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr time without time zone, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Time/DataTypeTime.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals(new java.sql.Time(10, 10, 11), rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr smallint, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Smallint/DataTypeSmallint.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals(1, rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr timestamp, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Timestamp/DataTypeTimestamp.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("2014-05-15 12:30:30.555", rs.getString(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr uuid, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Uuid/DataTypeUuid.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("123e4567-e89b-12d3-a456-426655440000", rs.getString(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr xml, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Xml/DataTypeXml.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertTrue(rs.getString(1).equals("<attrText>text</attrText>"));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}	
	
	// Testet ob der richtige Fehler ausgegeben wird, wenn die
	// Attribute der Tabelle innerhalb der Shp-Datei nicht gefunden werden kann.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
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
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr2 xml, the_geom2 geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
			// csv
			File data=new File(TEST_IN, "Attributes/Xml/DataTypeXml.shp");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("data base attribute names:"));
			assertTrue(e.getMessage().contains("the_geom"));
			assertTrue(e.getMessage().contains("attr"));
			assertTrue(e.getMessage().contains("not found in"));
			assertTrue(e.getMessage().contains("DataTypeXml.shp"));
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
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = null;
	        // shp
			File data=new File(TEST_IN, "Attributes/Boolean/DataTypeBoolean.shp");
			Shp2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertEquals(IoxException.class,e.getClass());
			assertEquals("connection==null.",e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob die Fehlermeldung: expected tablename ausgegeben wird,
	// wenn keine Parameter gesetzt sind.
	// --
	// - NOT SET: database-schema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	@Test
	public void import_AllNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        // shp
			File data=new File(TEST_IN, "Attributes/Boolean/DataTypeBoolean.shp");
			// DBSCHEMA: "csvtodbschema" not set
			// TABLE: "csvimportwithheader" not set
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
			fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("database table==null."));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob die Fehlermeldung: expected tablename ausgegeben wird,
	// wenn die Datenbank-Tabelle nicht als Parameter gesetzt wird.
	// --
	// - set: database-schema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	@Test
	public void import_TableNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        // shp
			File data=new File(TEST_IN, "Attributes/Boolean/DataTypeBoolean.shp");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
			// TABLE: "shpimporttable"
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("database table==null."));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob die Fehlermeldung: shapefile .. not found ausgegeben wird,
	// wenn die gesetzte shp Datei nicht existiert.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: shapefile ... not found
	@Test
	public void import_ShpFileNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        // shp
			File data=new File(TEST_IN, "NotExist/testPointAttrs.shp");
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("file: D:\\GIT\\iox-wkf\\iox-wkf\\src\\test\\data\\Shp2DB\\NotExist\\testPointAttrs.shp not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Testet ob die Fehlermeldung: import failed ausgegeben wird,
	// wenn die id als primary key in der Datenbank-Tabelle gesetzt wurde,
	// und die gleichen Daten in die Spalte id importiert werden soll.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: import failed.
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
	        	preStmt.execute("DROP TABLE IF EXISTS shpimportunique CASCADE");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shpimportunique(attr boolean NOT NULL,the_geom geometry(POINT,2056),CONSTRAINT shpimportunique_pkey PRIMARY KEY (attr)) WITH (OIDS=FALSE)");
	        	preStmt.executeUpdate("INSERT INTO shpimportunique(attr,the_geom) VALUES ('TRUE','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
		        // shp
				File data=new File(TEST_IN, "Attributes/Boolean/DataTypeBoolean.shp");
				// DBSCHEMA: "shptodbschema"
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimportunique");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
				fail();
			}
		}catch(IoxException e) {
			System.out.println(e.getMessage());
			assertTrue(e.getCause() instanceof SQLException);
			// unique violation.
			assertEquals("23505",((SQLException) e.getCause()).getSQLState());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// create shp file to test attribute-types.
	//@Test
	public void datatype_bigint_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "123");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Bigint/DataTypeBigInt.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_boolean_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "true");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Boolean/DataTypeBoolean.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_bit_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "1");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Bit/DataTypeBit.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_char_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "a");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Char/DataTypeChar.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_varchar_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "12345");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Varchar/DataTypeVarChar.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_date_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "2017-10-20");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Date/DataTypeDate.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_integer_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "12");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Integer/DataTypeInteger.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_numeric_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "12");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Numeric/DataTypeNumeric.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_text_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "testtext");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Text/DataTypeText.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_time_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "10:10:11");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Time/DataTypeTime.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_smallint_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "1");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Smallint/DataTypeSmallint.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_timestamp_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "2014-05-15T12:30:30.555000000");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Timestamp/DataTypeTimestamp.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_uuid_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "123e4567-e89b-12d3-a456-426655440000");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Uuid/DataTypeUuid.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_xml_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		inputObj.setattrvalue("attr", "<attrText>text</attrText>");
		IomObject coordValue=inputObj.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Attributes/Xml/DataTypeXml.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_point_Ok() throws IoxException, IOException{
		Iom_jObject objPointSuccess=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		objPointSuccess.setattrvalue("idname", "12");
		IomObject coordValue=objPointSuccess.addattrobj("the_geom", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Point/Point.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objPointSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_multiPoint_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objMultiPointSuccess=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		objMultiPointSuccess.setattrvalue("idname", "12");
		@SuppressWarnings("deprecation")
		IomObject multiCoordValue=objMultiPointSuccess.addattrobj("attrMPoint", "MULTICOORD");
		@SuppressWarnings("deprecation")
		IomObject coordValue1=multiCoordValue.addattrobj("coord", "COORD");
		coordValue1.setattrvalue("C1", "-0.22857142857142854");
		coordValue1.setattrvalue("C2", "0.5688311688311687");
		
		@SuppressWarnings("deprecation")
		IomObject coordValue2=multiCoordValue.addattrobj("coord", "COORD");
		coordValue2.setattrvalue("C1", "-0.19220779220779216");
		coordValue2.setattrvalue("C2", "0.6935064935064934");
		
		@SuppressWarnings("deprecation")
		IomObject coordValue3=multiCoordValue.addattrobj("coord", "COORD");
		coordValue3.setattrvalue("C1", "-0.48831168831168836");
		coordValue3.setattrvalue("C2", "0.32727272727272716");
		
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "MultiPoint/MultiPoint.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objMultiPointSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_lineString_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objPolylineSuccess=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		objPolylineSuccess.setattrvalue("idname", "12");
		IomObject polylineValue=objPolylineSuccess.addattrobj("attrLineString", "POLYLINE");
		IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
		IomObject coordStart=segments.addattrobj("segment", "COORD");
		IomObject coordEnd=segments.addattrobj("segment", "COORD");
		coordStart.setattrvalue("C1", "-0.22857142857142854");
		coordStart.setattrvalue("C2", "0.5688311688311687");
		coordEnd.setattrvalue("C1", "-0.22557142857142853");
		coordEnd.setattrvalue("C2", "0.5658311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "LineString/LineString.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objPolylineSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_multiLineString_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objMultiPolylineSuccess=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		objMultiPolylineSuccess.setattrvalue("idname", "12");
		IomObject multiPolylineValue=objMultiPolylineSuccess.addattrobj("attrMLineString", "MULTIPOLYLINE");
		
		IomObject polylineValue=multiPolylineValue.addattrobj("polyline", "POLYLINE");
		IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
		IomObject coordStart=segments.addattrobj("segment", "COORD");
		IomObject coordEnd=segments.addattrobj("segment", "COORD");
		coordStart.setattrvalue("C1", "-0.22857142857142854");
		coordStart.setattrvalue("C2", "0.5688311688311687");
		coordEnd.setattrvalue("C1", "-0.22557142857142853");
		coordEnd.setattrvalue("C2", "0.5658311688311687");
		
		IomObject polylineValue2=multiPolylineValue.addattrobj("polyline", "POLYLINE");
		IomObject segments2=polylineValue2.addattrobj("sequence", "SEGMENTS");
		IomObject coordStart2=segments2.addattrobj("segment", "COORD");
		IomObject coordEnd2=segments2.addattrobj("segment", "COORD");
		coordStart2.setattrvalue("C1", "-0.22557142857142853");
		coordStart2.setattrvalue("C2", "0.5658311688311687");
		coordEnd2.setattrvalue("C1", "-0.22755142857142853");
		coordEnd2.setattrvalue("C2", "0.5558351688311687");
		
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "MultiLineString/MultiLineString.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objMultiPolylineSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_polygon_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objSurfaceSuccess=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		objSurfaceSuccess.setattrvalue("idname", "12");
		IomObject multisurfaceValue=objSurfaceSuccess.addattrobj("attrPolygon", "MULTISURFACE");
		IomObject surfaceValue = multisurfaceValue.addattrobj("surface", "SURFACE");
		IomObject outerBoundary = surfaceValue.addattrobj("boundary", "BOUNDARY");
		// polyline
		IomObject polylineValue = outerBoundary.addattrobj("polyline", "POLYLINE");
		IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
		IomObject startSegment=segments.addattrobj("segment", "COORD");
		startSegment.setattrvalue("C1", "-0.22857142857142854");
		startSegment.setattrvalue("C2", "0.5688311688311687");
		IomObject endSegment=segments.addattrobj("segment", "COORD");
		endSegment.setattrvalue("C1", "-0.15857142857142854");
		endSegment.setattrvalue("C2", "0.5688311688311687");
		// polyline 2
		IomObject polylineValue2 = outerBoundary.addattrobj("polyline", "POLYLINE");
		IomObject segments2=polylineValue2.addattrobj("sequence", "SEGMENTS");
		IomObject startSegment2=segments2.addattrobj("segment", "COORD");
		startSegment2.setattrvalue("C1", "-0.15857142857142854");
		startSegment2.setattrvalue("C2", "0.5688311688311687");
		IomObject endSegment2=segments2.addattrobj("segment", "COORD");
		endSegment2.setattrvalue("C1", "-0.15857142857142854");
		endSegment2.setattrvalue("C2", "0.5888311688311687");
		// polyline 3
		IomObject polylineValue3 = outerBoundary.addattrobj("polyline", "POLYLINE");
		IomObject segments3=polylineValue3.addattrobj("sequence", "SEGMENTS");
		IomObject startSegment3=segments3.addattrobj("segment", "COORD");
		startSegment3.setattrvalue("C1", "-0.15857142857142854");
		startSegment3.setattrvalue("C2", "0.5888311688311687");
		IomObject endSegment3=segments3.addattrobj("segment", "COORD");
		endSegment3.setattrvalue("C1", "-0.22857142857142854");
		endSegment3.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "Polygon/Polygon.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
	
	//@Test
	public void datatype_multiPolygon_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objMultiSurfaceSuccess=new Iom_jObject(CREATEFILE_CLASSPATH, "o1");
		objMultiSurfaceSuccess.setattrvalue("idname", "12");
		IomObject multisurfaceValue=objMultiSurfaceSuccess.addattrobj("attrMultiPolygon", "MULTISURFACE");
		IomObject surfaceValue = multisurfaceValue.addattrobj("surface", "SURFACE");
		{
			IomObject outerBoundary = surfaceValue.addattrobj("boundary", "BOUNDARY");
			// polyline
			IomObject polylineValue = outerBoundary.addattrobj("polyline", "POLYLINE");
			IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
			IomObject startSegment=segments.addattrobj("segment", "COORD");
			startSegment.setattrvalue("C1", "-0.228");
			startSegment.setattrvalue("C2", "0.568");
			IomObject endSegment=segments.addattrobj("segment", "COORD");
			endSegment.setattrvalue("C1", "-0.158");
			endSegment.setattrvalue("C2", "0.568");
			// polyline 2
			IomObject polylineValue2 = outerBoundary.addattrobj("polyline", "POLYLINE");
			IomObject segments2=polylineValue2.addattrobj("sequence", "SEGMENTS");
			IomObject startSegment2=segments2.addattrobj("segment", "COORD");
			startSegment2.setattrvalue("C1", "-0.158");
			startSegment2.setattrvalue("C2", "0.568");
			IomObject endSegment2=segments2.addattrobj("segment", "COORD");
			endSegment2.setattrvalue("C1", "-0.158");
			endSegment2.setattrvalue("C2", "0.588");
			// polyline 3
			IomObject polylineValue3 = outerBoundary.addattrobj("polyline", "POLYLINE");
			IomObject segments3=polylineValue3.addattrobj("sequence", "SEGMENTS");
			IomObject startSegment3=segments3.addattrobj("segment", "COORD");
			startSegment3.setattrvalue("C1", "-0.158");
			startSegment3.setattrvalue("C2", "0.588");
			IomObject endSegment3=segments3.addattrobj("segment", "COORD");
			endSegment3.setattrvalue("C1", "-0.228");
			endSegment3.setattrvalue("C2", "0.568");
		}
		
		IomObject surfaceValue2 = multisurfaceValue.addattrobj("surface", "SURFACE");
		{
			IomObject outerBoundary = surfaceValue2.addattrobj("boundary", "BOUNDARY");
			// polyline
			IomObject polylineValue = outerBoundary.addattrobj("polyline", "POLYLINE");
			IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
			IomObject startSegment=segments.addattrobj("segment", "COORD");
			startSegment.setattrvalue("C1", "0.228");
			startSegment.setattrvalue("C2", "1.300");
			IomObject endSegment=segments.addattrobj("segment", "COORD");
			endSegment.setattrvalue("C1", "0.158");
			endSegment.setattrvalue("C2", "1.568");
			// polyline 2
			IomObject polylineValue2 = outerBoundary.addattrobj("polyline", "POLYLINE");
			IomObject segments2=polylineValue2.addattrobj("sequence", "SEGMENTS");
			IomObject startSegment2=segments2.addattrobj("segment", "COORD");
			startSegment2.setattrvalue("C1", "0.158");
			startSegment2.setattrvalue("C2", "1.568");
			IomObject endSegment2=segments2.addattrobj("segment", "COORD");
			endSegment2.setattrvalue("C1", "0.158");
			endSegment2.setattrvalue("C2", "0.500");
			// polyline 3
			IomObject polylineValue3 = outerBoundary.addattrobj("polyline", "POLYLINE");
			IomObject segments3=polylineValue3.addattrobj("sequence", "SEGMENTS");
			IomObject startSegment3=segments3.addattrobj("segment", "COORD");
			startSegment3.setattrvalue("C1", "0.158");
			startSegment3.setattrvalue("C2", "0.500");
			IomObject endSegment3=segments3.addattrobj("segment", "COORD");
			endSegment3.setattrvalue("C1", "0.228");
			endSegment3.setattrvalue("C2", "1.300");
		}
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_IN, "MultiPolygon/MultiPolygon.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objMultiSurfaceSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	}
}