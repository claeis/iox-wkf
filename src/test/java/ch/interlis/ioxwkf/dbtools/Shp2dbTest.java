package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.ioxwkf.dbtools.AbstractImport2db;
import ch.interlis.ioxwkf.dbtools.IoxWkfConfig;
import ch.interlis.ioxwkf.dbtools.Shp2db;

import org.testcontainers.containers.PostgisContainerProvider;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class Shp2dbTest {
	private Statement stmt=null;
	private static final String TEST_IN="src/test/data/Shp2DB/";
	
    static String WAIT_PATTERN = ".*database system is ready to accept connections.*\\s";
	
    @ClassRule
    public static PostgreSQLContainer postgres = 
        (PostgreSQLContainer) new PostgisContainerProvider()
        .newInstance().withDatabaseName("ioxwkf")
        .waitingFor(Wait.forLogMessage(WAIT_PATTERN, 2));
    	
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname character varying,geom geometry(POINT,2056))WITH (OIDS=FALSE)");
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
				ResultSet rs = stmt.executeQuery("SELECT idname,st_asewkt(geom) FROM shptodbschema.shpimporttable;");
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
	@Test
	public void import_MultiPoint_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
				  	assertEquals("SRID=2056;MULTIPOINT(-0.228571428571429 0.568831168831169,-0.192207792207792 0.693506493506493,-0.488311688311688 0.327272727272727)", rs.getObject(2));
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
	@Test
	public void import_LineString_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
				  	assertEquals("SRID=2056;LINESTRING(-0.228571428571429 0.568831168831169,-0.225571428571429 0.565831168831169)", rs.getObject(2));
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
				  	assertEquals("SRID=2056;POLYGON((-0.228571428571429 0.568831168831169,-0.158571428571429 0.588831168831169,-0.158571428571429 0.568831168831169,-0.228571428571429 0.568831168831169))", rs.getObject(2));
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
	public void import_MultiPolygon_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
				  	assertTrue(rs.getObject(2).equals("SRID=2056;MULTIPOLYGON(((-0.228 0.568,-0.158 0.588,-0.158 0.568,-0.228 0.568)))") ||
				  			(rs.getObject(2).equals("SRID=2056;MULTIPOLYGON(((0.228 1.3,0.158 0.5,0.158 1.568,0.228 1.3)))")));
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
	@Test
	public void import_attrName_similar_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(\"Attr\" character, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
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
				ResultSet rs = stmt.executeQuery("SELECT \"Attr\",st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
				// default: yyyy.MM.dd
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
	
	// Es wird getestet, ob das Date auf das definierte Format geprueft wird und das Date im richtigen Format in die Datenbank schreibt.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// - set: date-format
	// --
	// Erwartung: SUCCESS: datatype=date
	@Test
	public void import_Datatype_Date_DefinedFormat_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr1 date, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Date/DataTypeDate2.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				config.setValue(IoxWkfConfig.SETTING_DATEFORMAT, "yyyy-MM-dd");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr1,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
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
	
	// Wenn das Datum: 2017-Dez-20 erstellt wird, so muss innerhalb der DB, 2017-12-20 importiert sein.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// - set: date-format
	// --
	// Erwartung: SUCCESS: datatype=date
	@Test
	@Ignore("import must respect reader data type (should not try to parse valid xtfDates")
	public void import_Datatype_Date_DefinedFormatTextToNum_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		java.text.DateFormatSymbols symbols=new java.text.DateFormatSymbols();
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr1 date, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Date/DataTypeDate3.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				config.setValue(IoxWkfConfig.SETTING_DATEFORMAT, "yyyy-MMM-dd");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr1,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("2017-12-20", rs.getString(1));
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
					assertEquals(java.sql.Time.valueOf("20:30:59"), rs.getObject(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob die Time auf das definierte Format geprueft wird und die Time im richtigen Format in die Datenbank geschrieben wird.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// - set: date-format
	// --
	// Erwartung: SUCCESS: datatype=time
	@Test
	public void import_Datatype_Time_DefinedFormat_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr1 time without time zone, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Time/DataTypeTime2.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				config.setValue(IoxWkfConfig.SETTING_TIMEFORMAT, "HH:mm:ss.SSS");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr1,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("12:08:56.235", rs.getString(1));
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr1 timestamp, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
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
				ResultSet rs = stmt.executeQuery("SELECT attr1,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("2001-07-04 12:08:56.235", rs.getString(1));
				  	assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(2));
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob der TimeStamp auf das definierte Format geprueft wird und den TimeStamp im richtigen Format in die Datenbank schreibt.
	// - set: header-present
	// - set: database-schema
	// - set: database-table
	// - set: date-format
	// --
	// Erwartung: SUCCESS: datatype=timestamp
	@Test
	public void import_Datatype_Timestamp_DefinedFormat_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(attr1 timestamp, the_geom geometry(POINT,2056))WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File(TEST_IN, "Attributes/Timestamp/DataTypeTimestamp2.shp");
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
				config.setValue(IoxWkfConfig.SETTING_TIMESTAMPFORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSS");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
	        {
				stmt=jdbcConnection.createStatement();
				ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM shptodbschema.shpimporttable;");
				while(rowCount.next()) {
					assertEquals(1, rowCount.getInt(1));
				}
				ResultSet rs = stmt.executeQuery("SELECT attr1,st_asewkt(the_geom) FROM shptodbschema.shpimporttable;");
				ResultSetMetaData rsmd=rs.getMetaData();
				assertEquals(2, rsmd.getColumnCount());
				while(rs.next()){
					assertEquals("2001-07-04 12:08:56.235", rs.getString(1));
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
	
	// Das Dateformat in den Settings ist ungueltig, es muss die folgende Fehlermeldung ausgegeben werden:
	// --
	// Erwartung: FAIL: 2017-10-20 does not match format: HH:mm:ss.
	@Test
	public void import_Datatype_Date_WrongDateFormatSet_Fail() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
	        	try {
					// shp
					File data=new File(TEST_IN, "Attributes/Date/DataTypeDate.shp");
					config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
					config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
					config.setValue(IoxWkfConfig.SETTING_DATEFORMAT, "HH:mm:ss");
					AbstractImport2db shp2db=new Shp2db();
					shp2db.importData(data, jdbcConnection, config);
					fail();
				}catch(Exception e) {
					assertEquals(IoxException.class,e.getClass());
					assertEquals("2017-10-20 does not match format: HH:mm:ss.",e.getMessage());
				}
			}
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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
		Settings config=new Settings();
		config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "shptodbschema");
		config.setValue(IoxWkfConfig.SETTING_DBTABLE, "shpimporttable");
		Connection jdbcConnection=null;
		try{
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	        // shp
			File data=new File(TEST_IN, "NOTFOUND/testPointAttrs.shp");
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("file"));
			assertTrue(e.getMessage().contains("testPointAttrs"));
			assertTrue(e.getMessage().contains("not found."));
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
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
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