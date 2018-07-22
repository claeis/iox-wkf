package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Gpkg2dbTest {
    private String dburl=System.getProperty("dburl");
    private String dbuser=System.getProperty("dbusr");
    private String dbpwd=System.getProperty("dbpwd");
    private Statement stmt=null;
    private static final String TEST_IN="src/test/data/Gpkg2DB/";

    // Testet ob der Import eines Points in die Datenbank funktioniert,
    // wenn die Test-Konfiguration wie folgt gesetzt wird:
    // - set: database-schema
    // - set: database-table
    // --
    // Erwartung: SUCCESS.
    @Test
    public void import_Point_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
               
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, idname character varying, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "Point.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "Point";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,idname,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("12", rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.228571428571429 0.568831168831169)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }
    
    // Testet, ob der Import eines Linestrings in die Datenbank funktioniert,
    // wenn die Test-Konfiguration wie folgt gesetzt wird:
    // - set: database-schema
    // - set: database-table
    // --
    // Erwartung: SUCCESS.
    @Test
    public void import_LineString_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try{
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer,idname character varying,geom geometry(LINESTRING,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "LineString.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "LineString";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,idname,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("12", rs.getObject(2));
                    assertEquals("SRID=2056;LINESTRING(-0.228571428571429 0.568831168831169,-0.225571428571429 0.565831168831169)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }
    
    // Testet, ob der Import eines Multilinestrings in die Datenbank funktioniert,
    // wenn die Test-Konfiguration wie folgt gesetzt wird:
    // - set: database-schema
    // - set: database-table
    // --
    // Erwartung: SUCCESS.
    @Test
    public void import_MultiLineString_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try{
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer,idname character varying,geom geometry(MULTILINESTRING,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "MultiLineString.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "MultiLineString";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,idname,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("12", rs.getObject(2));
                    assertEquals("SRID=2056;MULTILINESTRING((-0.228571428571429 0.568831168831169,-0.225571428571429 0.565831168831169),(-0.225571428571429 0.565831168831169,-0.227551428571429 0.555835168831169))", rs.getObject(3));
                }
            }
        } finally {
            if(jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }

    // Testet, ob der Import eines Polygons in die Datenbank funktioniert,
    // wenn die Test-Konfiguration wie folgt gesetzt wird:
    // - set: database-schema
    // - set: database-table
    // --
    // Erwartung: SUCCESS.
    @Test
    public void import_Polygon_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try{
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer,idname character varying,geom geometry(POLYGON,2056))WITH (OIDS=FALSE)");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "Polygon.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "Polygon";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,idname,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("12", rs.getObject(2));
                    assertEquals("SRID=2056;POLYGON((-0.228571428571429 0.568831168831169,-0.158571428571429 0.588831168831169,-0.158571428571429 0.588831168831169,-0.158571428571429 0.568831168831169,-0.158571428571429 0.568831168831169,-0.228571428571429 0.568831168831169))", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }

    // Testet, ob der Import eines Multipolygons in die Datenbank funktioniert,
    // wenn die Test-Konfiguration wie folgt gesetzt wird:
    // - set: database-schema
    // - set: database-table
    // --
    // Erwartung: SUCCESS.
    @Test
    public void import_MultiPolygon_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try{
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer,idname character varying,geom geometry(MULTIPOLYGON,2056))");
                preStmt.close();
            }
            {
                // shp
                File data=new File(TEST_IN, "MultiPolygon.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "MultiPolygon";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(2, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,idname,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertTrue(rs.getObject(1).equals(1) || rs.getObject(1).equals(2));
                    assertEquals("12", rs.getObject(2));
                    assertTrue(rs.getObject(3).equals("SRID=2056;MULTIPOLYGON(((-0.228 0.568,-0.158 0.588,-0.158 0.588,-0.158 0.568,-0.158 0.568,-0.228 0.568)))") ||
                            (rs.getObject(3).equals("SRID=2056;MULTIPOLYGON(((0.228 1.3,0.158 0.5,0.158 0.5,0.158 1.568,0.158 1.568,0.228 1.3)))")));
                }
            }
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }

    // Testet, ob der Import eines Linestrings in die Datenbank funktioniert,
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
                preStmt.execute("DROP TABLE IF EXISTS gpkgmportnoschematable CASCADE");
                preStmt.execute("CREATE TABLE gpkgmportnoschematable(fid integer,idname character varying NOT NULL,geom geometry(POINT,2056),CONSTRAINT gpkgimporttable_pkey PRIMARY KEY (idname))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "Point.gpkg");
                //config.setValue(Config.DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgmportnoschematable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "Point";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgmportnoschematable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,idname,st_asewkt(geom) FROM gpkgmportnoschematable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("12", rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.228571428571429 0.568831168831169)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_BigInt_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try{
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer,attr bigint,geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeBigInt.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeBigInt";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(Long.valueOf("123"), rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
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
    public void import_Datatype_Bit_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try{
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr boolean, geom geometry(POINT,2056))WITH (OIDS=FALSE)");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeBit.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeBit";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(true, rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
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
    public void import_Datatype_Boolean_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr boolean, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeBoolean.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeBoolean";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals(true, rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
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
    public void import_Datatype_Char_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr character, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeChar.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeChar";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid, attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("a", rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }

    @Test
    public void import_attrName_similar_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, \"Attr\" character, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeChar.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeChar";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid, \"Attr\",st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("a", rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_VarChar_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try{
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                // drop schema
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                // create schema
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                // create table in schema
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr character varying, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                // shp
                File data=new File(TEST_IN, "DataTypeVarChar.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeVarChar";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid, attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("12345", rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_Date_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try{
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer,attr date, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeDate.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeDate";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals("2017-10-20", rs.getString(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_Integer_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr integer, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeInteger.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeInteger";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(1));
                    assertEquals(12, rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_Numeric_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr numeric, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeNumeric.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeNumeric";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(java.math.BigDecimal.valueOf(Long.valueOf("12")), rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_Text_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr character varying, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeText.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeText";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals("testtext", rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
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
    public void import_Datatype_Time_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer,attr time without time zone, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeTime.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeTime";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(java.sql.Time.valueOf("20:30:59"), rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_Smallint_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr smallint, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeSmallint.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeSmallint";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals(1, rs.getObject(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_Timestamp_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr1 timestamp, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeTimestamp.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeTimestamp";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr1,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals("2001-07-04 12:08:56.235", rs.getString(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_Uuid_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr uuid, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeUuid.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeUuid";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertEquals("123e4567-e89b-12d3-a456-426655440000", rs.getString(2));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
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
    public void import_Datatype_Xml_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS gpkgtodbschema CASCADE");
                preStmt.execute("CREATE SCHEMA gpkgtodbschema");
                preStmt.execute("CREATE TABLE gpkgtodbschema.gpkgimporttable(fid integer, attr xml, geom geometry(POINT,2056))");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeXml.gpkg");
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeXml";
                gpkg2db.importData(dataSet, jdbcConnection, config);
            }
            {
                stmt=jdbcConnection.createStatement();
                ResultSet rowCount = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM gpkgtodbschema.gpkgimporttable;");
                while(rowCount.next()) {
                    assertEquals(1, rowCount.getInt(1));
                }
                ResultSet rs = stmt.executeQuery("SELECT fid,attr,st_asewkt(geom) FROM gpkgtodbschema.gpkgimporttable;");
                ResultSetMetaData rsmd=rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
                while(rs.next()){
                    assertTrue(rs.getString(2).equals("<attrText>text</attrText>"));
                    assertEquals("SRID=2056;POINT(-0.402597402597403 1.3974025974026)", rs.getObject(3));
                }
            }
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }   

    // Wenn die Verbindung zu Datenbank: null ist,
    // muss die Fehlermeldung: connection=null ausgegeben werden.
    // --
    // Die Test-Konfiguration wird wie folgt gesetzt:
    // - set: GeoPackage-file
    // - NOT SET: connection content.
    // --
    // Erwartung: FEHLER: connection=null.
    @Test
    public void import_ConnectionFailed_Fail() throws Exception {
        Settings config=null;
        config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = null;

            File data=new File(TEST_IN, "DataTypeBoolean.gpkg");
            AbstractImport2db gpkg2db=new Gpkg2db();
            String dataSet = data.getAbsolutePath() + ";" + "DataTypeBoolean";
            gpkg2db.importData(dataSet, jdbcConnection, config);
            fail();
        } catch(Exception e) {
            assertEquals(IoxException.class,e.getClass());
            assertEquals("connection==null.",e.getMessage());
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }

    // Testet, ob die Fehlermeldung: expected tablename ausgegeben wird,
    // wenn keine Parameter gesetzt sind.
    // --
    // - NOT SET: database-schema
    // - NOT SET: database-table
    // --
    // Erwartung: FEHLER: expected tablename
    @Test
    public void import_AllNotSet_Fail() throws Exception {
        Settings config=null;
        config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);

            File data=new File(TEST_IN, "DataTypeBoolean.gpkg");
            // DBSCHEMA: "gpkgtodbschema" not set
            // TABLE: "gpkgimportwithheader" not set
            AbstractImport2db gpkg2db=new Gpkg2db();
            String dataSet = data.getAbsolutePath() + ";" + "DataTypeBoolean";
            gpkg2db.importData(dataSet, jdbcConnection, config);
            fail();
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("database table==null."));
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }

    // Testet, ob die Fehlermeldung: expected tablename ausgegeben wird,
    // wenn die Datenbank-Tabelle nicht als Parameter gesetzt wird.
    // --
    // - set: database-schema
    // - NOT SET: database-table
    // --
    // Erwartung: FEHLER: expected tablename
    @Test
    public void import_TableNotSet_Fail() throws Exception {
        Settings config=null;
        config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            // shp
            File data=new File(TEST_IN, "DataTypeBoolean.gpkg");
            config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
            // TABLE: "gpkgimporttable" not set
            AbstractImport2db gpkg2db=new Gpkg2db();
            String dataSet = data.getAbsolutePath() + ";" + "DataTypeBoolean";
            gpkg2db.importData(dataSet, jdbcConnection, config);
            fail();
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("database table==null."));
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }
    
    // Testet, ob die Fehlermeldung: shapefile .. not found ausgegeben wird,
    // wenn die gesetzte shp Datei nicht existiert.
    // Die Test-Konfiguration wird wie folgt gesetzt:
    // - set: database-schema
    // - set: database-table
    // --
    // Erwartung: FEHLER: geopackage ... not found
    @Test
    public void import_ShpFileNotFound_Fail() throws Exception {
        Settings config=new Settings();
        config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "gpkgtodbschema");
        config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimporttable");
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            // shp
            File data=new File(TEST_IN, "NOTFOUND/testPointAttrs.gpkg");
            AbstractImport2db gpkg2db=new Gpkg2db();
            String dataSet = data.getAbsolutePath() + ";" + "testPointAttrs";
            gpkg2db.importData(dataSet, jdbcConnection, config);
            fail();
        } catch(IoxException e) {
            assertTrue(e.getMessage().contains("file"));
            assertTrue(e.getMessage().contains("testPointAttrs"));
            assertTrue(e.getMessage().contains("not found."));
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }

    // Testet, ob die Fehlermeldung: import failed ausgegeben wird,
    // wenn die id als primary key in der Datenbank-Tabelle gesetzt wurde,
    // und die gleichen Daten in die Spalte id importiert werden soll.
    // Die Test-Konfiguration wird wie folgt gesetzt:
    // - set: database-schema
    // - set: database-table
    // --
    // Erwartung: FEHLER: import failed.
    @Test
    public void import_UniqueConstraint_Fail() throws Exception {
        Settings config=null;
        config=new Settings();
        Connection jdbcConnection=null;
        try {
            Class driverClass = Class.forName("org.postgresql.Driver");
            jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
            {
                Statement preStmt=jdbcConnection.createStatement();
                preStmt.execute("DROP TABLE IF EXISTS gpkgimportunique CASCADE");
                preStmt.execute("CREATE TABLE gpkgimportunique(fid integer, attr boolean NOT NULL,geom geometry(POINT,2056),CONSTRAINT gpkgimportunique_pkey PRIMARY KEY (attr))");
                preStmt.executeUpdate("INSERT INTO gpkgimportunique(attr,geom) VALUES ('TRUE','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
                preStmt.close();
            }
            {
                File data=new File(TEST_IN, "DataTypeBoolean.gpkg");
                // DBSCHEMA: "shptodbschema"
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgimportunique");
                AbstractImport2db gpkg2db=new Gpkg2db();
                String dataSet = data.getAbsolutePath() + ";" + "DataTypeBoolean";
                gpkg2db.importData(dataSet, jdbcConnection, config);
                fail();
            }
        } catch(IoxException e) {
            assertTrue(e.getCause() instanceof SQLException);
            // unique violation.
            assertEquals("23505",((SQLException) e.getCause()).getSQLState());
        } finally {
            if (jdbcConnection!=null){
                jdbcConnection.close();
            }
        }
    }
}
