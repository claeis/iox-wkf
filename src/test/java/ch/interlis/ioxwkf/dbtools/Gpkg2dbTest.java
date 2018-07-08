package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.junit.Test;

import ch.ehi.basics.settings.Settings;

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

}
