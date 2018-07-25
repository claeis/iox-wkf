package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;

import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2gpkg.Gpkg2iox;
import ch.interlis.iom.IomObject;


public class Db2GpkgTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private static final String TEST_OUT="build/test/data/DB2Gpkg/";

	@BeforeClass
	public static void setup() {
		new File(TEST_OUT).mkdirs();
	}

	// Es soll keine Fehlermeldung ausgegeben werden, wenn 1 Reihe der Tabelle in eine Gpkg-Datei geschrieben wird.
	// - set: database-dbtogpkgschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SingleRow_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_SingleRow_Ok.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.gpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.gpkgexport(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_singlerow_ok";

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_singlerow_ok");
		            while (rs.next()) {
		            	assertEquals("coord2d", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	}

	// Es soll keine Fehlermeldung ausgegeben werden, wenn keine Reihe in die Gpkg-Datei geschrieben wird.
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_NoRow_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_NoRow_Ok.gpkg");

		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.gpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_norow_ok";

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
	        	Statement stmt = null;
	        	ResultSet rs = null;
	        	try {
	        		Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	        		gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
	        		stmt = gpkgConnection.createStatement();
	        		rs = stmt.executeQuery("SELECT count(*) FROM gpkg_contents");
	        		while (rs.next()) {
	        			assertEquals(0, rs.getInt(1));
	        		}
	        		rs.close();
	        		stmt.close();
	        	} catch (SQLException e) {
	        		e.printStackTrace();
	        		fail();
	        	}
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	}

	// Es soll keine Fehlermeldung ausgegeben werden, weil auch ohne die Angabe des Datenbank Schemas, die Tabelle gefunden werden kann.
	// - NOT SET: database-dbtogpkgschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SchemaNotSet_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_SchemaNotSet_Ok.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP TABLE IF EXISTS defaultgpkgexport CASCADE");
	        	preStmt.execute("CREATE TABLE defaultgpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO defaultgpkgexport(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_schemanotset_ok";

				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultgpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_schemanotset_ok");
		            while (rs.next()) {
		            	assertEquals("coord2d", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	}

	// Es soll keine Fehlermeldung ausgegeben werden, weil 2 Tabellen mit dem selben Namen in unterschiedlichen Schemen existieren.
	// Dabei wird die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-table
	// --
	// Erwartung: Es sollte nur diese Tabelle gefunden werden, welche sich innerhalb des gesetzten Schemas befindet.
	@Test
	public void export_FindTableInDefinedSchema_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_FindTableInDefinedSchema_Ok.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("DROP TABLE IF EXISTS defaultgpkgexport CASCADE");

	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.defaultgpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.execute("CREATE TABLE defaultgpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.defaultgpkgexport(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_findtableindefinedschema_ok";

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultgpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_findtableindefinedschema_ok");
		            while (rs.next()) {
		            	assertEquals("coord2d", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	}
	
	// Es soll keine Fehlermeldung ausgegeben werden, weil 2 Tabellen mit dem selben Namen in unterschiedlichen Schemen existieren.
	// Dabei wird die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-table
	// --
	// Erwartung: Es sollte nur diese Tabelle gefunden werden, welche sich innerhalb des default Schemas befindet.
	@Test
	public void export_FindTableInDefaultSchema_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_FindTableInDefaultSchema_Ok.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("DROP TABLE IF EXISTS defaultgpkgexport CASCADE");

	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.defaultgpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.execute("CREATE TABLE defaultgpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO defaultgpkgexport(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_findtableindefaultschema_ok";

				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultgpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_findtableindefaultschema_ok");
		            while (rs.next()) {
		            	assertEquals("coord2d", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die Gpkg-Datei exportiert wurde.
	// - set: database-dbtogpkgschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bigint
	@Test
	public void export_Datatype_BigInt_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeBigint.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr bigint,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('9223372036854775807','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_datatype_bigint";

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_bigint");
		            while (rs.next()) {
		            	assertEquals(9223372036854775807L, rs.getLong(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die Gpkg-Datei exportiert wurde.
	// - set: database-dbtogpkgschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bigint
	@Test
	public void export_Datatype_Boolean_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeBoolean.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr boolean,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('true','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_datatype_boolean";

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_boolean");
		            while (rs.next()) {
		            	assertEquals(1, rs.getInt(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	} 
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die Gpkg-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bit
	@Test
	public void export_Datatype_Bit_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeBit.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr bit,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('1','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_datatype_bit";

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_bit");
		            while (rs.next()) {
		            	assertEquals("true", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}

	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die Gpkg-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bit1
	@Test
	public void export_Datatype_Bit1_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeBit1.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr bit(1),the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES (B'1','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_datatype_bit1";

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_bit1");
		            while (rs.next()) {
		            	assertEquals("true", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	}

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die Gpkg-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=bit1
	@Test
	public void export_Datatype_Bit3_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeBit3.gpkg");
		
		try {
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        pgConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr bit(3),the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES (B'101','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                String dataSet = data.getAbsolutePath() + ";" + "export_datatype_bit3";

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(dataSet, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_bit3");
		            while (rs.next()) {
		            	assertEquals("101", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
		        rs.close();
		        stmt.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		            fail();
		        }
	        }
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
			if (gpkgConnection!=null) {
				gpkgConnection.close();
			}
		}
	}
}
