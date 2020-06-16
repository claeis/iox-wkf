package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.PostgisContainerProvider;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2gpkg.Gpkg2iox;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxException;


public class Db2GpkgTest {
	private static final String TEST_OUT="build/test/data/DB2Gpkg/";

    static String WAIT_PATTERN = ".*database system is ready to accept connections.*\\s";
    
    @ClassRule
    public static PostgreSQLContainer postgres = 
        (PostgreSQLContainer) new PostgisContainerProvider()
        .newInstance().withDatabaseName("ioxwkf")
        .waitingFor(Wait.forLogMessage(WAIT_PATTERN, 2));

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
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());	        
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_singlerow_ok");
                
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());	        
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_norow_ok");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
	        pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());	        
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_schemanotset_ok");

				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultgpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_findtableindefinedschema_ok");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultgpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_findtableindefaultschema_ok");

				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "defaultgpkgexport");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_bigint");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_boolean");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_bit");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_bit1");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
	// Erwartung: SUCCESS: datatype=bit3
	@Test
	public void export_Datatype_Bit3_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeBit3.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
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
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_bit3");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
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
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die GPKG-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=char
	@Test
	public void export_Datatype_Char_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeCharacter.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('a','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_character");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_character");
		            while (rs.next()) {
		            	assertEquals("a", rs.getString(1));
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
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die GPKG-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=varchar
	@Test
	public void export_Datatype_VarChar_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeVarchar.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('abc','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_varchar");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_varchar");
		            while (rs.next()) {
		            	assertEquals("abc", rs.getString(1));
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

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die GPKG-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=date
	@Test
	public void export_Datatype_Date_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeDate.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr date,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('2017-02-15','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_date");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_date");
		            while (rs.next()) {
		            	assertEquals("2017-02-15", rs.getString(1));
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
	
	// Der DatenType: attr wird mit dem Wert: NULL in der Datenbank erstellt.
	// Die Tabelle darf somit keinen Wert mit dem Attribute-Namen: attr enthalten.
	// - set: database-dbtoshpschema
	// - set: database-exportdatatype
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_Datatype_Date_Null_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeDateNull.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr date,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES (NULL,'0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_date_null");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_date_null");
		            while (rs.next()) {
		            	assertEquals(null, rs.getObject(1));
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
	
	// Es werden 5 DatenTypen mit jeweils NULL Werte in der Datenbank erstellt.
	// Die Tabelle darf somit keine Werte enthalten.
	// - set: database-dbtoshpschema
	// - set: database-exportdatatype
	// --
	// Erwartung: SUCCESS.
	@Test
	public void export_SomeDataTypes_Null_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeDataTypesNull.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, attr2 bit, attr3 numeric, attr4 date, the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,attr2,attr3,attr4,the_geom) VALUES (NULL,NULL,NULL,NULL,NULL)");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_datatypes_null");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr,attr2,attr3,attr4,the_geom FROM export_datatype_datatypes_null");
		            while (rs.next()) {
		            	assertEquals(null, rs.getObject(1));
		            	assertEquals(null, rs.getObject(2));
		            	assertEquals(null, rs.getObject(3));
		            	assertEquals(null, rs.getObject(4));
		            	assertEquals(null, rs.getObject(5));
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

	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die GPKG-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=integer
	@Test
	public void export_Datatype_Integer_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeInteger.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr integer,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES (12,'0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_integer");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_integer");
		            while (rs.next()) {
		            	assertEquals(12, rs.getInt(1));
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
	// Erwartung: SUCCESS: datatype=numeric
	@Test
	public void export_Datatype_Numeric_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeNumeric.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr numeric,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES (123,'0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_numeric");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_numeric");
		            while (rs.next()) {
		            	assertEquals(123.0, rs.getDouble(1), 0.0001);
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
	// Erwartung: SUCCESS: datatype=character varying
	@Test
	public void export_Datatype_Text_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeText.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('character varying','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_text");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_text");
		            while (rs.next()) {
		            	assertEquals("character varying", rs.getString(1));
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
	// Erwartung: SUCCESS: datatype=smallint
	@Test
	public void export_Datatype_Smallint_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeSmallint.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr smallint,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES (5,'0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_smallint");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_smallint");
		            while (rs.next()) {
		            	assertEquals(5, rs.getInt(1));
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
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=timestamp
	@Test
	public void export_Datatype_Timestamp_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeTimestamp.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr timestamp,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('2014-05-15 12:30:30.555','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_timestamp");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_timestamp");
		            while (rs.next()) {
		            	assertEquals("2014-05-15T12:30:30.555Z", rs.getString(1));
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
	// Erwartung: SUCCESS: datatype=uuid	
	@Test
	public void export_Datatype_Uuid_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeUuid.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr uuid,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('123e4567-e89b-12d3-a456-426655440000','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_uuid");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_uuid");
		            while (rs.next()) {
		            	assertEquals("123e4567-e89b-12d3-a456-426655440000", rs.getString(1));
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
	// Erwartung: SUCCESS: datatype=xml
	@Test
	public void export_Datatype_Xml_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeXml.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr xml,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('<attrText>character varying</attrText>','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_xml");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_xml");
		            while (rs.next()) {
		            	assertEquals("<attrText>character varying</attrText>", rs.getString(1));
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
	// Erwartung: SUCCESS: datatype=point2d
	@Test
	public void export_Datatype_Point2d_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypePoint2d.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('coord2d','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_point2d");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_point2d");
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
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=point2d
	@Test
	public void export_Datatype_Point3d_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypePoint3d.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, the_geom geometry(POINTZ,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('coord3d','01010000a0080800001cd4411dd441cdbf0e69626cdd33e23fb0726891ed7cbf3f')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_point3d");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_point3d");
		            while (rs.next()) {
		            	assertEquals("coord3d", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687, C3 0.123}",
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
	// Erwartung: SUCCESS: datatype=multipoint
	@Test
	public void export_Datatype_MultiPoint2d_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeMultiPoint2d.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, the_geom geometry(MULTIPOINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('multicoord2d','0104000020080800000300000001010000001CD4411DD441CDBF0E69626CDD33E23F010100000074CFC8D2439AC8BF9E91A5873431E63F01010000006668E3AA7F40DFBFF094204F09F2D43F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_multipoint2d");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_multipoint2d");
		            while (rs.next()) {
		            	assertEquals("multicoord2d", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("MULTICOORD {coord [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.19220779220779216, C2 0.6935064935064934}, COORD {C1 -0.48831168831168836, C2 0.32727272727272716}]}", 
		            			iomGeom.toString());
		            }
			        rs.close();
			        stmt.close();
			        
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT z FROM gpkg_geometry_columns");
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
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die Gpkg-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=multipoint
	@Test
	public void export_Datatype_MultiPoint3d_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeMultiPoint3d.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, the_geom geometry(MULTIPOINTZ,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('multicoord3d','01040000a0080800000300000001010000801bd4411dd441cdbf0e69626cdd33e23f9a9999999999b93f010100008075cfc8d2439ac8bf9e91a5873431e63f9a9999999999c93f01010000806768e3aa7f40dfbff194204f09f2d43f333333333333d33f')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_multipoint3d");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_multipoint3d");
		            while (rs.next()) {
		            	assertEquals("multicoord3d", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("MULTICOORD {coord [COORD {C1 -0.2285714285714285, C2 0.5688311688311687, C3 0.1}, COORD {C1 -0.1922077922077922, C2 0.6935064935064934, C3 0.2}, COORD {C1 -0.4883116883116884, C2 0.3272727272727272, C3 0.3}]}", 
		            			iomGeom.toString());
		            }
			        rs.close();
			        stmt.close();
			        
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT z FROM gpkg_geometry_columns");
		            while (rs.next()) {
		            	assertEquals(1, rs.getInt(1));
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
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=linestring
	@Test
	public void export_Datatype_LineString_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeLineString.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, the_geom geometry(LINESTRING,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('linestring','010200002008080000020000001CD4411DD441CDBF0E69626CDD33E23F202A504A86DFCCBF8FFEA5F7491BE23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_linestring");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_linestring");
		            while (rs.next()) {
		            	assertEquals("linestring", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.22557142857142853, C2 0.5658311688311687}]}}", 
		            			iomGeom.toString());
		            }
			        rs.close();
			        stmt.close();
			        
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT z FROM gpkg_geometry_columns");
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
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die Gpkg-Datei exportiert wurde.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=multilinestring
	@Test
	public void export_Datatype_MultiLineString_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeMultiLineString.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, the_geom geometry(MULTILINESTRING,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('multilinestring','010500002008080000020000000102000000020000001CD4411DD441CDBF0E69626CDD33E23F202A504A86DFCCBF8FFEA5F7491BE23F010200000002000000202A504A86DFCCBF8FFEA5F7491BE23FADA9EFBB6720CDBF981603D666C9E13F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_multilinestring");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_multilinestring");
		            while (rs.next()) {
		            	assertEquals("multilinestring", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("MULTIPOLYLINE {polyline [POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.22557142857142853, C2 0.5658311688311687}]}}, POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22557142857142853, C2 0.5658311688311687}, COORD {C1 -0.22755142857142854, C2 0.5558351688311687}]}}]}", 
		            			iomGeom.toString());
		            }
			        rs.close();
			        stmt.close();
			        
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT z FROM gpkg_geometry_columns");
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
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die Gpkg-Datei exportiert wurde.
 	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=polygon
	@Test
	public void export_Datatype_Polygon_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypePolygon.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, the_geom geometry(POLYGON,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('polygon','01030000200808000001000000060000001CD4411DD441CDBF0E69626CDD33E23F26ABE58D114CC4BFB2D99F76B4D7E23F26ABE58D114CC4BFB2D99F76B4D7E23F26ABE58D114CC4BF0E69626CDD33E23F26ABE58D114CC4BF0E69626CDD33E23F1CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_polygon");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_polygon");
		            while (rs.next()) {
		            	assertEquals("polygon", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5888311688311687}, COORD {C1 -0.15857142857142853, C2 0.5888311688311687}, COORD {C1 -0.15857142857142853, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5688311688311687}, COORD {C1 -0.22857142857142854, C2 0.5688311688311687}]}}}}}", 
		            			iomGeom.toString());
		            }
			        rs.close();
			        stmt.close();
			        
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT z FROM gpkg_geometry_columns");
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
	
	// Es wird getestet, ob jeder Datentyp innerhalb der Datenbank-Tabelle im richtigen Format in die SHP Datei exportiert wurde.
	// - set: header-present
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS: datatype=multipolygon
	@Test
	public void export_Datatype_MultiPolygon_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_DataTypeMultiPolygon.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportdatatype(attr character varying, the_geom geometry(MULTIPOLYGON,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportdatatype(attr,the_geom) VALUES ('multipolygon','0106000020080800000100000001030000000100000006000000C976BE9F1A2FCDBF931804560E2DE23FD34D62105839C4BF37894160E5D0E23FD34D62105839C4BF37894160E5D0E23FD34D62105839C4BF931804560E2DE23FD34D62105839C4BF931804560E2DE23FC976BE9F1A2FCDBF931804560E2DE23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_datatype_multipolygon");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportdatatype");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_datatype_multipolygon");
		            while (rs.next()) {
		            	assertEquals("multipolygon", rs.getString(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.228, C2 0.568}, COORD {C1 -0.158, C2 0.588}, COORD {C1 -0.158, C2 0.588}, COORD {C1 -0.158, C2 0.568}, COORD {C1 -0.158, C2 0.568}, COORD {C1 -0.228, C2 0.568}]}}}}}", 
		            			iomGeom.toString());
		            }
			        rs.close();
			        stmt.close();
			        
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT z FROM gpkg_geometry_columns");
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
	
	// Testet, ob connection=null zu einer IoxException fuehrt. 
	@Test
	public void export_ConnectionFailed_Fail() throws Exception {
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        jdbcConnection = null;

			File data=new File(TEST_OUT,"export_ConnectionFailed_Fail.gpkg");
            config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_connectionfailed");

			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgexport");
			Db2Gpkg db2Gpkg=new Db2Gpkg();
			db2Gpkg.exportData(data, jdbcConnection, config);
	    	fail();
		} catch(Exception e) {
			assertEquals(IoxException.class,e.getClass());
			assertEquals("connection==null",e.getMessage());
		} finally {
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil das Schema zwar gesetzt wird, jedoch in der Datenbank nicht existiert.
	// Daraus folgt, dass die Datenbank Tabelle nicht gefunden werden kann.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: FEHLER: table dbtogpkgschema.table .. not found
	@Test
	public void export_TableInSchemaNotFound_Fail() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_TableInSchemaNotFound_Fail.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.gpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.gpkgexport(attr,the_geom) VALUES ('abc','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }

	        if(data.exists()) {
	        	data.delete();
	        }
            config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_table_in_schema_not_found");

	        config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema999999");
	        config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgexport");
	        AbstractExportFromdb db2Gpkg=new Db2Gpkg();
	        db2Gpkg.exportData(data, pgConnection, config);
	        fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("db table"));
			assertTrue(e.getMessage().contains("gpkgexport"));
			assertTrue(e.getMessage().contains("inside db schema"));
			assertTrue(e.getMessage().contains("dbtogpkgschema999999"));
			assertTrue(e.getMessage().contains("not found"));
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, weil die Tabelle zwar gesetzt wird, jedoch in der Datenbank nicht existiert.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: database-dbtogpkgschema
	// - set: database-table
	// --
	// Erwartung: FEHLER: table .. not found
	@Test
	public void export_TableNotFound_Fail() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_TableInSchemaNotFound_Fail.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.gpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.gpkgexport(attr,the_geom) VALUES ('abc','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }

	        if(data.exists()) {
	        	data.delete();
	        }
            config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_table_in_schema_not_found");

	        config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
	        config.setValue(IoxWkfConfig.SETTING_DBTABLE, "gpkgexport999999");
	        AbstractExportFromdb db2Gpkg=new Db2Gpkg();
	        db2Gpkg.exportData(data, pgConnection, config);
	        fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("db table"));
			assertTrue(e.getMessage().contains("gpkgexport999999"));
			assertTrue(e.getMessage().contains("inside db schema"));
			assertTrue(e.getMessage().contains("dbtogpkgschema"));
			assertTrue(e.getMessage().contains("not found"));
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
		}
	}
	
	// Es soll eine Fehlermeldung ausgegeben werden, da nichts in den Settings gesetzt wird.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - NOT SET: database-dbtogpkgschema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	@Test
	public void export_AllNotSet_Fail() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_AllNotSet_Fail.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.gpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.gpkgexport(attr,the_geom) VALUES ('abc','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }

	        if(data.exists()) {
	        	data.delete();
	        }
            config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_all_not_set");

	        // DBSCHEMA: "dbtogpkgschema" not set
			// TABLE: "gpkgexport" not set
	        AbstractExportFromdb db2Gpkg=new Db2Gpkg();
	        db2Gpkg.exportData(data, pgConnection, config);
	        fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("database table==null."));
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
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
	public void export_TableNotSet_Fail() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_TableNotSet_Fail.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.gpkgexport(attr character varying,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.gpkgexport(attr,the_geom) VALUES ('abc','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }

	        if(data.exists()) {
	        	data.delete();
	        }
            config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_table_not_set");

	        config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
	        AbstractExportFromdb db2Gpkg=new Db2Gpkg();
	        db2Gpkg.exportData(data, pgConnection, config);
	        fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("database table==null."));
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob zwei Geometrieattribute exportiert werden können.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: FAILURE: datatype=point2d
	@Test
	public void export_MultipleGeometryColumns_Fail() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_multiple_geometry_columns.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	// identische Geometrietypen
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportgpkg(attr character varying, the_point geometry(POINT,2056), the_other_point geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportgpkg(attr,the_point,the_other_point) VALUES ('fubar','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
			if(data.exists()) {
				data.delete();
			}
            config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_multiple_geometry_columns");

			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportgpkg");
			AbstractExportFromdb db2Gpkg=new Db2Gpkg();
			db2Gpkg.exportData(data, pgConnection, config);
			fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("export of:"));
			assertTrue(e.getMessage().contains("failed"));
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob zwei Geometrieattribute exportiert werden können.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: FAILURE: datatype=point2d
	@Test
	public void export_MultipleGeometryColumns2_Fail() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_multiple_geometry_columns.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	// unterschiedliche Geometrietypen
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exportgpkg(attr character varying, the_point geometry(POINT,2056), the_linestring geometry(LINESTRING,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exportgpkg(attr,the_point,the_linestring) VALUES ('fubar','0101000020080800001CD4411DD441CDBF0E69626CDD33E23F','010200002008080000020000001CD4411DD441CDBF0E69626CDD33E23F202A504A86DFCCBF8FFEA5F7491BE23F')");
	        	preStmt.close();
	        }
			if(data.exists()) {
				data.delete();
			}
            config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_multiple_geometry_columns2");

			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exportgpkg");
			AbstractExportFromdb db2Gpkg=new Db2Gpkg();
			db2Gpkg.exportData(data, pgConnection, config);
			fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("export of:"));
			assertTrue(e.getMessage().contains("failed"));
		} finally {
			if (pgConnection!=null) {
				pgConnection.close();
			}
		}
	}
	
	// Es wird getestet, ob zwei Tabellen in die GPKG-Datei exportiert wurden.
	// - set: database-dbtoshpschema
	// - set: database-table
	// --
	// Erwartung: SUCCESS
	@Test
	public void export_twoTables_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_TwoTables.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        {
	        	Statement preStmt=pgConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS dbtogpkgschema CASCADE");
	        	preStmt.execute("CREATE SCHEMA dbtogpkgschema");
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exporttable1(attr integer,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exporttable1(attr,the_geom) VALUES (1,'0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	
	        	preStmt.execute("CREATE TABLE dbtogpkgschema.exporttable2(attr integer,the_geom geometry(POINT,2056));");
	        	preStmt.executeUpdate("INSERT INTO dbtogpkgschema.exporttable2(attr,the_geom) VALUES (2,'0101000020080800001CD4411DD441CDBF0E69626CDD33E23F')");
	        	preStmt.close();
	        }
	        {
				if(data.exists()) {
					data.delete();
				}
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_table_1");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exporttable1");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
                config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_table_2");

				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtogpkgschema");
				config.setValue(IoxWkfConfig.SETTING_DBTABLE, "exporttable2");
				AbstractExportFromdb db2Gpkg=new Db2Gpkg();
				db2Gpkg.exportData(data, pgConnection, config);
			}
	        {
		        Statement stmt = null;
		        ResultSet rs = null;
		        try {
		        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
		        	gpkgConnection = DriverManager.getConnection("jdbc:sqlite:" + data.getAbsolutePath());
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_table_1");
		            while (rs.next()) {
		            	assertEquals(1, rs.getInt(1));
		            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(2));
		            	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
		            			iomGeom.toString());
		            }
			        rs.close();
			        stmt.close();
			        
		        	stmt = gpkgConnection.createStatement();
		        	rs = stmt.executeQuery("SELECT attr, the_geom FROM export_table_2");
		            while (rs.next()) {
		            	assertEquals(2, rs.getInt(1));
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

	@Ignore("Needs another source database. Export takes one minute.")
	@Test
	public void export_Realworld_Dataset_Ok() throws Exception {
		Settings config=new Settings();
		Connection pgConnection=null;
		Connection gpkgConnection = null;
		File data = new File(TEST_OUT,"export_Realworld_Dataset.gpkg");
		
		try {
            pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());          
	        
			if(data.exists()) {
				data.delete();
			}
            config.setValue(IoxWkfConfig.SETTING_GPKGTABLE, "export_realworld_dataset");

			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "agi_mopublic_pub");
			config.setValue(IoxWkfConfig.SETTING_DBTABLE, "mopublic_bodenbedeckung");
			AbstractExportFromdb db2Gpkg=new Db2Gpkg();
			db2Gpkg.exportData(data, pgConnection, config);
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
