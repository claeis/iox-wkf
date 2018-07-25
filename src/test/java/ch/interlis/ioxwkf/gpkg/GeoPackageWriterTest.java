package ch.interlis.ioxwkf.gpkg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.vividsolutions.jts.io.ParseException;

import ch.ehi.ili2gpkg.Gpkg2iox;
import ch.interlis.ioxwkf.dbtools.AttributeDescriptor;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.ioxwkf.gpkg.GeoPackageWriter;


// TODO:
// - curved geometries -> fail

public class GeoPackageWriterTest {

    private final static String TEST_IN="src/test/data/GpkgWriter";
    private final static String TEST_OUT="build/test/data/GpkgWriter";
    private static final String GPKG_THE_GEOM = "the_geom";
    private TransferDescription td=null;

    @BeforeClass
    public static void setupFolder() throws Ili2cFailure {
        new File(TEST_OUT).mkdirs();
        
        for (File f : new File(TEST_OUT).listFiles()) {
            if (f.getName().endsWith("gpkg") || f.getName().endsWith("journal") 
            		|| f.getName().endsWith("wal") || f.getName().endsWith("shm")) {
                f.delete();
            }
        }
    }

    @Before
    public void setup() throws Ili2cFailure {
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"/Test1.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        assertNotNull(td);
    }

    // In diesem Test wird ein transfer-event gestartet und gleich wieder beendet.
    // Die GeoPackage-Datei muss dabei erstellt werden, darf jedoch nur die gpkg-Tabellen aufweisen.
    // In diesem Test wird die td gesetzt.
    @Test
    public void setModel_emptyTransfer_Ok() throws IoxException, IOException {
        GeoPackageWriter writer = null;
        File file = new File(TEST_OUT,"testEmptyTransfer.gpkg");
        try {
            writer = new GeoPackageWriter(file, "test_empty_transfer");
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new EndTransferEvent());
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer = null;
            }
        }
        
        // check if geopackage file was initialized properly
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rowCount = null;
            ResultSet srsId = null;
            try {
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rowCount = stmt.executeQuery("SELECT COUNT(*) FROM gpkg_spatial_ref_sys");
                while (rowCount.next()) {
                    assertEquals(6, rowCount.getInt(1));
                }
                rowCount.close();
                
                srsId = stmt.executeQuery("SELECT srs_id, organization, definition FROM gpkg_spatial_ref_sys WHERE srs_id = 2056");
                while (srsId.next()) {
                    String definition = "PROJCS[\"CH1903+ / LV95\",GEOGCS[\"CH1903+\",DATUM[\"CH1903+\",SPHEROID[\"Bessel 1841\",6377397.155,299.1528128,AUTHORITY[\"EPSG\",\"7004\"]],TOWGS84[674.374,15.056,405.346,0,0,0,0],AUTHORITY[\"EPSG\",\"6150\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4150\"]],PROJECTION[\"Hotine_Oblique_Mercator_Azimuth_Center\"],PARAMETER[\"latitude_of_center\",46.95240555555556],PARAMETER[\"longitude_of_center\",7.439583333333333],PARAMETER[\"azimuth\",90],PARAMETER[\"rectified_grid_angle\",90],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",2600000],PARAMETER[\"false_northing\",1200000],UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],AXIS[\"Y\",EAST],AXIS[\"X\",NORTH],AUTHORITY[\"EPSG\",\"2056\"]]";
                    assertEquals(definition, srsId.getString(3));
                }
                srsId.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    
    // In diesem Test wird innerhalb der transfer-events, ein basket-event gestartet und gleich wieder beendet.
    // Die GeoPackage-Datei muss dabei erstellt werden, darf jedoch keinen Inhalt aufweisen.
    // In diesem Test wird die td gesetzt.
    @Test
    public void setModel_emptyBasket_Ok() throws IoxException, IOException {
        GeoPackageWriter writer = null;
        File file = new File(TEST_OUT,"testEmptyBasket.gpkg");
        try {
            writer = new GeoPackageWriter(file, "test_empty_basket");
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
         } finally {
            if (writer!=null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }
        // check if geopackage file was initialized properly
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rowCount = null;
            ResultSet srsId = null;
            try {
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rowCount = stmt.executeQuery("SELECT COUNT(*) FROM gpkg_spatial_ref_sys");
                while (rowCount.next()) {
                    assertEquals(6, rowCount.getInt(1));
                }
                rowCount.close();
                
                srsId = stmt.executeQuery("SELECT srs_id, organization, definition FROM gpkg_spatial_ref_sys WHERE srs_id = 2056");
                while (srsId.next()) {
                    String definition = "PROJCS[\"CH1903+ / LV95\",GEOGCS[\"CH1903+\",DATUM[\"CH1903+\",SPHEROID[\"Bessel 1841\",6377397.155,299.1528128,AUTHORITY[\"EPSG\",\"7004\"]],TOWGS84[674.374,15.056,405.346,0,0,0,0],AUTHORITY[\"EPSG\",\"6150\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4150\"]],PROJECTION[\"Hotine_Oblique_Mercator_Azimuth_Center\"],PARAMETER[\"latitude_of_center\",46.95240555555556],PARAMETER[\"longitude_of_center\",7.439583333333333],PARAMETER[\"azimuth\",90],PARAMETER[\"rectified_grid_angle\",90],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",2600000],PARAMETER[\"false_northing\",1200000],UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],AXIS[\"Y\",EAST],AXIS[\"X\",NORTH],AUTHORITY[\"EPSG\",\"2056\"]]";
                    assertEquals(definition, srsId.getString(3));
                }
                srsId.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    
    // In diesem Test wird innerhalb der basket-events, ein leeres object-event dem writer uebergeben.
    // Die GeoPackage-Datei und die Tabelle müssen dabei erstellt werden. Die Tabelle soll 0 Objekte enthalten.
    // In diesem Test wird die td gesetzt.
    @Test
    public void setModel_emptyObject_Ok() throws IoxException, IOException {
        Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
        GeoPackageWriter writer = null;
        File file = new File(TEST_OUT,"setModel_emptyObject_Ok.gpkg");
        try {
            writer = new GeoPackageWriter(file, "empty_object_ok");
            writer.setModel(td);
            writer.setDefaultSridCode("2056");
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(objSuccess));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
        	e.printStackTrace();
        } finally {
            if (writer!=null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }
        // check if empty table was created
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT count(attrpoint) FROM empty_object_ok");
                while (rs.next()) {
                    assertEquals(0, rs.getInt(1));
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        // check if gpkg_contents is updated
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT count(*) FROM gpkg_contents WHERE table_name = 'empty_object_ok'");
                while (rs.next()) {
                    assertEquals(1, rs.getInt(1));
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        // check if gpkg_geometry_columns is updated
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT column_name FROM gpkg_geometry_columns WHERE table_name = 'empty_object_ok'");
                while (rs.next()) {
                    assertEquals("attrpoint", rs.getString(1));
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }        
    }
    
    // In diesem Test wird innerhalb der basket-events, ein leeres object-event dem writer uebergeben.
    // Die GeoPackage-Datei muss dabei erstellt werden, jedoch keine leere Tabelle.
    // In diesem Test wird die td NICHT gesetzt.
    @Test
    public void emptyObject_Ok() throws IoxException, IOException {
        Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
        GeoPackageWriter writer = null;
        File file = new File(TEST_OUT,"emptyObject_Ok.gpkg");
        try {
            writer = new GeoPackageWriter(file, "empty_object_ok");
            writer.setDefaultSridCode("2056");
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(objSuccess));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
            e.printStackTrace();
        } finally {
            if (writer!=null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }
        // check if there is no table in the geopackage file
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT count(*) FROM gpkg_contents");
                while (rs.next()) {
                    assertEquals(0, rs.getInt(1));
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    // In diesem Test wird versucht nochmals eine Tabelle mit gleichem Namen in der Geopackage-Datei anzulegen.
    // Es wird eine IoxException geworfen.
    // In diesem Test wird die td gesetzt.
    @Test
    public void setModel_emptyObject_Fail() throws IoxException, IOException {
        Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
        GeoPackageWriter writer = null;
        try {
            File file = new File(TEST_IN,"emptyObject_Ok.gpkg");
            writer = new GeoPackageWriter(file, "empty_object_ok");
            writer.setModel(td);
            writer.setDefaultSridCode("2056");
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(objSuccess));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
            fail();
        } catch(IoxException e) {
            assertEquals("[SQLITE_ERROR] SQL error or missing database (table empty_object_ok already exists)", e.getMessage());
        } finally {
            if (writer!=null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }
    }
    
	// Der Benutzer gibt 3 Models an.
	// Es wird getestet, ob der Name des Models (Name des Models: StadtModel.ili) stimmt.
	// Es wird getestet, ob der Name des Topics (Topic: Topic1) stimmt.
	// Es wird getestet, ob der Name der Klasse (Class: Polygon) stimmt.
	// Das Model, welches als letztes angegeben wird, wird zuerst auf die Zielklasse kontrolliert.
	@Test
    public void setMultipleModels_ClassFoundInLastInputModel_Ok() throws IoxException, Ili2cFailure, IOException{
		Iom_jObject objSurfaceSuccess=new Iom_jObject("BundesModel.Topic1.Polygon", "o1");
		objSurfaceSuccess.setattrvalue("id", "10");
		IomObject multisurfaceValue=objSurfaceSuccess.addattrobj(GPKG_THE_GEOM, "MULTISURFACE");
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
        GeoPackageWriter writer = null;
		// ili-datei lesen
		TransferDescription tdM=null;
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntryConditionClass=new FileEntry(TEST_IN+"/StadtModel.ili", FileEntryKind.ILIMODELFILE); // first input model
		ili2cConfig.addFileEntry(fileEntryConditionClass);
		FileEntry fileEntry=new FileEntry(TEST_IN+"/KantonModel.ili", FileEntryKind.ILIMODELFILE); // second input model
		ili2cConfig.addFileEntry(fileEntry);
		FileEntry fileEntry2=new FileEntry(TEST_IN+"/BundesModel.ili", FileEntryKind.ILIMODELFILE); // third input model
		ili2cConfig.addFileEntry(fileEntry2);
		tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		assertNotNull(tdM);
		File file = new File(TEST_OUT,"classFoundInLastInputModel_Ok.gpkg");
		try {
			writer = new GeoPackageWriter(file, "class_found_in_last_input_model_ok");
			writer.setModel(tdM);
			writer.setDefaultSridCode("2056");
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("BundesModel.Topic1","bid1"));
			
			writer.write(new ObjectEvent(objSurfaceSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if (writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
        // check if there is a polygon in the table
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
            	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT the_geom FROM class_found_in_last_input_model_ok");
                while (rs.next()) {
                	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
                	assertEquals("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5888311688311687}, COORD {C1 -0.15857142857142853, C2 0.5888311688311687}, COORD {C1 -0.22857142857142854, C2 0.5688311688311687}]}}}}}",
                			iomGeom.toString());
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } catch (ParseException e) {
				e.printStackTrace();
                fail();
			} finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
	}
    
    // In diesem Test wird eine Punkt-Geometrie in eine neue Tabelle geschrieben.
    // In diesem Test wird die td gesetzt.
    @Test
    public void setModel_point_Ok() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
        GeoPackageWriter writer = null;
        File file = new File(TEST_OUT,"setModel_point_Ok.gpkg");
        try {
            writer = new GeoPackageWriter(file, "point_ok");
            writer.setModel(td);
            writer.setDefaultSridCode("2056");
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(objSuccess));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
            // TODO
        } finally {
            if (writer!=null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }
        // check if there is a point in the table
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
            	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT attrpoint FROM point_ok");
                while (rs.next()) {
                	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
                	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
                			iomGeom.toString());
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } catch (ParseException e) {
				e.printStackTrace();
                fail();
			} finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    // In diesem Test wird eine Punkt-Geometrie und drei Attribute in eine neue Tabelle geschrieben.
    // In diesem Test wird die td gesetzt.
	@Test
	public void setModel_pointAttribute_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject("Test1.Topic1.Point2", "o1");
		inputObj.setattrvalue("id1", "1");
		inputObj.setattrvalue("aText", "text1");
		inputObj.setattrvalue("aDouble", "53434");
		inputObj.setattrvalue("aBoolean", "true");
		inputObj.setattrvalue("aDate", "2018-07-24");
		inputObj.setattrvalue("aDatetime", "2018-07-24T19:51:35.123");
		inputObj.setattrvalue("aBlob", "iVBORw0KGgoAAAANSUhEUgAAAEcAAAAjCAIAAABJt4AEAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAALiMAAC4jAXilP3YAAAAYdEVYdFNvZnR3YXJlAHBhaW50Lm5ldCA0LjAuOWwzfk4AAABSSURBVFhH7c8xDQAwDAPB8IcSJqVSEu3UoWHw1ks32OPX6UDzZ3hrrxBWcVjFYRWHVRxWcVjFYRWHVRxWcVjFYRWHVRxWcVjFYRXHV5Vl/gRdFz8WhOvgDqIcAAAAAElFTkSuQmCC");
		IomObject coordValue=inputObj.addattrobj("attrPoint2", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
        GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"Point2.gpkg");
		try {
            writer = new GeoPackageWriter(file, "point_ok");
			writer.setModel(td);
            writer.setDefaultSridCode("2056");
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if (writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
        // check if all attributes and the geom is available
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
            	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT id1, atext, adouble, aboolean, adate, adatetime, ablob, attrpoint2 FROM point_ok");
                while (rs.next()) {
    				assertEquals(1, rs.getInt(1));
    				assertEquals("text1", rs.getString(2));
    				assertEquals(53434, rs.getDouble(3), 0.0001);
    				assertEquals(true, rs.getBoolean(4));
    				assertEquals("2018-07-24", rs.getString(5));
    				assertEquals("2018-07-24T19:51:35.123Z", rs.getString(6));
	            	byte[] aboolean = rs.getBytes(7);
	            	assertEquals("iVBORw0KGgoAAAANSUhEUgAAAEcAAAAjCAIAAABJt4AEAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAALiMAAC4jAXilP3YAAAAYdEVYdFNvZnR3YXJlAHBhaW50Lm5ldCA0LjAuOWwzfk4AAABSSURBVFhH7c8xDQAwDAPB8IcSJqVSEu3UoWHw1ks32OPX6UDzZ3hrrxBWcVjFYRWHVRxWcVjFYRWHVRxWcVjFYRWHVRxWcVjFYRXHV5Vl/gRdFz8WhOvgDqIcAAAAAElFTkSuQmCC",
	            			 Base64.getEncoder().encodeToString(aboolean));
                	IomObject iomGeom = gpkg2iox.read(rs.getBytes(8));
                	assertEquals("COORD {C1 -0.4025974025974026, C2 1.3974025974025972}",
                			iomGeom.toString());
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } catch (ParseException e) {
				e.printStackTrace();
                fail();
			} finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
	}
		
	// Es wird getestet, ob eine Fehlermeldung ausgegeben wird, wenn eine Coord in einen Point konvertiert wird.
    // In diesem Test wird die td NICHT gesetzt.
	@Test
	public void point_Ok() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
        GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"NoModelSetPointOk.gpkg");
		try {
			writer = new GeoPackageWriter(file, "no_model_set_point_ok");
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} catch(IoxException e) {
			throw new IoxException(e);
		} finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
        // check if point is written into new table
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
            	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
                conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT attrpoint FROM no_model_set_point_ok");
                while (rs.next()) {
                	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
                	assertEquals("COORD {C1 -0.22857142857142854, C2 0.5688311688311687}",
                			iomGeom.toString());
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            } catch (ParseException e) {
				e.printStackTrace();
                fail();
			} finally {
                if (conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
	}

	// Es wird getestet, ob MULTICOORD geschrieben werden kann.
    // In diesem Test wird die td NICHT gesetzt.
	@Test
	public void multiPoint_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objSuccessFormat=new Iom_jObject("Test1.Topic1.MultiPoint", "o1");
		IomObject multiCoordValue=objSuccessFormat.addattrobj("attrMPoint", "MULTICOORD");
		IomObject coordValue1=multiCoordValue.addattrobj("coord", "COORD");
		coordValue1.setattrvalue("C1", "-0.22857142857142854");
		coordValue1.setattrvalue("C2", "0.5688311688311687");
		
		IomObject coordValue2=multiCoordValue.addattrobj("coord", "COORD");
		coordValue2.setattrvalue("C1", "-0.19220779220779216");
		coordValue2.setattrvalue("C2", "0.6935064935064934");
		
		IomObject coordValue3=multiCoordValue.addattrobj("coord", "COORD");
		coordValue3.setattrvalue("C1", "-0.48831168831168836");
		coordValue3.setattrvalue("C2", "0.32727272727272716");

		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"MultiPoint.gpkg");
		try {
			writer = new GeoPackageWriter(file, "multi_point");
			//writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccessFormat));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if (writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	    // check if multipoint is written into new table
	    {
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT attrmpoint FROM multi_point");
	            while (rs.next()) {
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
	            	assertEquals("MULTICOORD {coord [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.19220779220779216, C2 0.6935064935064934}, COORD {C1 -0.48831168831168836, C2 0.32727272727272716}]}",
	            			iomGeom.toString());
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	    }
	}
	
	// Es wird getestet, ob MULTICOORD geschrieben werden kann. Zusaetzlich werden Attribute erstellt.
    // In diesem Test wird die td NICHT gesetzt.
	@Test
	public void multiPointAttribute_Ok() throws IoxException, IOException, Ili2cFailure {
		Iom_jObject objSuccessFormat=new Iom_jObject("Test1.Topic1.MultiPoint2", "o1");
		objSuccessFormat.setattrvalue("textattr2", "text1");
		IomObject multiCoordValue=objSuccessFormat.addattrobj("multipoint2", "MULTICOORD");
		IomObject coordValue1=multiCoordValue.addattrobj("coord", "COORD");
		coordValue1.setattrvalue("C1", "-0.22857142857142854");
		coordValue1.setattrvalue("C2", "0.5688311688311687");
		
		IomObject coordValue2=multiCoordValue.addattrobj("coord", "COORD");
		coordValue2.setattrvalue("C1", "-0.19220779220779216");
		coordValue2.setattrvalue("C2", "0.6935064935064934");
		
		IomObject coordValue3=multiCoordValue.addattrobj("coord", "COORD");
		coordValue3.setattrvalue("C1", "-0.48831168831168836");
		coordValue3.setattrvalue("C2", "0.32727272727272716");
		
		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"MultiPointAttr.gpkg");
		try {
			writer = new GeoPackageWriter(file, "multi_point_attr");
			//writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccessFormat));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	    // check if multipoint w/ attributes is written into new table
	    {
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT multipoint2, textattr2 FROM multi_point_attr");
	            while (rs.next()) {
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
	            	assertEquals("MULTICOORD {coord [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.19220779220779216, C2 0.6935064935064934}, COORD {C1 -0.48831168831168836, C2 0.32727272727272716}]}",
	            			iomGeom.toString());
	            	assertEquals("text1", rs.getString(2));
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	    }
	}
	
	// Es wird getestet, ob eine POLYLINE geschrieben werden kann. Zusaetzlich werden Attribute erstellt.
    // In diesem Test wird die td gesetzt.
	@Test
	public void setModel_lineStringAttributes_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objStraightsSuccess=new Iom_jObject("Test1.Topic1.LineString2", "o1");
		objStraightsSuccess.setattrvalue("attr1LS", "text1");
		objStraightsSuccess.setattrvalue("attr2LS", "5");
		IomObject polylineValue=objStraightsSuccess.addattrobj("attrLineString2", "POLYLINE");
		IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
		IomObject coordStart=segments.addattrobj("segment", "COORD");
		IomObject coordEnd=segments.addattrobj("segment", "COORD");
		coordStart.setattrvalue("C1", "-0.22857142857142854");
		coordStart.setattrvalue("C2", "0.5688311688311687");
		coordEnd.setattrvalue("C1", "-0.22557142857142853");
		coordEnd.setattrvalue("C2", "0.5658311688311687");
		File file = new File(TEST_OUT,"LineString2.gpkg");
		GeoPackageWriter writer = null;
		try {
			writer = new GeoPackageWriter(file, "linestring_2");
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objStraightsSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
		// check if linestring w/ attributes is written into new table
	    {
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT attrlinestring2, attr1ls, attr2ls FROM linestring_2");
	            while (rs.next()) {
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
	            	System.out.println(iomGeom);
	            	assertEquals("POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.22557142857142853, C2 0.5658311688311687}]}}",
	            			iomGeom.toString());
	            	assertEquals("text1", rs.getString(2));
	            	assertEquals(5, rs.getInt(3));
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	    }
	} 	    

	// Es wird getestet, ob eine MULTIPOLYLINE geschrieben werden kann. Zusaetzlich werden Attribute erstellt.
    // In diesem Test wird die td NICHT gesetzt.
	@Ignore("Returns POLYLINE instead of MULTIPOLYLINE. Where's the bug/misunderstanding?")
	// https://github.com/claeis/ili2db/issues/193
	@Test
	public void multiLineStringAttributes_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objStraightsSuccess=new Iom_jObject("Test1.Topic1.MultiLineString2", "o1");
		IomObject multiPolylineValue=objStraightsSuccess.addattrobj("attrMLineString2", "MULTIPOLYLINE");
		objStraightsSuccess.setattrvalue("attr1MLS", "text2");
		objStraightsSuccess.setattrvalue("attr2MLS", "6");
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
		coordStart2.setattrvalue("C1", "-0.223557142857142853");
		coordStart2.setattrvalue("C2", "0.5658311688311687");
		coordEnd2.setattrvalue("C1", "-0.22755142857142853");
		coordEnd2.setattrvalue("C2", "0.5558351688311687");
		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"MultiLineString2.gpkg");
		try {
			writer = new GeoPackageWriter(file, "multi_linestring_2");
			//writer.setModel(td);
			writer.setDefaultSridCode("2056");
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objStraightsSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if (writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
	    {
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT attrmlinestring2, attr1mls, attr2mls FROM multi_linestring_2");
	            while (rs.next()) {	            	
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
	            	assertEquals("MULTIPOLYLINE {polyline [POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.22557142857142853, C2 0.5658311688311687}]}}, POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22355714285714284, C2 0.5658311688311687}, COORD {C1 -0.22755142857142854, C2 0.5558351688311687}]}}]}",
	            			iomGeom.toString());
	            	assertEquals("text2", rs.getString(2));
	            	assertEquals(6, rs.getInt(3));
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	    }
	}
	
	// Es wird getestet, ob eine SURFACE geschrieben werden kann. 
    // In diesem Test wird die td gesetzt.
	// Bemerkung: Das Iom-Modell erwartet immer eine MULTISURFACE (in diesem Fall mit nur einer SURFACE drin).
	@Test
	public void setModel_polygon_Ok() throws IoxException, IOException, Ili2cFailure {
		Iom_jObject objSurfaceSuccess=new Iom_jObject("Test1.Topic1.Polygon", "o1");
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
		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"Polygon.gpkg");
		try {
			writer = new GeoPackageWriter(file, "polygon");
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
		// check
	    {
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT attrpolygon FROM polygon");
	            while (rs.next()) {
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
	            	assertEquals("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5888311688311687}, COORD {C1 -0.15857142857142853, C2 0.5888311688311687}, COORD {C1 -0.22857142857142854, C2 0.5688311688311687}]}}}}}",
	            			iomGeom.toString());
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	    }
	}
	
	// Es wird getestet, ob eine SURFACE und zusätzliche Attribute geschrieben werden können. 
    // In diesem Test wird die td gesetzt.
	@Test
	public void setModel_polygonAttributes_Ok() throws IoxException, IOException, Ili2cFailure {
		Iom_jObject objSurfaceSuccess=new Iom_jObject("Test1.Topic1.Polygon2", "o1");
		objSurfaceSuccess.setattrvalue("attr1PG","text2");
		objSurfaceSuccess.setattrvalue("attr2PG","6");
		IomObject multisurfaceValue=objSurfaceSuccess.addattrobj("attrPolygon2", "MULTISURFACE");
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
		
		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"Polygon2.gpkg");
		try {
			writer = new GeoPackageWriter(file, "polygon_2");
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
		// check
		{
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT attrpolygon2, attr1pg, attr2pg FROM polygon_2");
	            while (rs.next()) {
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
	            	System.out.println(iomGeom);
	            	assertEquals("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.22857142857142854, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5688311688311687}, COORD {C1 -0.15857142857142853, C2 0.5888311688311687}, COORD {C1 -0.15857142857142853, C2 0.5888311688311687}, COORD {C1 -0.22857142857142854, C2 0.5688311688311687}]}}}}}",
	            			iomGeom.toString());
	            	assertEquals("text2", rs.getString(2));
	            	assertEquals(6, rs.getInt(3));
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
		}
	}
	
	// Es wird getestet, ob eine MULTISURFACE geschrieben werden kann. 
    // In diesem Test wird die td NICHT gesetzt.
	@Test
	public void multiPolygon_Ok() throws IoxException, IOException, Ili2cFailure {
		Iom_jObject objSurfaceSuccess=new Iom_jObject("Test1.Topic1.MultiPolygon", "o1");
		IomObject multisurfaceValue=objSurfaceSuccess.addattrobj("attrMultiPolygon", "MULTISURFACE");
		
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
		
		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"MultiPolygon.gpkg");
		try {
			writer = new GeoPackageWriter(file, "multipolygon");
			//writer.setModel(td);
			writer.setDefaultSridCode("2056");
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
		// check
		{
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT attrmultipolygon FROM multipolygon");
	            while (rs.next()) {
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
	            	assertEquals("MULTISURFACE {surface [SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.228, C2 0.568}, COORD {C1 -0.158, C2 0.568}, COORD {C1 -0.158, C2 0.568}, COORD {C1 -0.158, C2 0.588}, COORD {C1 -0.158, C2 0.588}, COORD {C1 -0.228, C2 0.568}]}}}}, SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 0.228, C2 1.3}, COORD {C1 0.158, C2 1.568}, COORD {C1 0.158, C2 1.568}, COORD {C1 0.158, C2 0.5}, COORD {C1 0.158, C2 0.5}, COORD {C1 0.228, C2 1.3}]}}}}]}",
	            			iomGeom.toString());
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
		}
	}
	
	// TODO
	// Es wird getestet, ob eine MULTISURFACE und zusätzliche Attribute geschrieben werden können. 
    // In diesem Test wird die td NICHT gesetzt.
	@Test
	public void multiPolygonAttributes_Ok() throws IoxException, IOException, Ili2cFailure {
		Iom_jObject objSurfaceSuccess=new Iom_jObject("Test1.Topic1.MultiPolygon2", "o1");
		objSurfaceSuccess.setattrvalue("attr1MPG","text3");
		objSurfaceSuccess.setattrvalue("attr2MPG","8");
		IomObject multisurfaceValue=objSurfaceSuccess.addattrobj("attrMultiPolygon2", "MULTISURFACE");
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
		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"MultiPolygon2.gpkg");
		try {
			writer = new GeoPackageWriter(file, "multipolygon_2");
//			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if(writer!=null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
		// check
		{
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT attrmultipolygon2, attr1mpg, attr2mpg FROM multipolygon_2");
	            while (rs.next()) {
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(1));
	            	assertEquals("MULTISURFACE {surface [SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.228, C2 0.568}, COORD {C1 -0.158, C2 0.568}, COORD {C1 -0.158, C2 0.568}, COORD {C1 -0.158, C2 0.588}, COORD {C1 -0.158, C2 0.588}, COORD {C1 -0.228, C2 0.568}]}}}}, SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 0.228, C2 1.3}, COORD {C1 0.158, C2 1.568}, COORD {C1 0.158, C2 1.568}, COORD {C1 0.158, C2 0.5}, COORD {C1 0.158, C2 0.5}, COORD {C1 0.228, C2 1.3}]}}}}]}",
	            			iomGeom.toString());
	            	assertEquals("text3", rs.getString(2));
	            	assertEquals("8", rs.getString(3));	            	
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
		}

	}
	
    // In diesem Test wird eine Punkt-Geometrie und mehrere Attribute in eine neue Tabelle geschrieben.
	// Dabei wird die Methode setAttributeDescriptors(attrDescs) verwendet.
    // In diesem Test wird die td NICHT gesetzt.
	@Test
	public void setAttrDesc_pointAttribute_Ok() throws IoxException, IOException, Ili2cFailure {
		AttributeDescriptor[] attrDescs = new AttributeDescriptor[9];
		
		String id1_attr = "id1";
		AttributeDescriptor id1AttrDesc = new AttributeDescriptor();
		id1AttrDesc.setDbColumnName(id1_attr.toLowerCase());
		id1AttrDesc.setIomAttributeName(id1_attr);
		id1AttrDesc.setDbColumnTypeName("INTEGER");
		attrDescs[0] = id1AttrDesc;
		
		String text_attr = "aText";
		AttributeDescriptor textAttrDesc = new AttributeDescriptor();
		textAttrDesc.setDbColumnName(text_attr.toLowerCase());
		textAttrDesc.setIomAttributeName(text_attr);
		textAttrDesc.setDbColumnTypeName("TEXT");
		attrDescs[1] = textAttrDesc;

		String double_attr = "aDouble";
		AttributeDescriptor doubleAttrDesc = new AttributeDescriptor();
		doubleAttrDesc.setDbColumnName(double_attr.toLowerCase());
		doubleAttrDesc.setIomAttributeName(double_attr);
		doubleAttrDesc.setDbColumnTypeName("REAL");
		attrDescs[2] = doubleAttrDesc;

		String geom_attr = "attrPoint2";
		AttributeDescriptor geomAttrDesc = new AttributeDescriptor();
		geomAttrDesc.setDbColumnName(geom_attr.toLowerCase());
		geomAttrDesc.setIomAttributeName(geom_attr);
		geomAttrDesc.setDbColumnGeomTypeName("POINT");
		geomAttrDesc.setSrId(2056);
		geomAttrDesc.setCoordDimension(2);
		attrDescs[3] = geomAttrDesc;

		String date_attr = "aDate";
		AttributeDescriptor dateAttrDesc = new AttributeDescriptor();
		dateAttrDesc.setDbColumnName(date_attr.toLowerCase());
		dateAttrDesc.setIomAttributeName(date_attr);
		dateAttrDesc.setDbColumnTypeName("DATE");
		attrDescs[4] = dateAttrDesc;

		String int_attr = "aInt";
		AttributeDescriptor intAttrDesc = new AttributeDescriptor();
		intAttrDesc.setDbColumnName(int_attr.toLowerCase());
		intAttrDesc.setIomAttributeName(int_attr);
		intAttrDesc.setDbColumnTypeName("INTEGER");
		attrDescs[5] = intAttrDesc;

		String datetime_attr = "aDateTime";
		AttributeDescriptor datetimeAttrDesc = new AttributeDescriptor();
		datetimeAttrDesc.setDbColumnName(datetime_attr.toLowerCase());
		datetimeAttrDesc.setIomAttributeName(datetime_attr);
		datetimeAttrDesc.setDbColumnTypeName("DATETIME");
		attrDescs[6] = datetimeAttrDesc;
				
		String boolean_attr = "aBoolean";
		AttributeDescriptor booleanAttrDesc = new AttributeDescriptor();
		booleanAttrDesc.setDbColumnName(boolean_attr.toLowerCase());
		booleanAttrDesc.setIomAttributeName(boolean_attr);
		booleanAttrDesc.setDbColumnTypeName("BOOLEAN");
		attrDescs[7] = booleanAttrDesc;

		String blob_attr = "aBlob";
		AttributeDescriptor blobAttrDesc = new AttributeDescriptor();
		blobAttrDesc.setDbColumnName(blob_attr.toLowerCase());
		blobAttrDesc.setIomAttributeName(blob_attr);
		blobAttrDesc.setDbColumnTypeName("BLOB");
		attrDescs[8] = blobAttrDesc;
		
		Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Point2", "o1");
		inputObj.setattrvalue(id1_attr, "1");
		inputObj.setattrvalue(text_attr, "text1");
		inputObj.setattrvalue(double_attr, "53434.1234");
		IomObject coordValue=inputObj.addattrobj(geom_attr, "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		inputObj.setattrvalue(date_attr, "2017-04-22");
		inputObj.setattrvalue(int_attr, "1234"); 
		inputObj.setattrvalue(datetime_attr, "2017-04-22T10:23:54.123");
		inputObj.setattrvalue(boolean_attr, "true");
		inputObj.setattrvalue(blob_attr, "iVBORw0KGgoAAAANSUhEUgAAAEcAAAAjCAIAAABJt4AEAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAALiMAAC4jAXilP3YAAAAYdEVYdFNvZnR3YXJlAHBhaW50Lm5ldCA0LjAuOWwzfk4AAABSSURBVFhH7c8xDQAwDAPB8IcSJqVSEu3UoWHw1ks32OPX6UDzZ3hrrxBWcVjFYRWHVRxWcVjFYRWHVRxWcVjFYRWHVRxWcVjFYRXHV5Vl/gRdFz8WhOvgDqIcAAAAAElFTkSuQmCC");

		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"Point2.gpkg");
		try {
			writer = new GeoPackageWriter(file, "point_2");
			writer.setAttributeDescriptors(attrDescs);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		} finally {
	    	if (writer != null) {
	    		try {
					writer.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		writer=null;
	    	}
		}
		// check
	    {
	        Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;
	        try {
	        	Gpkg2iox gpkg2iox = new Gpkg2iox(); 
	            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
	            stmt = conn.createStatement();
	            rs = stmt.executeQuery("SELECT id1, atext, adouble, attrpoint2, adate, aint, adatetime, aboolean, ablob FROM point_2");
	            while (rs.next()) {
	            	assertEquals(1, rs.getInt(1));
	            	assertEquals("text1", rs.getString(2));
	            	assertEquals(53434.1234, rs.getDouble(3), 0.0001);
	            	IomObject iomGeom = gpkg2iox.read(rs.getBytes(4));
	            	assertEquals("COORD {C1 -0.4025974025974026, C2 1.3974025974025972}",
	            			iomGeom.toString());
	            	assertEquals("2017-04-22", rs.getString(5));
	            	assertEquals(1234, rs.getInt(6));
	            	assertEquals("2017-04-22T10:23:54.123Z", rs.getString(7));
	            	assertEquals(1, rs.getInt(8));
	            	byte[] aboolean = rs.getBytes(9);
	            	assertEquals("iVBORw0KGgoAAAANSUhEUgAAAEcAAAAjCAIAAABJt4AEAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAALiMAAC4jAXilP3YAAAAYdEVYdFNvZnR3YXJlAHBhaW50Lm5ldCA0LjAuOWwzfk4AAABSSURBVFhH7c8xDQAwDAPB8IcSJqVSEu3UoWHw1ks32OPX6UDzZ3hrrxBWcVjFYRWHVRxWcVjFYRWHVRxWcVjFYRWHVRxWcVjFYRXHV5Vl/gRdFz8WhOvgDqIcAAAAAElFTkSuQmCC",
	            			 Base64.getEncoder().encodeToString(aboolean));
	            }
	            rs.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	            fail();
	        } catch (ParseException e) {
				e.printStackTrace();
	            fail();
			} finally {
	            if (conn != null){
	                try {
	                    conn.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	    }
	}
	
	// Das Model wird gesetzt. Es soll eine Fehlermeldung ausgegeben werden,
	// weil die Klasse innerhalb des angegebenen Modells nicht gefunden werden kann.
	@Test
	public void classOfModelNotFound_Fail() throws IoxException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point99", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"classOfModelNotFound_Fail.gpkg");
		try {
			writer = new GeoPackageWriter(file, "class_of_model_not_found_fail");
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		} catch(IoxException e) {
			assertTrue(e.getMessage().contains("Test1.Topic1.Point99"));
			assertTrue(e.getMessage().contains("not found in"));
			assertTrue(e.getMessage().contains("Test1"));
		} finally {
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
	
	// Es wird getestet, ob eine Fehlermeldung ausgegeben wird, wenn die Coord nicht konvertiert werden kann.
	@Test
	public void failtedToConvertCoord_Fail() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C3", "0.5688311688311687");
		GeoPackageWriter writer = null;
		File file = new File(TEST_OUT,"failedToConvertToCoord_Fail.gpkg");
		try {
			writer = new GeoPackageWriter(file, "failed_to_convert_coord");
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		} catch(IoxException e) {
			assertTrue(e.getMessage().contains("failed to read C2 <null>"));
		} finally {
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
