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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.io.ParseException;

import ch.ehi.ili2gpkg.Gpkg2iox;
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
import ch.interlis.ioxwkf.shp.ShapeReader;
import ch.interlis.ioxwkf.shp.ShapeWriter;

// TODO:
// - curved geometries -> fail
// - blob
// - boolean

public class GeoPackageWriterTest {

    private final static String TEST_IN="src/test/data/GpkgWriter";
    private final static String TEST_OUT="build/test/data/GpkgWriter";
    private static final String GPKG_THE_GEOM = "the_geom";
    private TransferDescription td=null;

    @BeforeClass
    public static void setupFolder() throws Ili2cFailure {
        new File(TEST_OUT).mkdirs();
        
        for (File f : new File(TEST_OUT).listFiles()) {
            if (f.getName().endsWith("gpkg")) {
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

    // TODO: mit/ohne Erstellen der Tabelle?
    // In diesem Test wird ein transfer-event gestartet und gleich wieder beendet.
    // Die GeoPackage-Datei muss dabei erstellt werden, darf jedoch keinen Inhalt aufweisen.
    // In diesem Test wird die td gesetzt.
    @Test
    public void setModel_emptyTransfer_Ok() throws IoxException, IOException {
        assertTrue(true);
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
            fail();
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
    
	// Der Benutzer gibt 3 models an.
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
		inputObj.setattrvalue("Text", "text1");
		inputObj.setattrvalue("Double", "53434");
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
                rs = stmt.executeQuery("SELECT id1, text, double, attrpoint2 FROM point_ok");
                while (rs.next()) {
    				assertEquals(1, rs.getInt(1));
    				assertEquals("text1", rs.getString(2));
    				assertEquals(53434, rs.getDouble(3), 0.0001);

                	IomObject iomGeom = gpkg2iox.read(rs.getBytes(4));
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
	
	// TODO: 
    // In diesem Test wird eine Punkt-Geometrie und mehrere Attribute in eine neue Tabelle geschrieben.
	// Dabei wird die Methode setAttributeDescriptors(attrDescs) verwendet.
    // In diesem Test wird die td NICHT gesetzt.
	@Test
	public void setAttrDesc_pointAttribute_Ok() throws IoxException, IOException, Ili2cFailure{

		
		
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

	// Es wird getestet ob MULTICOORD geschrieben werden kann
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

		
		System.out.println(objSuccessFormat);
	}
}
