package ch.interlis.ioxwkf.gpkg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;

// TODO:
// - curved geometries -> fail
// - blob
// - boolean

public class GeoPackageWriterTest {

    private final static String TEST_IN="src/test/data/GpkgWriter";
    private final static String TEST_OUT="build/test/data/GpkgWriter";
    private TransferDescription td=null;

    @BeforeClass
    public static void setupFolder() throws Ili2cFailure {
        new File(TEST_OUT).mkdirs();
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
         }finally {
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
    // Die GeoPackage-Datei und die Tabelle müssen dabei erstellt werden. Die Tabelle soll 1 Objekt enthalten, welches jedoch keinen Inhalt aufweisen darf.
    // In diesem Test wird die td gesetzt.
    @Test
    public void setModel_emptyObject_Ok() throws IoxException, IOException {
        Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
        GeoPackageWriter writer = null;
        try {
            File file = new File(TEST_OUT,"emptyObject_Ok.gpkg");
            writer = new GeoPackageWriter(file, "empty_object_ok");
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(objSuccess));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
            assertEquals("no feature found in Test1.Topic1.Point", e.getMessage());
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

        assertTrue(true);
    }

}
