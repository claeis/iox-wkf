package ch.interlis.ioxwkf.gpkg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.junit.Ignore;
import org.junit.Test;

import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.ioxwkf.gpkg.GeoPackageReader;
import ch.interlis.ioxwkf.shp.ShapeReader;

public class GeoPackageReaderTest {
    
    private static final String TEST_IN="src/test/data/GpkgReader/";
    private static final String GPKG_THE_GEOM = "geom";

    // Es wird getestet, ob ein Point Element in ein Interlis IomObject convertiert werden kann.
    @Test
    public void singlePoint_Ok() throws IoxException, IOException{
        GeoPackageReader reader = null;
        try {
            reader = new GeoPackageReader(new File(TEST_IN+"Point/Point2d.gpkg"), "point2d");
            assertTrue(true); // TODO: remove
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read(); // read the only feature in the table
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607630.91"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228592.976"));
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if(reader != null) {
                reader.close();
                reader = null;
            }
        }
    }
    
    // Es wird getestet, ob ein Model an den Reader gegeben werden kann und die objecttag Informationen des IomObjects
    // mit den Informationen des Models uebereinstimmen.
    @Test
    public void setModel_singlePoint_Ok() throws IoxException, IOException, Ili2cFailure {
        GeoPackageReader reader=null;
        try {
            // compile model
            Configuration ili2cConfig=new Configuration();
            FileEntry fileEntry=new FileEntry(TEST_IN+"Point/GpkgModel.ili", FileEntryKind.ILIMODELFILE);
            ili2cConfig.addFileEntry(fileEntry);
            TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
            assertNotNull(td);
            reader=new GeoPackageReader(new File(TEST_IN+"Point/Point2d.gpkg"), "point2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(iomObj.getobjecttag().equals("GpkgModel.Topic1.Point2d"));
            assertTrue(attrObj.getattrvalue("C1").equals("2607630.91"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228592.976"));
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob der Modellname der zu importierenden Tabelle aus der Geopackage-Datei entspricht.
    @Test
    public void modelName_Ok() throws IoxException, IOException{
        GeoPackageReader reader = null;
        try {
            reader = new GeoPackageReader(new File(TEST_IN+"Point/Point2d.gpkg"), "point2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            assertTrue(iomObj.getobjecttag().contains("point2d"));
            event=reader.read();
            assertTrue(event instanceof EndBasketEvent);
            event=reader.read();
            assertTrue(event instanceof EndTransferEvent);
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob der Modellname dem Modellnamen entspricht.
    @Test
    public void setModel_modelName_Ok() throws IoxException, IOException, Ili2cFailure {
        GeoPackageReader reader = null;
        try {
            // compile model
            Configuration ili2cConfig=new Configuration();
            FileEntry fileEntry=new FileEntry(TEST_IN+"Point/GpkgModel.ili", FileEntryKind.ILIMODELFILE);
            ili2cConfig.addFileEntry(fileEntry);
            TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
            assertNotNull(td);
            reader = new GeoPackageReader(new File(TEST_IN+"Point/Point2d.gpkg"), "point2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            assertTrue(iomObj.getobjecttag().contains("GpkgModel"));
            event=reader.read();
            assertTrue(event instanceof EndBasketEvent);
            event=reader.read();
            assertTrue(event instanceof EndTransferEvent);
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }
    
    // Es wird getestet, ob der Topicname "Topic" entspricht.
    @Test
    public void topicName_Ok() throws IoxException, IOException{
        GeoPackageReader reader=null;
        try {
            reader = new GeoPackageReader(new File(TEST_IN+"Point/Point2d.gpkg"), "point2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            assertTrue(iomObj.getobjecttag().contains("Topic"));
            event=reader.read();
            assertTrue(event instanceof EndBasketEvent);
            event=reader.read();
            assertTrue(event instanceof EndTransferEvent);
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob der Topicname dem sich im Model befindenden Topicnamen entspricht.
    @Test
    public void setModel_topicName_Ok() throws IoxException, IOException, Ili2cFailure{
        GeoPackageReader reader=null;
        try {
            // compile model
            Configuration ili2cConfig=new Configuration();
            FileEntry fileEntry=new FileEntry(TEST_IN+"Point/GpkgModel.ili", FileEntryKind.ILIMODELFILE);
            ili2cConfig.addFileEntry(fileEntry);
            TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
            assertNotNull(td);
            reader = new GeoPackageReader(new File(TEST_IN+"Point/Point2d.gpkg"), "point2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            assertTrue(iomObj.getobjecttag().contains("Topic1"));
            event=reader.read();
            assertTrue(event instanceof EndBasketEvent);
            event=reader.read();
            assertTrue(event instanceof EndTransferEvent);
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob der Name der Klasse "Class" enthaltet.
    @Test
    public void className_Ok() throws IoxException, IOException{
        GeoPackageReader reader=null;
        try {
            reader = new GeoPackageReader(new File(TEST_IN+"Point/Point2d.gpkg"), "point2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            assertTrue(iomObj.getobjecttag().contains("Class"));
            event=reader.read();
            assertTrue(event instanceof EndBasketEvent);
            event=reader.read();
            assertTrue(event instanceof EndTransferEvent);
        } catch(Exception e) {
            throw new IoxException(e);
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob der Name der Klasse dem Modelklassennamen entspricht.
    @Ignore // Nicht mehr notwendig, da bereits mit Test 3 abgedeckt wird?
    @Test
    public void setModel_className_Ok() throws IoxException, IOException{
        GeoPackageReader reader=null;
        try {
            reader = new GeoPackageReader(new File(TEST_IN+"Point/Point2d.gpkg"), "point2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            assertTrue(iomObj.getobjecttag().contains("point2d"));
            event=reader.read();
            assertTrue(event instanceof EndBasketEvent);
            event=reader.read();
            assertTrue(event instanceof EndTransferEvent);
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob die OID der Objekte unterschiedlich sind.
    @Test
    public void oidsAreUnique_Ok() throws IoxException, IOException {
        HashSet<String> objectIds=new HashSet<String>();
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"MultiPoint/MultiPoint2d.gpkg"), "MultiPoint2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            while(!(event instanceof EndBasketEvent)) {
                IomObject iomObj=((ObjectEvent)event).getIomObject();
                if (!objectIds.contains(iomObj.getobjectoid())){
                    objectIds.add(iomObj.getobjectoid());
                    event=reader.read();
                } else {
                    fail();
                }
            }
            assertTrue(event instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } catch(Exception ex) {
            ex.getMessage().contains("expected unique object id.");
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob ein MultiPoint Element in ein Interlis IomObject convertiert werden kann.
    @Test
    public void multiPoint_Ok() throws IoxException, IOException {
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"MultiPoint/MultiPoint2d.gpkg"), "MultiPoint2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607657.233"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228644.403"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607630.91"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228592.976"));
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607666.468"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228543.775"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607673.552"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228625.437"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607627.238"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228642.533"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607565.879"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228555.122"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607593.004"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228552.176"));

            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);            
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob ein MultiPoint Element in ein Interlis IomObject konvertiert werden kann.
    // Model wurde gesetzt.
    @Test
    public void setModel_MultiPoint_Ok() throws IoxException, IOException, Ili2cFailure {
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"MultiPoint/GpkgModel.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"MultiPoint/MultiPoint2d.gpkg"), "MultiPoint2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td2);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607657.233"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228644.403"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607630.91"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228592.976"));
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607666.468"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228543.775"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607673.552"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228625.437"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607627.238"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228642.533"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607565.879"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228555.122"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607593.004"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228552.176"));

            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);            
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob ein LineString Element in ein Interlis IomObject konvertiert werden kann.
    @Test
    public void singleLineString_Ok() throws IoxException, IOException {
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"LineString/LineString2d.gpkg"), "LineString2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject multiPolylineObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            IomObject sequence=multiPolylineObj.getattrobj("sequence", 0);
            IomObject segment=sequence.getattrobj("segment", 0);
            assertTrue(segment.getattrvalue("C1").equals("2605938.955"));
            assertTrue(segment.getattrvalue("C2").equals("1229215.481"));
            IomObject segment2=sequence.getattrobj("segment", 1);
            assertTrue(segment2.getattrvalue("C1").equals("2605931.098"));
            assertTrue(segment2.getattrvalue("C2").equals("1229295.039"));
            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob ein LineString Element in ein Interlis IomObject konvertiert werden kann.
    // Model wurde gesetzt.
    @Test
    public void setModel_SingleLineString_Ok() throws IoxException, IOException, Ili2cFailure {
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"LineString/GpkgModel.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"LineString/LineString2d.gpkg"), "LineString2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td2);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject multiPolylineObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            IomObject sequence=multiPolylineObj.getattrobj("sequence", 0);
            IomObject segment=sequence.getattrobj("segment", 0);
            assertTrue(segment.getattrvalue("C1").equals("2605938.955"));
            assertTrue(segment.getattrvalue("C2").equals("1229215.481"));
            IomObject segment2=sequence.getattrobj("segment", 1);
            assertTrue(segment2.getattrvalue("C1").equals("2605931.098"));
            assertTrue(segment2.getattrvalue("C2").equals("1229295.039"));
            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }
    
    // Es wird getestet, ob zwei LineString Elemente zwei Interlis IomObjects konvertiert werden kann.
    @Test
    public void singleParallelLineString_Ok() throws IoxException, IOException {
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"LineString2/LineString2d.gpkg"), "LineString2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject multiPolylineObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            IomObject sequence=multiPolylineObj.getattrobj("sequence", 0);
            IomObject segment=sequence.getattrobj("segment", 0);
            assertTrue(segment.getattrvalue("C1").equals("2605938.955"));
            assertTrue(segment.getattrvalue("C2").equals("1229215.481"));
            IomObject segment2=sequence.getattrobj("segment", 1);
            assertTrue(segment2.getattrvalue("C1").equals("2605931.098"));
            assertTrue(segment2.getattrvalue("C2").equals("1229295.039"));
            
            assertTrue(reader.read() instanceof ObjectEvent);
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet ob ein MultiLineString Element in ein Interlis IomObject convertiert werden kann.
    @Test
    public void multiLineString_Ok() throws IoxException, IOException {
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"MultiLineString/MultiLineString2d.gpkg"), "MultiLineString2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject multiPolylineObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            IomObject polyline=multiPolylineObj.getattrobj("polyline", 0);
            
            IomObject sequence=polyline.getattrobj("sequence", 0);
            IomObject segment=sequence.getattrobj("segment", 0);
            assertTrue(segment.getattrvalue("C1").equals("-0.22857142857142854"));
            assertTrue(segment.getattrvalue("C2").equals("0.5688311688311687"));
            IomObject segment2=sequence.getattrobj("segment", 1);
            assertTrue(segment2.getattrvalue("C1").equals("-0.22557142857142853"));
            assertTrue(segment2.getattrvalue("C2").equals("0.5658311688311687"));
            
            IomObject polyline2=multiPolylineObj.getattrobj("polyline", 1);
            IomObject sequence2=polyline2.getattrobj("sequence", 0);
            IomObject segment3=sequence2.getattrobj("segment", 0);
            assertTrue(segment3.getattrvalue("C1").equals("-0.22557142857142853"));
            assertTrue(segment3.getattrvalue("C2").equals("0.5658311688311687"));
            IomObject segment4=sequence2.getattrobj("segment", 1);
            assertTrue(segment4.getattrvalue("C1").equals("-0.22755142857142854"));
            assertTrue(segment4.getattrvalue("C2").equals("0.5558351688311687"));
            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob ein MultiLineString Element in ein Interlis IomObject convertiert werden kann.
    // Model wurde gesetzt.
    @Test
    public void setModel_MultiLineString_Ok() throws IoxException, IOException, Ili2cFailure {
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"MultiLineString/GpkgModel.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"MultiLineString/MultiLineString2d.gpkg"), "MultiLineString2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td2);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject multiPolylineObj=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            {
                IomObject polyline=multiPolylineObj.getattrobj("polyline", 0);
                IomObject sequence=polyline.getattrobj("sequence", 0);
                IomObject segment=sequence.getattrobj("segment", 0);
                assertTrue(segment.getattrvalue("C1").equals("-0.22857142857142854"));
                assertTrue(segment.getattrvalue("C2").equals("0.5688311688311687"));
                IomObject segment2=sequence.getattrobj("segment", 1);
                assertTrue(segment2.getattrvalue("C1").equals("-0.22557142857142853"));
                assertTrue(segment2.getattrvalue("C2").equals("0.5658311688311687"));
            }
            {
                IomObject polyline=multiPolylineObj.getattrobj("polyline", 1);
                IomObject sequence=polyline.getattrobj("sequence", 0);
                IomObject segment=sequence.getattrobj("segment", 0);
                assertTrue(segment.getattrvalue("C1").equals("-0.22557142857142853"));
                assertTrue(segment.getattrvalue("C2").equals("0.5658311688311687"));
                IomObject segment2=sequence.getattrobj("segment", 1);
                assertTrue(segment2.getattrvalue("C1").equals("-0.22755142857142854"));
                assertTrue(segment2.getattrvalue("C2").equals("0.5558351688311687"));
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob ein SinglePolygon Element in ein Interlis IomObject konvertiert werden kann.
    @Test
    public void singlePolygon_Ok() throws IoxException, IOException{
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"Polygon/Polygon2d.gpkg"), "Polygon2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject multisurface=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            
            assertTrue(multisurface.getattrvaluecount("surface")==1);
            
            IomObject surface=multisurface.getattrobj("surface", 0);
            IomObject boundary=surface.getattrobj("boundary", 0);
            IomObject polylineObj=boundary.getattrobj("polyline", 0);
            IomObject sequence=polylineObj.getattrobj("sequence", 0);
            IomObject segment=sequence.getattrobj("segment", 0);
            assertTrue(segment.getattrvalue("C1").equals("-0.6374695863746959"));
            assertTrue(segment.getattrvalue("C2").equals("0.6618004866180048"));
            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob ein SinglePolygon Element in ein Interlis IomObject konvertiert werden kann.
    // Model wurde gesetzt.
    @Test
    public void setModel_SinglePolygon_Ok() throws IoxException, IOException, Ili2cFailure{
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"Polygon/GpkgModel.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"Polygon/Polygon2d.gpkg"), "Polygon2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td2);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject multisurface=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            
            assertTrue(multisurface.getattrvaluecount("surface")==1);
            
            IomObject surface=multisurface.getattrobj("surface", 0);
            IomObject boundary=surface.getattrobj("boundary", 0);
            IomObject polylineObj=boundary.getattrobj("polyline", 0);
            IomObject sequence=polylineObj.getattrobj("sequence", 0);
            IomObject segment=sequence.getattrobj("segment", 0);
            assertTrue(segment.getattrvalue("C1").equals("-0.6374695863746959"));
            assertTrue(segment.getattrvalue("C2").equals("0.6618004866180048"));
            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }   
    
    // Es wird getestet, ob ein MultiPolygon Element in ein Interlis IomObject konvertiert werden kann.
    @Test
    public void multiPolygon_Ok() throws IoxException, IOException{
        GeoPackageReader reader=null;
        IoxEvent event=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"MultiPolygon/MultiPolygon2d.gpkg"), "MultiPolygon2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject multisurface=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(multisurface.getattrvaluecount("surface")==1);
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            multisurface=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(multisurface.getattrvaluecount("surface")==1);
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            multisurface=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(multisurface.getattrvaluecount("surface")==1);
            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
            if (event!=null) {
                event=null;
            }
        }
    }

    // Es wird getestet ob ein MultiPolygon Element in ein Interlis IomObject convertiert werden kann.
    // Model wurde gesetzt.
    @Test
    public void setModel_multiPolygon_Ok() throws IoxException, IOException, Ili2cFailure{
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"MultiPolygon/GpkgModel.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        IomObject multisurface=null;
        GeoPackageReader reader=null;
        IoxEvent event=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"MultiPolygon/MultiPolygon2d.gpkg"), "MultiPolygon2d");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td2);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            multisurface=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(multisurface.getattrvaluecount("surface")==1);
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            multisurface=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(multisurface.getattrvaluecount("surface")==1);
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            multisurface=iomObj.getattrobj(GPKG_THE_GEOM, 0);
            assertTrue(multisurface.getattrvaluecount("surface")==1);
            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                ili2cConfig=null;
                fileEntry=null;
                multisurface=null;
                td2=null;
                reader.close();
                reader=null;
            }
            if (event!=null) {
                event=null;
            }
        }
    }
    
    // Es wird getestet, ob Attribute Elemente in Interlis IomObjects konvertiert werden koennen.
    @Test
    public void attributes_Ok() throws IoxException, IOException{
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"Attributes/Attributes.gpkg"), "Attributes");
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            String attr1=iomObj.getattrvalue("t_id"); // Integer
            assertTrue(attr1.equals("27646313"));
            String attr2=iomObj.getattrvalue("objektname"); // Text
            assertTrue(attr2.equals("St. Ursenkathedrale"));
            String attr4=iomObj.getattrvalue("orientierung"); // Double
            assertTrue(attr4.equals("18.36"));
            assertEquals("2016-10-28", iomObj.getattrvalue("nachfuehrung")); // Date
            assertEquals("2018-05-03T00:00:00Z", iomObj.getattrvalue("importdatum")); // Timestamp
            assertEquals("1", iomObj.getattrvalue("aboolean")); // Boolean            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        }finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob Attribute Elemente in Interlis IomObjects konvertiert werden koennen und die Attributenamen mit den Model Attribute Namen uebereinstimmen.
    // Model wurde gesetzt.
    @Test
    public void setModel_attributes_Ok() throws IoxException, IOException, Ili2cFailure{
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"Attributes/GpkgModelAttrs.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"Attributes/Attributes.gpkg"), "Attributes");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td2);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            String attr1=iomObj.getattrvalue("t_id"); // Integer
            assertTrue(attr1.equals("27646313"));
            String attr2=iomObj.getattrvalue("objektname"); // Text
            assertTrue(attr2.equals("St. Ursenkathedrale"));
            String attr4=iomObj.getattrvalue("orientierung"); // Double
            assertTrue(attr4.equals("18.36"));
            assertEquals("2016-10-28", iomObj.getattrvalue("nachfuehrung")); // Date
            assertEquals("2018-05-03T00:00:00Z", iomObj.getattrvalue("importdatum")); // Timestamp
            assertEquals("1", iomObj.getattrvalue("aboolean")); // Boolean            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    @Test
    public void setModel_attributesNull_Ok() throws IoxException, IOException, Ili2cFailure{
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"Attributes/AttributesNull.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"Attributes/AttributesNull.gpkg"), "AttributesNull");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td2);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            event=reader.read();
            iomObj=((ObjectEvent)event).getIomObject();
            assertEquals(null,iomObj.getattrvalue("aenum"));
            assertTrue(event instanceof ObjectEvent);
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob die Attributnamen, bei unterschiedlicher Gross/Kleinschreibung in der GeoPackage-Tabelle, gem. ili Modell gelesen werden
    @Test
    public void setModel_similarAttributes_Ok() throws IoxException, IOException, Ili2cFailure {
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"Attributes/GpkgModelSimilarAttrs.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"Attributes/AttributesSimilarAttrs.gpkg"), "AttributesSimilarAttrs");
            assertTrue(reader.read() instanceof StartTransferEvent);
            reader.setModel(td2);
            assertTrue(reader.read() instanceof StartBasketEvent);
            
            IoxEvent event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            IomObject iomObj=((ObjectEvent)event).getIomObject();
            String attr1=iomObj.getattrvalue("integer");
            assertEquals("8",attr1);
            String attr2=iomObj.getattrvalue("text");
            assertEquals("text1",attr2);
            String attr3=iomObj.getattrvalue("id");
            assertEquals("1",attr3);
            String attr4=iomObj.getattrvalue("double");
            assertEquals("53434.0",attr4);
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

    // Es wird getestet, ob mehrere Klassen mit zutreffenen Attributen zu einer Exception fuehren 
    @Test
    public void multipleClassFoundInModel_Fail() throws IoxException, Ili2cFailure, IOException {
        // ili-datei lesen
        GeoPackageReader reader=null;
        TransferDescription tdM=null;
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntryConditionClass=new FileEntry(TEST_IN+"MultipleClasses/StadtModel.ili", FileEntryKind.ILIMODELFILE); // first input model
        ili2cConfig.addFileEntry(fileEntryConditionClass);
        FileEntry fileEntry=new FileEntry(TEST_IN+"MultipleClasses/KantonModel.ili", FileEntryKind.ILIMODELFILE); // second input model
        ili2cConfig.addFileEntry(fileEntry);
        FileEntry fileEntry2=new FileEntry(TEST_IN+"MultipleClasses/BundesModel.ili", FileEntryKind.ILIMODELFILE); // third input model
        ili2cConfig.addFileEntry(fileEntry2);
        tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        assertNotNull(tdM);
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"MultipleClasses/Polygon.gpkg"), "Polygon");
            reader.setModel(tdM);
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read();
            assertEquals("BundesModel.Topic1.Polygon",((ObjectEvent)event).getIomObject().getobjecttag());
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }
    
    // Es wird getestet, dass wenn die Anzahl Attribute nicht stimmt, das zu einer Exception fuehrt
    @Test
    public void setModel_attributeCountDifferent_Fail() throws IoxException, IOException, Ili2cFailure{
        // compile model
        Configuration ili2cConfig=new Configuration();
        FileEntry fileEntry=new FileEntry(TEST_IN+"DifferentAttributeCount/GpkgModelDiffAttrCount.ili", FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        assertNotNull(td);
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"DifferentAttributeCount/LineString.gpkg"), "LineString");
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            reader.read();
            fail();
        } catch(IoxException ex){
            assertEquals("attributes 'fid,geom' not found in model: 'GpkgModel'.",ex.getMessage());
        } finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }
    
    // Es wird getestet ob eine nicht-GeoPackage-Datei zu einer Exception führt.
    @Test
    public void wrongFormat_Fail() throws IoxException, IOException {
        GeoPackageReader reader=null;
        try {
            reader=new GeoPackageReader(new File(TEST_IN+"Point/Point.shx"), "Point");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("expected valid geopackage file"));
        } finally {
            if (reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }
}
