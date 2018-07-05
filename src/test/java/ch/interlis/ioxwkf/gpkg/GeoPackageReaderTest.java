package ch.interlis.ioxwkf.gpkg;

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
            IomObject attrObj=iomObj.getattrobj("geom", 0);
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
            IomObject attrObj=iomObj.getattrobj("geom", 0);
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
            IomObject attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607657.233"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228644.403"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607630.91"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228592.976"));
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607666.468"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228543.775"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607673.552"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228625.437"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607627.238"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228642.533"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607565.879"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228555.122"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
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
            IomObject attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607657.233"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228644.403"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607630.91"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228592.976"));
            
            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607666.468"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228543.775"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607673.552"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228625.437"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607627.238"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228642.533"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
            assertTrue(attrObj.getattrvalue("C1").equals("2607565.879"));
            assertTrue(attrObj.getattrvalue("C2").equals("1228555.122"));

            event=reader.read();
            assertTrue(event instanceof ObjectEvent);
            iomObj=((ObjectEvent)event).getIomObject();
            attrObj=iomObj.getattrobj("geom", 0);
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
            IomObject multiPolylineObj=iomObj.getattrobj("geom", 0);
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
            IomObject multiPolylineObj=iomObj.getattrobj("geom", 0);
            IomObject sequence=multiPolylineObj.getattrobj("sequence", 0);
            System.out.println(sequence);
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
            IomObject multiPolylineObj=iomObj.getattrobj("geom", 0);
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


}
