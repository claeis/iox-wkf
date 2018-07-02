package ch.interlis.ioxwkf.gpkg;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

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
import ch.interlis.ioxwkf.shp.ShapeReader;;

public class GeoPackageReaderTest {
    
    private static final String TEST_IN="src/test/data/GpkgReader/";

    // Es wird getestet ob ein Point Element in ein Interlis IomObject convertiert werden kann.
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
    
    // Es wird getestet ob ein Model an den Reader gegeben werden kann und die objecttag Informationen des IomObjects
    // mit den Informationen des Models uebereinstimmen.
    @Test
    public void setModel_singlePoint_Ok() throws IoxException, IOException, Ili2cFailure{
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
//            IoxEvent event=reader.read();
//            assertTrue(event instanceof ObjectEvent);
//            IomObject iomObj=((ObjectEvent)event).getIomObject();
//            IomObject attrObj=iomObj.getattrobj("geometry", 0);
//            assertTrue(iomObj.getobjecttag().equals("ShapeModel.Topic1.Point"));
//            assertTrue(attrObj.getattrvalue("C1").equals("-0.22857142857142854"));
//            assertTrue(attrObj.getattrvalue("C2").equals("0.5688311688311687"));
//            assertTrue(reader.read() instanceof EndBasketEvent);
//            assertTrue(reader.read() instanceof EndTransferEvent);
        }finally {
            if(reader!=null) {
                reader.close();
                reader=null;
            }
        }
    }

}
