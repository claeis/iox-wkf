package ch.interlis.ioxwkf.gpkg;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

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

    @Test
    public void singlePoint_Ok() throws IoxException, IOException{
        GeoPackageReader reader = null;
        try {
            reader = new GeoPackageReader(new File(TEST_IN+"point/point2d.gpkg"), "point2d");
            assertTrue(true); // TODO: remove
            assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof StartBasketEvent);
            IoxEvent event=reader.read(); // read the only feature in the table
            System.out.println(event.toString());
            assertTrue(event instanceof ObjectEvent);
            
            System.out.println("*********************");

            IomObject iomObj=((ObjectEvent)event).getIomObject();
            IomObject attrObj=iomObj.getattrobj("geom", 0);
            System.out.println(attrObj);

            
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);

//            IoxEvent event = reader.read();
//            System.out.println(event.toString());
//            event = reader.read();
//            System.out.println(event);
//            event = reader.read();
//            System.out.println(event);


            
            // 2607880.24330579303205013 1228286.63974978867918253
        } finally {
            if(reader != null) {
                reader.close();
                reader = null;
            }
        }
    }
}
