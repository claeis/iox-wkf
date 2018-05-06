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
import ch.interlis.ioxwkf.gpkg.GeoPackageReader;;

public class GeoPackageReaderTest {
    
    private static final String TEST_IN="src/test/data/GpkgReader/";

    @Test
    public void singlePoint_Ok() throws IoxException, IOException{
        GeoPackageReader reader = null;
        try {
            reader = new GeoPackageReader(new File(TEST_IN+"point/point.gpkg"), "point");
            assertTrue(true);
            
            IoxEvent event = reader.read();
            System.out.println("*********************");
            System.out.println(event.toString());
            event = reader.read();
           System.out.println(event);
            

            
            // 2607880.24330579303205013 1228286.63974978867918253
        } finally {
            if(reader != null) {
                reader.close();
                reader = null;
            }
        }
    }
}
