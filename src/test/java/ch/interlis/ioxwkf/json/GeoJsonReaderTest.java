package ch.interlis.ioxwkf.json;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;

import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.EndBasketEvent;
import ch.interlis.iox.EndTransferEvent;
import ch.interlis.iox.ObjectEvent;
import ch.interlis.iox.StartBasketEvent;
import ch.interlis.iox.StartTransferEvent;
import ch.interlis.iox_j.jts.Iox2jts;

public class GeoJsonReaderTest {
	
    private static final String TOPIC_TEST1_TOPIC1 = "Test1.Topic1";
    private static final String TOPIC_TEST1_TOPIC2 = "Test1.Topic2";
    private static final String TOPIC_TEST1_TOPIC3 = "Test1.Topic3";
	private static final String CLASS_TEST1_TOPIC1_STREET = "Test1.Topic1.Street";
    private static final String CLASS_TEST1_TOPIC1_PARCEL = "Test1.Topic1.Parcel";
    private static final String CLASS_TEST1_TOPIC1_SIMPLE_ATTR = "Test1.Topic1.SimpleAttr";
    private static final String CLASS_TEST1_TOPIC1_POINT_OF_INTEREST = "Test1.Topic1.PointOfInterest";
    private static final String CLASS_TEST1_TOPIC2_STRUCTA = "Test1.Topic2.StructA";
    private static final String CLASS_TEST1_TOPIC2_CLASSA = "Test1.Topic2.ClassA";
    private static final String CLASS_TEST1_TOPIC3_AREA_OF_INTEREST = "Test1.Topic3.AreaOfInterest";
    private final static String TEST_IN="src/test/data/GeoJsonReader";
	private final static String TEST_OUT="build/test/data/GeoJsonReader";
    private static final double E = 1.0; //0.000001;
	private TransferDescription td=null;
	
	@BeforeClass
	public static void setupFolder() throws Ili2cFailure
	{
		new File(TEST_OUT).mkdirs();
	}
	@Before
	public void setup() throws Ili2cFailure
	{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry(TEST_IN+"/Test1.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		assertNotNull(td);
	}
	   @Test
	    public void wgs84toLV95() throws IoxException, IOException{
	        Coordinate coord=new Coordinate(7.438637222222222,46.95108111111111);
	        GeoJson2iox.convertCoordinate(coord,GeoJson2iox.LV95);
	           System.out.println(coord);
	        assertEquals(2600000.000,coord.x,E);
	        assertEquals(1200000.000,coord.y,E);
	   }
	   @Test
	   public void wgs84toLV03() throws IoxException, IOException{
	       //Coordinate coord=new Coordinate(600000.000,200000.000);
	       //Iox2geoJson.convertCoordinate(coord);
	       Coordinate coord=new Coordinate(7.438637222222222,46.95108111111111);
	       //System.out.println(coord);
	       GeoJson2iox.convertCoordinate(coord,GeoJson2iox.LV03);
           System.out.println(coord);
	       assertEquals(600000.000,coord.x,E);
	       assertEquals(200000.000,coord.y,E);
	  }
	
	@Test
	public void emptyTransfer() throws IoxException, IOException {
		GeoJsonReader reader = null;
		File file = new File(TEST_IN,"emptyTransfer.json");
		try {
			reader = new GeoJsonReader(file);
			reader.setModel(td);
			assertTrue(reader.read() instanceof StartTransferEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
		}finally {
			if(reader!=null) {
				try {
					reader.close();
				} catch (IoxException e) {
					throw new IoxException(e);
				}
	    		reader=null;
			}
		}
	}
	
    @Test
    public void multiBasket() throws IoxException, IOException{
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"multiBasket.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            {
                assertTrue(startBasket instanceof StartBasketEvent);
                assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
                assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            }
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                assertEquals("o1",((ObjectEvent) objectEvent).getIomObject().getobjectoid());
                assertEquals("Test1.Topic1.SimpleAttr",((ObjectEvent) objectEvent).getIomObject().getobjecttag());
                assertEquals(0,((ObjectEvent) objectEvent).getIomObject().getattrcount());
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            startBasket=reader.read();
            {
                assertTrue(startBasket instanceof StartBasketEvent);
                assertEquals("bid2",((StartBasketEvent) startBasket).getBid());
                assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            }
            objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                assertEquals("o2",((ObjectEvent) objectEvent).getIomObject().getobjectoid());
                assertEquals("Test1.Topic1.SimpleAttr",((ObjectEvent) objectEvent).getIomObject().getobjecttag());
                assertEquals(0,((ObjectEvent) objectEvent).getIomObject().getattrcount());
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }
    @Test
    public void multiBasketWrongBid_Fail() throws IoxException, IOException{
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"multiBasketWrongBid_Fail.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            {
                assertTrue(startBasket instanceof StartBasketEvent);
                assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
                assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            }
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                assertEquals("o1",((ObjectEvent) objectEvent).getIomObject().getobjectoid());
                assertEquals("Test1.Topic1.SimpleAttr",((ObjectEvent) objectEvent).getIomObject().getobjecttag());
                assertEquals(0,((ObjectEvent) objectEvent).getIomObject().getattrcount());
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            startBasket=reader.read();
            {
                assertTrue(startBasket instanceof StartBasketEvent);
                assertEquals("bid2",((StartBasketEvent) startBasket).getBid());
                assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            }
            objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                assertEquals("o2",((ObjectEvent) objectEvent).getIomObject().getobjectoid());
                assertEquals("Test1.Topic1.SimpleAttr",((ObjectEvent) objectEvent).getIomObject().getobjecttag());
                assertEquals(0,((ObjectEvent) objectEvent).getIomObject().getattrcount());
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            reader.read();
            fail();
        }catch(IoxException ex) {
            assertTrue(ex.getMessage().contains("unexpected BID"));
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }
    @Test
    public void multiObject() throws IoxException, IOException {
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"multiObject.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                assertEquals("o1",((ObjectEvent) objectEvent).getIomObject().getobjectoid());
                assertEquals("Test1.Topic1.SimpleAttr",((ObjectEvent) objectEvent).getIomObject().getobjecttag());
                assertEquals(0,((ObjectEvent) objectEvent).getIomObject().getattrcount());
            }
            objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                assertEquals("o2",((ObjectEvent) objectEvent).getIomObject().getobjectoid());
                assertEquals("Test1.Topic1.SimpleAttr",((ObjectEvent) objectEvent).getIomObject().getobjecttag());
                assertEquals(0,((ObjectEvent) objectEvent).getIomObject().getattrcount());
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }
	
    @Test
    public void emptyObject() throws IoxException, IOException {
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"emptyObject.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            assertTrue(objectEvent instanceof ObjectEvent);
            assertEquals("o1",((ObjectEvent) objectEvent).getIomObject().getobjectoid());
            assertEquals("Test1.Topic1.SimpleAttr",((ObjectEvent) objectEvent).getIomObject().getobjecttag());
            assertEquals(0,((ObjectEvent) objectEvent).getIomObject().getattrcount());
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }

    @Test
    public void simpleAttr() throws IoxException, IOException {
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"simpleAttr.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o1",obj1.getobjectoid());
                assertEquals("Test1.Topic1.SimpleAttr",obj1.getobjecttag());
                assertEquals("lineA",obj1.getattrvalue("attrText"));
                assertEquals("lineB\nnextlineB",obj1.getattrvalue("attrMtext" ));
                assertEquals("-20",obj1.getattrvalue("attrInt" ));
                assertEquals("-10.0",obj1.getattrvalue("attrDec" ));
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }
    @Test
    public void attrUnknown() throws IoxException, IOException {
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"attrUnknown.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o1",obj1.getobjectoid());
                assertEquals("Test1.Topic1.SimpleAttr",obj1.getobjecttag());
                assertEquals("lineA",obj1.getattrvalue("attrUnknown"));
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }
	
    @Test
    public void structAttr() throws IoxException, IOException {
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"structAttr.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals(TOPIC_TEST1_TOPIC2,((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o1",obj1.getobjectoid());
                assertEquals(CLASS_TEST1_TOPIC2_CLASSA,obj1.getobjecttag());
                IomObject structa=obj1.getattrobj("attrStruct", 0);
                assertEquals(CLASS_TEST1_TOPIC2_STRUCTA,structa.getobjecttag());
                assertEquals("lineA",structa.getattrvalue("attrText"));
                IomObject structb0=obj1.getattrobj("attrBag", 0);
                assertEquals(CLASS_TEST1_TOPIC2_STRUCTA,structb0.getobjecttag());
                assertEquals("lineB",structb0.getattrvalue("attrText"));
                IomObject structb1=obj1.getattrobj("attrBag", 1);
                assertEquals(CLASS_TEST1_TOPIC2_STRUCTA,structb1.getobjecttag());
                assertEquals("lineC",structb1.getattrvalue("attrText"));
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }
	
	@Test
	public void coord() throws IoxException, IOException{
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"coord.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals(TOPIC_TEST1_TOPIC1,((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o1",obj1.getobjectoid());
                assertEquals(CLASS_TEST1_TOPIC1_POINT_OF_INTEREST,obj1.getobjecttag());
                IomObject coordValue=obj1.getattrobj("attrPoint", 0);
                assertEquals("COORD",coordValue.getobjecttag());
                assertEquals(2600000.000,Double.parseDouble(coordValue.getattrvalue("C1")),E);
                assertEquals(1200000.000,Double.parseDouble(coordValue.getattrvalue("C2")),E);
            }
            objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o2",obj1.getobjectoid());
                assertEquals(CLASS_TEST1_TOPIC1_POINT_OF_INTEREST,obj1.getobjecttag());
                assertEquals(0,obj1.getattrvaluecount("attrPoint"));
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
	}
    @Test
    public void multiGeomAttr() throws IoxException, IOException{
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"multiGeomAttr.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals(TOPIC_TEST1_TOPIC3,((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o1",obj1.getobjectoid());
                assertEquals(CLASS_TEST1_TOPIC3_AREA_OF_INTEREST,obj1.getobjecttag());
                IomObject coordValue=obj1.getattrobj("attrPoint", 0);
                assertEquals("COORD",coordValue.getobjecttag());
                //assertEquals(2600000.000,Double.parseDouble(coordValue.getattrvalue("C1")),E);
                //assertEquals(1200000.000,Double.parseDouble(coordValue.getattrvalue("C2")),E);
                IomObject surfaceValue=obj1.getattrobj("attrSurface", 0);
                assertEquals("MULTISURFACE",surfaceValue.getobjecttag());
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }

    @Test
    public void polyline() throws Exception{
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"polyline.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals(TOPIC_TEST1_TOPIC1,((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o1",obj1.getobjectoid());
                assertEquals(CLASS_TEST1_TOPIC1_STREET,obj1.getobjecttag());
                IomObject polylineValue=obj1.getattrobj("attrLine", 0);
                //assertEquals("POLYLINE {sequence SEGMENTS {segment [COORD {C1 2460000.0, C2 1045000.0}, COORD {C1 2460001.0, C2 1045000.0}]}}",polylineValue.toString());
                LineString jtsPolyline = Iox2jts.polyline2JTSlineString(polylineValue, false, 0.0);
                Coordinate coords[] = jtsPolyline.getCoordinates();
                assertEquals(2,coords.length);
                //assertEquals(2460000.000,coords[0].x,E);
                //assertEquals(1045000.000,coords[0].y,E);
                //assertEquals(2460001.000,coords[1].x,E);
                //assertEquals(1045000.000,coords[1].y,E);
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }

    @Test
    public void surface() throws IoxException, IOException{
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"surface.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals(TOPIC_TEST1_TOPIC1,((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o1",obj1.getobjectoid());
                assertEquals(CLASS_TEST1_TOPIC1_PARCEL,obj1.getobjecttag());
                IomObject polylineValue=obj1.getattrobj("attrSurface", 0);
                //assertEquals("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 2460000.0, C2 1045000.0}, COORD {C1 2460001.0, C2 1045000.0}, COORD {C1 2460001.0, C2 1045001.0}, COORD {C1 2460000.0, C2 1045001.0}, COORD {C1 2460000.0, C2 1045000.0}]}}}}}",polylineValue.toString());
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
    }
    
	@Test
	public void classOfModelNotFound() throws IoxException {
        GeoJsonReader reader = null;
        File file = new File(TEST_IN,"classOfModelNotFound.json");
        try {
            reader = new GeoJsonReader(file);
            reader.setModel(td);
            assertTrue(reader.read() instanceof StartTransferEvent);
            IoxEvent startBasket=reader.read();
            assertTrue(startBasket instanceof StartBasketEvent);
            assertEquals("bid1",((StartBasketEvent) startBasket).getBid());
            assertEquals("Test1.Topic1",((StartBasketEvent) startBasket).getType());
            IoxEvent objectEvent=reader.read();
            {
                assertTrue(objectEvent instanceof ObjectEvent);
                final IomObject obj1 = ((ObjectEvent) objectEvent).getIomObject();
                assertEquals("o1",obj1.getobjectoid());
                assertEquals("Test1.Topic1.Point99",obj1.getobjecttag());
            }
            assertTrue(reader.read() instanceof EndBasketEvent);
            assertTrue(reader.read() instanceof EndTransferEvent);
            assertNull(reader.read());
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                reader=null;
            }
        }
		
	}
	
}