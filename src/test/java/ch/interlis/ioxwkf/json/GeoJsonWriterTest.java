package ch.interlis.ioxwkf.json;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;

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

public class GeoJsonWriterTest {
	
    private static final String TOPIC_TEST1_TOPIC1 = "Test1.Topic1";
    private static final String TOPIC_TEST1_TOPIC2 = "Test1.Topic2";
    private static final String TOPIC_TEST1_TOPIC3 = "Test1.Topic3";
    private static final String CLASS_TEST1_TOPIC1_SIMPLE_ATTR = "Test1.Topic1.SimpleAttr";
    private static final String CLASS_TEST1_TOPIC1_POINT_OF_INTEREST = "Test1.Topic1.PointOfInterest";
    private static final String CLASS_TEST1_TOPIC1_STREET = "Test1.Topic1.Street";
    private static final String CLASS_TEST1_TOPIC1_PARCEL = "Test1.Topic1.Parcel";
    private static final String CLASS_TEST1_TOPIC2_STRUCTA = "Test1.Topic2.StructA";
    private static final String CLASS_TEST1_TOPIC2_CLASSA = "Test1.Topic2.ClassA";
    private static final String CLASS_TEST1_TOPIC3_POINT_OF_INTEREST = "Test1.Topic3.PointOfInterest";
    private static final String CLASS_TEST1_TOPIC3_AREA_OF_INTEREST = "Test1.Topic3.AreaOfInterest";
    private final static String TEST_IN="src/test/data/GeoJsonWriter";
	private final static String TEST_OUT="build/test/data/GeoJsonWriter";
	private TransferDescription td=null;
    private static final double E = 0.000001;
	
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
	public void emptyTransfer() throws IoxException, IOException {
		GeoJsonWriter writer = null;
		File file = new File(TEST_OUT,"emptyTransfer.json");
		try {
			writer = new GeoJsonWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new EndTransferEvent());
		}finally {
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
	
	@Test
	public void emptyBasket() throws IoxException, IOException {
		GeoJsonWriter writer = null;
		try {
			File file = new File(TEST_OUT,"emptyBasket.json");
			writer = new GeoJsonWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
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
    @Test
    public void multiBasket() throws IoxException, IOException{
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC1_POINT_OF_INTEREST, "o1");
        IomObject coordValue=obj1.addattrobj("attrPoint", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045000.000");
        Iom_jObject obj2=new Iom_jObject(CLASS_TEST1_TOPIC1_POINT_OF_INTEREST, "o2");
        GeoJsonWriter writer = null;
        File file = new File(TEST_OUT,"multiBasket.json");
        try {
            writer = new GeoJsonWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
            writer.write(new ObjectEvent(obj1));
            writer.write(new EndBasketEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid2"));
            writer.write(new ObjectEvent(obj2));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        }finally {
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
	
	@Test
	public void emptyObject() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject(CLASS_TEST1_TOPIC1_SIMPLE_ATTR, "o1");
		GeoJsonWriter writer = null;
		try {
			File file = new File(TEST_OUT,"emptyObject.json");
			writer = new GeoJsonWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
			writer.write(new ObjectEvent(objSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
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

    @Test
    public void simpleAttr() throws IoxException, IOException {
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC1_SIMPLE_ATTR, "o1");
        obj1.setattrvalue("attrText", "lineA");
        obj1.setattrvalue("attrMtext", "lineB\nnextlineB");
        obj1.setattrvalue("attrInt", "-20");
        obj1.setattrvalue("attrDec", "-10.0");
        GeoJsonWriter writer = null;
        try {
            File file = new File(TEST_OUT,"simpleAttr.json");
            writer = new GeoJsonWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
            writer.write(new ObjectEvent(obj1));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        }finally {
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
    @Test
    public void attrUnknown() throws IoxException, IOException {
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC1_SIMPLE_ATTR, "o1");
        obj1.setattrvalue("attrUnknown", "lineA");
        GeoJsonWriter writer = null;
        try {
            File file = new File(TEST_OUT,"attrUnknown.json");
            writer = new GeoJsonWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
            writer.write(new ObjectEvent(obj1));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        }finally {
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
	
    @Test
    public void structAttr() throws IoxException, IOException {
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC2_CLASSA, "o1");
        IomObject structa=obj1.addattrobj("attrStruct", CLASS_TEST1_TOPIC2_STRUCTA);
        structa.setattrvalue("attrText", "lineA");
        IomObject structb=obj1.addattrobj("attrBag", CLASS_TEST1_TOPIC2_STRUCTA);
        structb.setattrvalue("attrText", "lineB");
        IomObject structc=obj1.addattrobj("attrBag", CLASS_TEST1_TOPIC2_STRUCTA);
        structc.setattrvalue("attrText", "lineC");
        GeoJsonWriter writer = null;
        try {
            File file = new File(TEST_OUT,"structAttr.json");
            writer = new GeoJsonWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC2,"bid1"));
            writer.write(new ObjectEvent(obj1));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        }finally {
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
	
	@Test
	public void coord() throws IoxException, IOException{
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC1_POINT_OF_INTEREST, "o1");
        IomObject coordValue=obj1.addattrobj("attrPoint", "COORD");
        coordValue.setattrvalue("C1", "2600000.000");
        coordValue.setattrvalue("C2", "1200000.000");
		GeoJsonWriter writer = null;
		File file = new File(TEST_OUT,"coord.json");
		try {
			writer = new GeoJsonWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
            writer.write(new ObjectEvent(obj1));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
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
   @Test
    public void lv95toWGS84() throws IoxException, IOException{
        Coordinate coord=new Coordinate(2600000.000,1200000.000);
        Iox2geoJson.convertCoordinate(coord);
        assertEquals(7.438637222222222,coord.x,E);
        assertEquals(46.95108111111111,coord.y,E);
   }
   @Test
   public void lv03toWGS84() throws IoxException, IOException{
       Coordinate coord=new Coordinate(600000.000,200000.000);
       Iox2geoJson.convertCoordinate(coord);
       assertEquals(7.438637222222222,coord.x,E);
       assertEquals(46.95108111111111,coord.y,E);
  }
    @Test
    public void nullGeom() throws IoxException, IOException{
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC1_POINT_OF_INTEREST, "o1");
        GeoJsonWriter writer = null;
        File file = new File(TEST_OUT,"nullGeom.json");
        try {
            writer = new GeoJsonWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
            writer.write(new ObjectEvent(obj1));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        }finally {
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

    @Test
    public void polyline() throws IoxException, IOException{
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC1_STREET, "o1");
        //IomObject multiPolylineValue=obj1.addattrobj("attrLine", "MULTIPOLYLINE");
        IomObject polylineValue=obj1.addattrobj("attrLine", "POLYLINE");
        IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
        IomObject coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045000.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460001.000");
        coordValue.setattrvalue("C2", "1045000.000");
        GeoJsonWriter writer = null;
        File file = new File(TEST_OUT,"polyline.json");
        try {
            writer = new GeoJsonWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
            writer.write(new ObjectEvent(obj1));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        }finally {
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
    @Test
    public void surface() throws IoxException, IOException{
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC1_PARCEL, "o1");
        
        IomObject multisurfaceValue=obj1.addattrobj("attrSurface", "MULTISURFACE");
        IomObject surfaceValue = multisurfaceValue.addattrobj("surface", "SURFACE");
        IomObject outerBoundary = surfaceValue.addattrobj("boundary", "BOUNDARY");
        IomObject polylineValue = outerBoundary.addattrobj("polyline", "POLYLINE");
        IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
        IomObject coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045000.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460001.000");
        coordValue.setattrvalue("C2", "1045000.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460001.000");
        coordValue.setattrvalue("C2", "1045001.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045001.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045000.000");
        GeoJsonWriter writer = null;
        File file = new File(TEST_OUT,"surface.json");
        try {
            writer = new GeoJsonWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC3,"bid1"));
            writer.write(new ObjectEvent(obj1));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        }finally {
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

    @Test
    public void multiGeomAttr() throws IoxException, IOException{
        Iom_jObject obj1=new Iom_jObject(CLASS_TEST1_TOPIC3_AREA_OF_INTEREST, "o1");
        IomObject coordValue=obj1.addattrobj("attrPoint", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045000.000");
        
        IomObject multisurfaceValue=obj1.addattrobj("attrSurface", "MULTISURFACE");
        IomObject surfaceValue = multisurfaceValue.addattrobj("surface", "SURFACE");
        IomObject outerBoundary = surfaceValue.addattrobj("boundary", "BOUNDARY");
        IomObject polylineValue = outerBoundary.addattrobj("polyline", "POLYLINE");
        IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045000.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460001.000");
        coordValue.setattrvalue("C2", "1045000.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460001.000");
        coordValue.setattrvalue("C2", "1045001.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045001.000");
        coordValue=segments.addattrobj("segment", "COORD");
        coordValue.setattrvalue("C1", "2460000.000");
        coordValue.setattrvalue("C2", "1045000.000");
        GeoJsonWriter writer = null;
        File file = new File(TEST_OUT,"multiGeomAttr.json");
        try {
            writer = new GeoJsonWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC3,"bid1"));
            writer.write(new ObjectEvent(obj1));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        }finally {
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
    
	@Test
	public void classOfModelNotFound_Fail() throws IoxException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point99", "o1");
		GeoJsonWriter writer = null;
		try {
			File file = new File(TEST_OUT,"classOfModelNotFound_Fail.json");
			writer = new GeoJsonWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent(TOPIC_TEST1_TOPIC1,"bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("Test1.Topic1.Point99"));
			assertTrue(e.getMessage().contains("not found"));
		}finally {
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