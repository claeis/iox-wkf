package ch.interlis.iom_j.shp;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.iox_j.jts.Iox2jtsException;

public class ShapeWriterTest {
	
	private final static String TEST_OUT="src/test/data/ShapeWriter";
	private TransferDescription td=null;
	
	@Before
	public void setup() throws Ili2cFailure
	{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		assertNotNull(td);
	}
	
	@Test
	public void emptyTransfer_Ok() throws Iox2jtsException, IoxException {
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"testEmptyTransfer.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		writer.close();
	    		writer=null;
	    	}
		}
	}
	
	@Test
	public void emptyBasket_Ok() throws Iox2jtsException, IoxException {
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"testEmptyBakset.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}finally {
	    	if(writer!=null) {
	    		writer.close();
	    		writer=null;
	    	}
		}
	}
	
	// Der Benutzer gibt 3 models an.
	// Es wird getestet ob der Name des Models (Name des Models: StadtModel.ili) stimmt.
	// Es wird getestet ob der Name des Topics (Topic: Topic1) stimmt.
	// Es wird getestet ob der Name der Klasse (Class: Polygon) stimmt.
	// Das Model welches als letztes angegeben wird, wird zuerst auf die Zielklasse kontrolliert.
	@Test
    public void setMultipleModels_ClassFoundInLastInputModel_Ok() throws IoxException, Ili2cFailure, IOException{
		Iom_jObject objSurfaceSuccess=new Iom_jObject("BundesModel.Topic1.Polygon", "o1");
		objSurfaceSuccess.setattrvalue("id", "10");
		IomObject multisurfaceValue=objSurfaceSuccess.addattrobj("the_geom", "MULTISURFACE");
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
		ShapeWriter writer = null;
		// ili-datei lesen
		ShapeReader reader=null;
		TransferDescription tdM=null;
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntryConditionClass=new FileEntry(TEST_OUT+"/Polygon/StadtModel.ili", FileEntryKind.ILIMODELFILE); // first input model
		ili2cConfig.addFileEntry(fileEntryConditionClass);
		FileEntry fileEntry=new FileEntry(TEST_OUT+"/Polygon/KantonModel.ili", FileEntryKind.ILIMODELFILE); // second input model
		ili2cConfig.addFileEntry(fileEntry);
		FileEntry fileEntry2=new FileEntry(TEST_OUT+"/Polygon/BundesModel.ili", FileEntryKind.ILIMODELFILE); // third input model
		ili2cConfig.addFileEntry(fileEntry2);
		tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		assertNotNull(tdM);
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Polygon/Polygon.shp"));
			writer.setModel(tdM);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("BundesModel.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
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
		try {
			reader=new ShapeReader(new File(TEST_OUT,"Polygon/Polygon.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			reader.setModel(tdM);
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getattrvaluecount("id")==1);
			IomObject multisurface=iomObj.getattrobj("the_geom", 0);
			
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			IomObject surface=multisurface.getattrobj("surface", 0);
			IomObject boundary=surface.getattrobj("boundary", 0);
			
			// polyline 1
			IomObject polylineObj=boundary.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(segment.getattrvalue("C2").equals("0.5688311688311687"));
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("-0.15857142857142853"));
			assertTrue(segment2.getattrvalue("C2").equals("0.5888311688311687"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn eine Coord in einen Point konvertiert wird.
	@Test
	public void point_Ok() throws IoxException, IOException{
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		@SuppressWarnings("deprecation")
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Point/Point.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
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
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"Point/Point.shp"));
			IoxEvent event2=reader.read();
			assertTrue(event2 instanceof StartTransferEvent);
			event2=reader.read();
			assertTrue(event2 instanceof StartBasketEvent);
			event2=reader.read();
			assertTrue(event2 instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event2).getIomObject();
			IomObject attrObj=iomObj.getattrobj("the_geom", 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.5688311688311687"));
			event2=reader.read();
			assertTrue(event2 instanceof EndBasketEvent);
			event2=reader.read();
			assertTrue(event2 instanceof EndTransferEvent);
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
		
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn 3 Attribute in Fields konvertiert werden
	@Test
	public void pointAttribute_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject("Test1.Topic1.Point2", "o1");
		inputObj.setattrvalue("id1", "1");
		inputObj.setattrvalue("Text", "text1");
		inputObj.setattrvalue("Double", "53434");
		@SuppressWarnings("deprecation")
		IomObject coordValue=inputObj.addattrobj("attrPoint2", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"PointAttributes/Point2.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(inputObj));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"PointAttributes/Point2.shp"));
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			String attr2=iomObj.getattrvalue("Text");
			assertTrue(attr2.equals("text1"));
			String attr3=iomObj.getattrvalue("id1");
			assertTrue(attr3.equals("1"));
			String attr4=iomObj.getattrvalue("Double");
			assertTrue(attr4.equals("53434"));
			String attr5=iomObj.getattrobj("the_geom", 0).toString();
			assertTrue(attr5.equals("COORD {C1 -0.4025974025974026, C2 1.3974025974025972}"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn eine Coord in einen Point konvertiert wird.
	// Wenn das Modell nicht gesetzt wird.
	@Test
	public void noModelSetPoint_Ok() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Point/NoModelSetPointOk.shp"));
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}catch(IoxException e) {
			throw new IoxException(e);
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
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"Point/NoModelSetPointOk.shp"));
			IoxEvent event2=reader.read();
			assertTrue(event2 instanceof StartTransferEvent);
			event2=reader.read();
			assertTrue(event2 instanceof StartBasketEvent);
			event2=reader.read();
			assertTrue(event2 instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event2).getIomObject();
			IomObject attrObj=iomObj.getattrobj("the_geom", 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.5688311688311687"));
			event2=reader.read();
			assertTrue(event2 instanceof EndBasketEvent);
			event2=reader.read();
			assertTrue(event2 instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein mehrere Coords in einen MultiPoint konvertiert wird.
	@Test
	public void multiPoint_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objSuccessFormat=new Iom_jObject("Test1.Topic1.MultiPoint", "o1");
		@SuppressWarnings("deprecation")
		IomObject multiCoordValue=objSuccessFormat.addattrobj("attrMPoint", "MULTICOORD");
		@SuppressWarnings("deprecation")
		IomObject coordValue1=multiCoordValue.addattrobj("coord", "COORD");
		coordValue1.setattrvalue("C1", "-0.22857142857142854");
		coordValue1.setattrvalue("C2", "0.5688311688311687");
		
		@SuppressWarnings("deprecation")
		IomObject coordValue2=multiCoordValue.addattrobj("coord", "COORD");
		coordValue2.setattrvalue("C1", "-0.19220779220779216");
		coordValue2.setattrvalue("C2", "0.6935064935064934");
		
		@SuppressWarnings("deprecation")
		IomObject coordValue3=multiCoordValue.addattrobj("coord", "COORD");
		coordValue3.setattrvalue("C1", "-0.48831168831168836");
		coordValue3.setattrvalue("C2", "0.32727272727272716");
		
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"MultiPoint/MultiPoint.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccessFormat));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"MultiPoint/MultiPoint.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject attrObj=iomObj.getattrobj("the_geom", 0);
			IomObject coordObj=attrObj.getattrobj("coord", 0);
			assertTrue(coordObj.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(coordObj.getattrvalue("C2").equals("0.5688311688311687"));
			IomObject coordObj2=attrObj.getattrobj("coord", 1);
			assertTrue(coordObj2.getattrvalue("C1").equals("-0.19220779220779216"));
			assertTrue(coordObj2.getattrvalue("C2").equals("0.6935064935064934"));
			IomObject coordObj3=attrObj.getattrobj("coord", 2);
			assertTrue(coordObj3.getattrvalue("C1").equals("-0.48831168831168836"));
			assertTrue(coordObj3.getattrvalue("C2").equals("0.32727272727272716"));
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein mehrere Coords in einen MultiPoint konvertiert wird.
	// Zusaetzlich werden Attribute erstellt.
	@Test
	public void multiPointAttribute_Ok() throws IoxException, IOException, Ili2cFailure{
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
		
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"MultiPointAttributes/MultiPoint2.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccessFormat));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"MultiPointAttributes/MultiPoint2.shp"));
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getattrvalue("textattr2").equals("text1"));
			IomObject attrObj=iomObj.getattrobj("the_geom", 0);
			IomObject coordObj=attrObj.getattrobj("coord", 0);
			assertTrue(coordObj.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(coordObj.getattrvalue("C2").equals("0.5688311688311687"));
			IomObject coordObj2=attrObj.getattrobj("coord", 1);
			assertTrue(coordObj2.getattrvalue("C1").equals("-0.19220779220779216"));
			assertTrue(coordObj2.getattrvalue("C2").equals("0.6935064935064934"));
			IomObject coordObj3=attrObj.getattrobj("coord", 2);
			assertTrue(coordObj3.getattrvalue("C1").equals("-0.48831168831168836"));
			assertTrue(coordObj3.getattrvalue("C2").equals("0.32727272727272716"));
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(reader!=null) {
	    		try {
	    			reader.close();
				} catch (Exception e) {
					throw new IoxException(e);
				}
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein Polygon in einen LineString konvertiert wird.
	@Test
	public void lineString_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objStraightsSuccess=new Iom_jObject("Test1.Topic1.LineString", "o1");
		@SuppressWarnings("deprecation")
		IomObject polylineValue=objStraightsSuccess.addattrobj("attrLineString", "POLYLINE");
		@SuppressWarnings("deprecation")
		IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
		@SuppressWarnings("deprecation")
		IomObject coordStart=segments.addattrobj("segment", "COORD");
		@SuppressWarnings("deprecation")
		IomObject coordEnd=segments.addattrobj("segment", "COORD");
		coordStart.setattrvalue("C1", "-0.22857142857142854");
		coordStart.setattrvalue("C2", "0.5688311688311687");
		coordEnd.setattrvalue("C1", "-0.22557142857142853");
		coordEnd.setattrvalue("C2", "0.5658311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"LineString/LineString.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objStraightsSuccess));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"LineString/LineString.shp"));
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multiPolylineObj=iomObj.getattrobj("the_geom", 0);
			IomObject polylineObj=multiPolylineObj.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			IomObject coord=sequence.getattrobj("segment", 0);
			assertTrue(coord.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(coord.getattrvalue("C2").equals("0.5688311688311687"));
			IomObject coord2=sequence.getattrobj("segment", 1);
			assertTrue(coord2.getattrvalue("C1").equals("-0.22557142857142853"));
			assertTrue(coord2.getattrvalue("C2").equals("0.5658311688311687"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein Polygon in einen LineString konvertiert wird.
	// Zusaetzlich werden Attribute erstellt.
	@Test
	public void lineStringAttributes_Ok() throws IoxException, IOException, Ili2cFailure{
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
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"LineStringAttributes/LineString2.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objStraightsSuccess));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"LineStringAttributes/LineString2.shp"));
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getattrvalue("attr1LS").equals("text1"));
			assertTrue(iomObj.getattrvalue("attr2LS").equals("5"));
			IomObject multiPolylineObj=iomObj.getattrobj("the_geom", 0);
			IomObject polylineObj=multiPolylineObj.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			IomObject coord=sequence.getattrobj("segment", 0);
			assertTrue(coord.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(coord.getattrvalue("C2").equals("0.5688311688311687"));
			IomObject coord2=sequence.getattrobj("segment", 1);
			assertTrue(coord2.getattrvalue("C1").equals("-0.22557142857142853"));
			assertTrue(coord2.getattrvalue("C2").equals("0.5658311688311687"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn mehrere Polylines in einen MultiLineString konvertiert wird.
	@Test
	public void multiLineString_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objStraightsSuccess=new Iom_jObject("Test1.Topic1.MultiLineString", "o1");
		IomObject multiPolylineValue=objStraightsSuccess.addattrobj("attrMLineString", "MULTIPOLYLINE");
		
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
		coordStart2.setattrvalue("C1", "-0.22557142857142853");
		coordStart2.setattrvalue("C2", "0.5658311688311687");
		coordEnd2.setattrvalue("C1", "-0.22755142857142853");
		coordEnd2.setattrvalue("C2", "0.5558351688311687");
		
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"MultiLineString/MultiLineString.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objStraightsSuccess));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"MultiLineString/MultiLineString.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multiPolylineObj=iomObj.getattrobj("the_geom", 0);
			// polyline
			IomObject polylineObj=multiPolylineObj.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(segment.getattrvalue("C2").equals("0.5688311688311687"));
			
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("-0.22557142857142853"));
			assertTrue(segment2.getattrvalue("C2").equals("0.5658311688311687"));
			
			// polyline2
			IomObject polylineObj2=multiPolylineObj.getattrobj("polyline", 1);
			IomObject sequence2=polylineObj2.getattrobj("sequence", 0);
			
			IomObject segment3=sequence2.getattrobj("segment", 0);
			assertTrue(segment3.getattrvalue("C1").equals("-0.22557142857142853"));
			assertTrue(segment3.getattrvalue("C2").equals("0.5658311688311687"));
			
			IomObject segment4=sequence2.getattrobj("segment", 1);
			assertTrue(segment4.getattrvalue("C1").equals("-0.22755142857142854"));
			assertTrue(segment4.getattrvalue("C2").equals("0.5558351688311687"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn mehrere Polylines in einen MultiLineString konvertiert wird.
	// Zusaetzlich werden Attribute erstellt.
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
		coordStart2.setattrvalue("C1", "-0.22557142857142853");
		coordStart2.setattrvalue("C2", "0.5658311688311687");
		coordEnd2.setattrvalue("C1", "-0.22755142857142853");
		coordEnd2.setattrvalue("C2", "0.5558351688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"MultiLineStringAttributes/MultiLineString2.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objStraightsSuccess));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"MultiLineStringAttributes/MultiLineString2.shp"));
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getattrvalue("attr1MLS").equals("text2"));
			assertTrue(iomObj.getattrvalue("attr2MLS").equals("6"));
			IomObject multiPolylineObj=iomObj.getattrobj("the_geom", 0);
			// polyline
			IomObject polylineObj=multiPolylineObj.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(segment.getattrvalue("C2").equals("0.5688311688311687"));
			
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("-0.22557142857142853"));
			assertTrue(segment2.getattrvalue("C2").equals("0.5658311688311687"));
			
			// polyline2
			IomObject polylineObj2=multiPolylineObj.getattrobj("polyline", 1);
			IomObject sequence2=polylineObj2.getattrobj("sequence", 0);
			
			IomObject segment3=sequence2.getattrobj("segment", 0);
			assertTrue(segment3.getattrvalue("C1").equals("-0.22557142857142853"));
			assertTrue(segment3.getattrvalue("C2").equals("0.5658311688311687"));
			
			IomObject segment4=sequence2.getattrobj("segment", 1);
			assertTrue(segment4.getattrvalue("C1").equals("-0.22755142857142854"));
			assertTrue(segment4.getattrvalue("C2").equals("0.5558351688311687"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein Surface in ein Polygon konvertiert wird.
	@Test
	public void polygon_Ok() throws IoxException, IOException, Ili2cFailure{
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
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Polygon/Polygon.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"Polygon/Polygon.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			reader.setModel(td2);
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multisurface=iomObj.getattrobj("the_geom", 0);
			
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			IomObject surface=multisurface.getattrobj("surface", 0);
			IomObject boundary=surface.getattrobj("boundary", 0);
			
			// polyline 1
			IomObject polylineObj=boundary.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(segment.getattrvalue("C2").equals("0.5688311688311687"));
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("-0.15857142857142853"));
			assertTrue(segment2.getattrvalue("C2").equals("0.5888311688311687"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein Surface in ein Polygon konvertiert wird.
	// Zusaetzlich werden Attribute erstellt.
	@Test
	public void polygonAttributes_Ok() throws IoxException, IOException, Ili2cFailure{
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
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"PolygonAttributes/Polygon2.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"PolygonAttributes/Polygon2.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			reader.setModel(td2);
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getattrvalue("attr1PG").equals("text2"));
			assertTrue(iomObj.getattrvalue("attr2PG").equals("6"));
			IomObject multisurface=iomObj.getattrobj("the_geom", 0);
			
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			IomObject surface=multisurface.getattrobj("surface", 0);
			IomObject boundary=surface.getattrobj("boundary", 0);
			
			// polyline 1
			IomObject polylineObj=boundary.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(segment.getattrvalue("C2").equals("0.5688311688311687"));
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("-0.15857142857142853"));
			assertTrue(segment2.getattrvalue("C2").equals("0.5888311688311687"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein Surface in ein Polygon konvertiert wird.
	@Test
	public void multiPolygon_Ok() throws IoxException, IOException, Ili2cFailure{
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
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"MultiPolygon/MultiPolygon.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"MultiPolygon/MultiPolygon.shp"));
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multisurface=iomObj.getattrobj("the_geom", 0);
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			IomObject surface=multisurface.getattrobj("surface", 0);
			IomObject boundary=surface.getattrobj("boundary", 0);
			// polyline 1
			IomObject polylineObj=boundary.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			assertTrue(sequence.getattrvaluecount("segment")==6);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-0.228"));
			assertTrue(segment.getattrvalue("C2").equals("0.568"));
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("-0.158"));
			assertTrue(segment2.getattrvalue("C2").equals("0.588"));
			// surface2
			event=reader.read();
			
			event=reader.read();
			assertTrue(event instanceof EndBasketEvent);
			event=reader.read();
			assertTrue(event instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein Surface in ein Polygon konvertiert wird.
	// Zusaetzlich werden Attribute erstellt.
	@Test
	public void multiPolygonAttributes_Ok() throws IoxException, IOException, Ili2cFailure{
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
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"MultiPolygonAttributes/MultiPolygon2.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSurfaceSuccess));
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
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry("src/test/data/ShapeWriter/Test1Read.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_OUT,"MultiPolygonAttributes/MultiPolygon2.shp"));
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getattrvalue("attr1MPG").equals("text3"));
			assertTrue(iomObj.getattrvalue("attr2MPG").equals("8"));
			IomObject multisurface=iomObj.getattrobj("the_geom", 0);
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			IomObject surface=multisurface.getattrobj("surface", 0);
			IomObject boundary=surface.getattrobj("boundary", 0);
			// polyline 1
			IomObject polylineObj=boundary.getattrobj("polyline", 0);
			IomObject sequence=polylineObj.getattrobj("sequence", 0);
			assertTrue(sequence.getattrvaluecount("segment")==6);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-0.228"));
			assertTrue(segment.getattrvalue("C2").equals("0.568"));
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("-0.158"));
			assertTrue(segment2.getattrvalue("C2").equals("0.588"));
			// surface2
			event=reader.read();
			
			event=reader.read();
			assertTrue(event instanceof EndBasketEvent);
			event=reader.read();
			assertTrue(event instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn kein Attribute innerhalb des IomObjektes gefunden werden kann.
	@Test
	public void emptyObject_Fail() throws IoxException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Fails/emptyObject_Fail.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().equals("no feature found in Test1.Topic1.Point oid o1 {}"));
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
	
	// die srid wird gesetzt und ist falsch oder kann nicht gefunden werden.
	@Test
	public void sridWrong_Fail() throws IoxException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Fails/sridWrong_Fail.shp"));
			writer.setModel(td);
			writer.setSridCode("99999999");
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().equals("coordinate reference: EPSG:99999999 not found"));
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
	
	// das Model wird gesetzt. Es soll eine Fehlermeldung ausgegeben werden,
	// weil die Klasse innerhalb des angegebenen Modells nicht gefunden werden kann.
	@Test
	public void classOfModelNotFound_Fail() throws IoxException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point99", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Fails/ClassOfModelNotFound_Fail.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().equals("class Test1.Topic1.Point99 not found in model Test1"));
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn die Coord nicht konvertiert werden kann.
	@Test
	public void failedToConvertToJts_Fail() throws IoxException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C3", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Fails/failedToConvertToJts_Fail.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().equals("failed to convert COORD {C3 0.5688311688311687, C1 -0.22857142857142854} to jts"));
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn 2 Objekte auf einmal gesetzt werden.
	@Test
	public void maxCountOfObjectWrong_Fail() throws IoxException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		IomObject coordValue2=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue2.setattrvalue("C1", "-0.22857142857142854");
		coordValue2.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"Fails/maxCountOfObjectWrong_Fail.shp"));
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().equals("max one COORD value allowed (attrPoint)"));
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