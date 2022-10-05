package ch.interlis.ioxwkf.shp;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.AttributeTypeBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.Point;
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

public class ShapeWriterTest {
	
	private final static String TEST_IN="src/test/data/ShapeWriter";
	private final static String TEST_OUT="build/test/data/ShapeWriter";
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
	
	// In diesem Test wird ein transfer-event gestartet und gleich wieder beendet.
	// Die Shapedatei muss dabei erstellt werden, darf jedoch keinen Inhalt aufweisen.
	// In diesem Test wird die td gesetzt.
	@Test
	public void setModel_emptyTransfer_Ok() throws IoxException, IOException {
		ShapeWriter writer = null;
		File file = new File(TEST_OUT,"testEmptyTransfer.shp");
		try {
			writer = new ShapeWriter(file);
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
		{
			//Open the file for reading
			FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"testEmptyTransfer.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		assertEquals(false,featureCollectionIter.hasNext());
    		featureCollectionIter.close();
    		dataStore.dispose();
    	}
	}
	
	// In diesem Test wird ein transfer-event gestartet und gleich wieder beendet.
	// Die Shapedatei muss dabei erstellt werden, darf jedoch keinen Inhalt aufweisen.
	// In diesem Test wird die td NICHT gesetzt.
	@Test
	public void emptyTransfer_Ok() throws IoxException, IOException {
		ShapeWriter writer = null;
		try {
			File file = new File(TEST_OUT,"testEmptyTransfer.shp");
			writer = new ShapeWriter(file);
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
		{
			//Open the file for reading
			FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"testEmptyTransfer.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		assertEquals(false,featureCollectionIter.hasNext());
    		featureCollectionIter.close();
    		dataStore.dispose();
    	}
	}
	
	// In diesem Test wird innerhalb der transfer-events, ein basket-event gestartet und gleich wieder beendet.
	// Die Shapedatei muss dabei erstellt werden, darf jedoch keinen Inhalt aufweisen.
	// In diesem Test wird die td gesetzt.
	@Test
	public void setModel_emptyBasket_Ok() throws IoxException, IOException {
		ShapeWriter writer = null;
		try {
			File file = new File(TEST_OUT,"testEmptyBakset.shp");
			writer = new ShapeWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
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
		{
			//Open the file for reading
			FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"testEmptyBakset.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		assertEquals(false,featureCollectionIter.hasNext());
    		featureCollectionIter.close();
    		dataStore.dispose();
    	}
	}
	
	// In diesem Test wird innerhalb der transfer-events, ein basket-event gestartet und gleich wieder beendet.
	// Die Shapedatei muss dabei erstellt werden, darf jedoch keinen Inhalt aufweisen.
	// In diesem Test wird die td NICHT gesetzt.
	@Test
	public void emptyBasket_Ok() throws IoxException, IOException {
		ShapeWriter writer = null;
		try {
			File file = new File(TEST_OUT,"testEmptyBakset.shp");
			writer = new ShapeWriter(file);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
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
		{
			//Open the file for reading
			FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"testEmptyBakset.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		assertEquals(false,featureCollectionIter.hasNext());
    		featureCollectionIter.close();
    		dataStore.dispose();
    	}
	}
	
	// In diesem Test wird innerhalb der basket-events, ein leeres object-event dem writer uebergeben.
	// Die Shapedatei muss dabei erstellt werden, soll 1 Objekt enthalten, welches jedoch keinen Inhalt aufweisen darf.
	// In diesem Test wird die td gesetzt.
	@Test
	public void setModel_emptyObject_Ok() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		ShapeWriter writer = null;
		try {
			File file = new File(TEST_OUT,"emptyObject_Ok.shp");
			writer = new ShapeWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}catch(IoxException e) {
			assertEquals("no feature found in Test1.Topic1.Point", e.getMessage());
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
		{
			//Open the file for reading
			FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"emptyObject_Ok.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		assertEquals(true,featureCollectionIter.hasNext());
    		// feature object
    		SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
    		assertEquals(1,shapeObj.getAttributeCount());
    		assertEquals(null,shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM));
    		assertEquals(false,featureCollectionIter.hasNext());
    		featureCollectionIter.close();
    		dataStore.dispose();
    	}
	}
	
	// In diesem Test wird innerhalb der basket-events, ein leeres object-event dem writer uebergeben.
	// Die Shapedatei muss dabei erstellt werden, soll 1 Objekt enthalten, welches jedoch keinen Inhalt aufweisen darf.
	// In diesem Test wird die td NICHT gesetzt.
	@Test
	public void emptyObject_Ok() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		ShapeWriter writer = null;
		try {
			File file = new File(TEST_OUT,"emptyObject_Ok.shp");
			writer = new ShapeWriter(file);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			writer.write(new EndBasketEvent());
			writer.write(new EndTransferEvent());
		}catch(IoxException e) {
			assertEquals("no feature found in Test1.Topic1.Point", e.getMessage());
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
		{
			//Open the file for reading
			FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"emptyObject_Ok.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		assertEquals(true,featureCollectionIter.hasNext());
    		// feature object
    		SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
    		assertEquals(1,shapeObj.getAttributeCount());
    		assertEquals(null,shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM));
    		assertEquals(false,featureCollectionIter.hasNext());
    		featureCollectionIter.close();
    		dataStore.dispose();
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
		IomObject multisurfaceValue=objSurfaceSuccess.addattrobj(ShapeReader.GEOTOOLS_THE_GEOM, "MULTISURFACE");
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
		FileEntry fileEntryConditionClass=new FileEntry(TEST_IN+"/StadtModel.ili", FileEntryKind.ILIMODELFILE); // first input model
		ili2cConfig.addFileEntry(fileEntryConditionClass);
		FileEntry fileEntry=new FileEntry(TEST_IN+"/KantonModel.ili", FileEntryKind.ILIMODELFILE); // second input model
		ili2cConfig.addFileEntry(fileEntry);
		FileEntry fileEntry2=new FileEntry(TEST_IN+"/BundesModel.ili", FileEntryKind.ILIMODELFILE); // third input model
		ili2cConfig.addFileEntry(fileEntry2);
		tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		assertNotNull(tdM);
		File file = new File(TEST_OUT,"Polygon.shp");
		try {
			writer = new ShapeWriter(file);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"Polygon.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) {
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			Object attr2=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTIPOLYGON (((-0.2285714285714285 0.5688311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5688311688311687, -0.1585714285714285 0.5688311688311687, -0.2285714285714285 0.5688311688311687)))",attr2.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn eine Coord in einen Point konvertiert wird.
	@Test
	public void point_Ok() throws IoxException, IOException{
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		File file = new File(TEST_OUT,"Point.shp");
		try {
			writer = new ShapeWriter(file);
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
		{
			//Open the file for reading
        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"Point.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		if(featureCollectionIter.hasNext()) {
				// feature object
				SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
				Object attr1=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
				assertEquals("POINT (-0.2285714285714285 0.5688311688311687)",attr1.toString());
    		}
    		featureCollectionIter.close();
    		dataStore.dispose();
    	}
	}
		
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn 3 Attribute in Fields konvertiert werden
	@Test
	public void setModel_pointAttribute_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject inputObj=new Iom_jObject("Test1.Topic1.Point2", "o1");
		inputObj.setattrvalue("id1", "1");
		inputObj.setattrvalue("Text", "text1");
		inputObj.setattrvalue("Double", "53434");
		IomObject coordValue=inputObj.addattrobj("attrPoint2", "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		ShapeWriter writer = null;
		File file = new File(TEST_OUT,"Point2.shp");
		try {
			writer = new ShapeWriter(file);
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
		{
			//Open the file for reading
        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"Point2.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		if(featureCollectionIter.hasNext()) {
				// feature object
				SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
				Object attr1=shapeObj.getAttribute("id1");
				assertEquals("1",attr1.toString());
				Object attr2=shapeObj.getAttribute("Text");
				assertEquals("text1",attr2.toString());
				Object attr3=shapeObj.getAttribute("Double");
				assertEquals("53434",attr3.toString());
				Object attr4=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
				assertEquals("POINT (-0.4025974025974026 1.3974025974025972)",attr4.toString());
    		}
    		featureCollectionIter.close();
    		dataStore.dispose();
    	}
	}
	@Test
	public void setAttrDesc_pointAttribute_Ok() throws IoxException, IOException, Ili2cFailure{
		AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
		AttributeDescriptor[] attrDescs=new AttributeDescriptor[6];
		
		String id1_attr = "id1";
		attributeBuilder.setBinding(String.class);
		attributeBuilder.setName(id1_attr);
		attributeBuilder.setMinOccurs(0);
		attributeBuilder.setMaxOccurs(1);
		attributeBuilder.setNillable(true);
		attrDescs[0]=attributeBuilder.buildDescriptor(id1_attr);
		
		String text_attr = "Text";
		attributeBuilder.setBinding(String.class);
		attributeBuilder.setName(text_attr);
		attributeBuilder.setMinOccurs(0);
		attributeBuilder.setMaxOccurs(1);
		attributeBuilder.setNillable(true);
		attrDescs[1]=attributeBuilder.buildDescriptor(text_attr);
		
		String double_attr = "Double";
		attributeBuilder.setBinding(Double.class);
		attributeBuilder.setName(double_attr);
		attributeBuilder.setMinOccurs(0);
		attributeBuilder.setMaxOccurs(1);
		attributeBuilder.setNillable(true);
		attrDescs[2]=attributeBuilder.buildDescriptor(double_attr);
		
		String geom_attr = "attrPoint2";
		attributeBuilder.setBinding(Point.class);
		attributeBuilder.setName(geom_attr);
		attributeBuilder.setMinOccurs(0);
		attributeBuilder.setMaxOccurs(1);
		attributeBuilder.setNillable(true);
		attrDescs[3]=attributeBuilder.buildDescriptor(geom_attr);

		String date_attr = "adate";
		attributeBuilder.setBinding(java.util.Date.class);
		attributeBuilder.setName(date_attr);
		attributeBuilder.setMinOccurs(0);
		attributeBuilder.setMaxOccurs(1);
		attributeBuilder.setNillable(true);
		attrDescs[4]=attributeBuilder.buildDescriptor(date_attr);

		String int_attr = "aint";
		attributeBuilder.setBinding(Integer.class);
		attributeBuilder.setName(int_attr);
		attributeBuilder.setMinOccurs(0);
		attributeBuilder.setMaxOccurs(1);
		attributeBuilder.setNillable(true);
		attrDescs[5]=attributeBuilder.buildDescriptor(int_attr);
		
		Iom_jObject inputObj=new Iom_jObject("Test1.Topic1.Point2", "o1");
		inputObj.setattrvalue(id1_attr, "1");
		inputObj.setattrvalue(text_attr, "text1");
		inputObj.setattrvalue(double_attr, "53434");
		IomObject coordValue=inputObj.addattrobj(geom_attr, "COORD");
		coordValue.setattrvalue("C1", "-0.4025974025974026");
		coordValue.setattrvalue("C2", "1.3974025974025972");
		inputObj.setattrvalue(date_attr, "2017-04-22");
		inputObj.setattrvalue(int_attr, "1234");
		ShapeWriter writer = null;
		File file = new File(TEST_OUT,"Point2.shp");
		try {
			writer = new ShapeWriter(file);
			writer.setAttributeDescriptors(attrDescs);
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
		{
			//Open the file for reading
        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"Point2.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		if(featureCollectionIter.hasNext()) {
				// feature object
				SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
				Object attr1=shapeObj.getAttribute(id1_attr);
				assertEquals("1",attr1.toString());
				Object attr2=shapeObj.getAttribute(text_attr);
				assertEquals("text1",attr2.toString());
				Object attr3=shapeObj.getAttribute(double_attr);
				assertEquals(53434.0,attr3);
				Object attr4=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
				assertEquals("POINT (-0.4025974025974026 1.3974025974025972)",attr4.toString());
				Object attr5=shapeObj.getAttribute(date_attr);
				assertEquals("2017-04-22",new SimpleDateFormat("yyyy-MM-dd").format(attr5));
				Object attr6=shapeObj.getAttribute(int_attr);
				assertEquals(Integer.class.getName(),attr6.getClass().getName());
				assertEquals(1234,attr6);
    		}
    		featureCollectionIter.close();
    		dataStore.dispose();
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
		File file = new File(TEST_OUT,"NoModelSetPointOk.shp");
		try {
			writer = new ShapeWriter(file);
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
		{
			//Open the file for reading
        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"NoModelSetPointOk.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		if(featureCollectionIter.hasNext()) {
				// feature object
				SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
				Object attr1=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
				assertEquals("POINT (-0.2285714285714285 0.5688311688311687)",attr1.toString());
    		}
    		featureCollectionIter.close();
    		dataStore.dispose();
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
		
		ShapeWriter writer = null;
		File file = new File(TEST_OUT,"MultiPoint.shp");
		try {
			writer = new ShapeWriter(file);
			//writer.setModel(td);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"MultiPoint.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) {
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			Object attr2=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTIPOINT ((-0.2285714285714285 0.5688311688311687), (-0.1922077922077922 0.6935064935064934), (-0.4883116883116884 0.3272727272727272))",attr2.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
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
		File file = new File(TEST_OUT,"MultiPoint2.shp");
		try {
			writer = new ShapeWriter(file);
			//writer.setModel(td);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"MultiPoint2.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) {
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			Object attr1=shapeObj.getAttribute("textattr2");
			assertEquals("text1",attr1.toString());
			Object attr2=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTIPOINT ((-0.2285714285714285 0.5688311688311687), (-0.1922077922077922 0.6935064935064934), (-0.4883116883116884 0.3272727272727272))",attr2.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird, wenn ein Polygon in einen LineString konvertiert wird.
	@Test
	public void lineString_Ok() throws IoxException, IOException, Ili2cFailure{
		Iom_jObject objStraightsSuccess=new Iom_jObject("Test1.Topic1.LineString", "o1");
		IomObject polylineValue=objStraightsSuccess.addattrobj(ShapeReader.GEOTOOLS_THE_GEOM, "POLYLINE");
		IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
		IomObject coordStart=segments.addattrobj("segment", "COORD");
		IomObject coordEnd=segments.addattrobj("segment", "COORD");
		coordStart.setattrvalue("C1", "-0.22857142857142854");
		coordStart.setattrvalue("C2", "0.5688311688311687");
		coordEnd.setattrvalue("C1", "-0.22557142857142853");
		coordEnd.setattrvalue("C2", "0.5658311688311687");
		ShapeWriter writer = null;
		File file = new File(TEST_OUT,"LineString.shp");
		try {
			writer = new ShapeWriter(file);
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
		{
			//Open the file for reading
        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new File(TEST_OUT,"LineString.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		if(featureCollectionIter.hasNext()) {
				// feature object
				SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
				Object attr2=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
				MultiLineString multiline=(MultiLineString)attr2;
				assertEquals("MULTILINESTRING ((-0.2285714285714285 0.5688311688311687, -0.2255714285714285 0.5658311688311687))",attr2.toString());
    		}
    		featureCollectionIter.close();
    		dataStore.dispose();
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
		File file = new File(TEST_OUT,"LineString2.shp");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(file);
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
		{
			//Open the file for reading
        	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new File(TEST_OUT,"LineString2.shp"));
        	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
    		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
    		if(featureCollectionIter.hasNext()) {
				// feature object
				SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
				Object attr1=shapeObj.getAttribute("attr1LS");
				assertEquals("text1",attr1.toString());
				Object attr2=shapeObj.getAttribute("attr2LS");
				assertEquals("5",attr2.toString());
				Object attr3=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
				assertEquals("MULTILINESTRING ((-0.2285714285714285 0.5688311688311687, -0.2255714285714285 0.5658311688311687))",attr3.toString());
    		}
    		featureCollectionIter.close();
    		dataStore.dispose();
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
		File file = new File(TEST_OUT,"MultiLineString.shp");
		try {
			writer = new ShapeWriter(file);
			//writer.setModel(td);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"MultiLineString.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) {
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			Object attr2=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTILINESTRING ((-0.2285714285714285 0.5688311688311687, -0.2255714285714285 0.5658311688311687), (-0.2255714285714285 0.5658311688311687, -0.2275514285714285 0.5558351688311687))",attr2.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
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
		File file = new File(TEST_OUT,"MultiLineString2.shp");
		try {
			writer = new ShapeWriter(file);
			//writer.setModel(td);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"MultiLineString2.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) {
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			Object attr1=shapeObj.getAttribute("attr1MLS");
			assertEquals("text2",attr1.toString());
			Object attr2=shapeObj.getAttribute("attr2MLS");
			assertEquals("6",attr2.toString());
			Object attr3=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTILINESTRING ((-0.2285714285714285 0.5688311688311687, -0.2255714285714285 0.5658311688311687), (-0.2255714285714285 0.5658311688311687, -0.2275514285714285 0.5558351688311687))",attr3.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
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
		File file = new File(TEST_OUT,"Polygon.shp");
		try {
			writer = new ShapeWriter(file);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"Polygon.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) {
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			System.out.println(shapeObj.toString());
			Object attr2=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTIPOLYGON (((-0.2285714285714285 0.5688311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5688311688311687, -0.1585714285714285 0.5688311688311687, -0.2285714285714285 0.5688311688311687)))",attr2.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
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
		File file = new File(TEST_OUT,"Polygon2.shp");
		try {
			writer = new ShapeWriter(file);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"Polygon2.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) { //attr1PG, text2; attr2PG, 6
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			Object attr1=shapeObj.getAttribute("attr1PG");
			assertEquals("text2",attr1.toString());
			Object attr2=shapeObj.getAttribute("attr2PG");
			assertEquals("6",attr2.toString());
			Object attr3=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTIPOLYGON (((-0.2285714285714285 0.5688311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5688311688311687, -0.1585714285714285 0.5688311688311687, -0.2285714285714285 0.5688311688311687)))",attr3.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
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
		File file = new File(TEST_OUT,"MultiPolygon.shp");
		try {
			writer = new ShapeWriter(file);
			//writer.setModel(td);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"MultiPolygon.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) {
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			Object attr2=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTIPOLYGON (((-0.228 0.568, -0.158 0.588, -0.158 0.588, -0.158 0.568, -0.158 0.568, -0.228 0.568)), ((0.228 1.3, 0.158 0.5, 0.158 0.5, 0.158 1.568, 0.158 1.568, 0.228 1.3)))",attr2.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
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
		File file = new File(TEST_OUT,"MultiPolygon2.shp");
		try {
			writer = new ShapeWriter(file);
			//writer.setModel(td);
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
		//Open the file for reading
    	FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT,"MultiPolygon2.shp"));
    	SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
		SimpleFeatureIterator featureCollectionIter=featuresSource.getFeatures().features();
		if(featureCollectionIter.hasNext()) {
			// feature object
			SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();
			Object attr1=shapeObj.getAttribute("attr1MPG");
			assertEquals("text3",attr1.toString());
			Object attr2=shapeObj.getAttribute("attr2MPG");
			assertEquals("8",attr2.toString());
			Object attr3=shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
			assertEquals("MULTIPOLYGON (((-0.228 0.568, -0.158 0.588, -0.158 0.588, -0.158 0.568, -0.158 0.568, -0.228 0.568)), ((0.228 1.3, 0.158 0.5, 0.158 0.5, 0.158 1.568, 0.158 1.568, 0.228 1.3)))",attr3.toString());
		}
		featureCollectionIter.close();
		dataStore.dispose();
	}
	
	// die srid wird gesetzt und ist falsch oder kann nicht gefunden werden.
	@Test
	public void sridWrong_Fail() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C2", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			writer = new ShapeWriter(new File(TEST_OUT,"sridWrong_Fail.shp"));
			writer.setModel(td);
			writer.setDefaultSridCode("99999999");
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
			File file = new File(TEST_OUT,"ClassOfModelNotFound_Fail.shp");
			writer = new ShapeWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("Test1.Topic1.Point99"));
			assertTrue(e.getMessage().contains("not found in"));
			assertTrue(e.getMessage().contains("Test1"));
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
	public void failedToConvertToJts_Fail() throws IoxException, IOException {
		Iom_jObject objSuccess=new Iom_jObject("Test1.Topic1.Point", "o1");
		IomObject coordValue=objSuccess.addattrobj("attrPoint", "COORD");
		coordValue.setattrvalue("C1", "-0.22857142857142854");
		coordValue.setattrvalue("C3", "0.5688311688311687");
		ShapeWriter writer = null;
		try {
			File file = new File(TEST_OUT,"failedToConvertToJts_Fail.shp");
			writer = new ShapeWriter(file);
			writer.setModel(td);
			writer.write(new StartTransferEvent());
			writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
			writer.write(new ObjectEvent(objSuccess));
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().equals("failed to convert COORD to jts"));
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
	
	
	// Ueberlange Attributnamen (> 10 Zeichen)
	// Algorithmus alleine (NameUtility.shortcutName()) kann doch wieder
	// zu Namenskonflikten fuehren. Aus diesem Grund muessen nach dem
	// Kuerzen alle zusammen betrachtet werden.
	@Test
	public void verySimilarLongAttributName_pointAttribute_Ok() throws IoxException, IOException {
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Point2", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("ino2_2020_range", "text1");
        inputObj.setattrvalue("ino2_2010_range", "text2");
        inputObj.setattrvalue("Double", "53434");
        IomObject coordValue = inputObj.addattrobj("attrPoint2", "COORD");
        coordValue.setattrvalue("C1", "-0.4025974025974026");
        coordValue.setattrvalue("C2", "1.3974025974025972");

        ShapeWriter writer = null;
        File file = new File(TEST_OUT, "Point2.shp");
        try {
            writer = new ShapeWriter(file);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1", "bid1"));
            writer.write(new ObjectEvent(inputObj));
            writer.write(new EndBasketEvent());
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

        {
            // Open the file for reading
            FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT, "Point2.shp"));
            SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
            SimpleFeatureIterator featureCollectionIter = featuresSource.getFeatures().features();
            if (featureCollectionIter.hasNext()) {
                // feature object
                SimpleFeature shapeObj = (SimpleFeature) featureCollectionIter.next();
                
                Object attr1 = shapeObj.getAttribute("id1");
                assertEquals("1", attr1.toString());
                Object attr2 = shapeObj.getAttribute("ino2_rnge");
                assertEquals("text1", attr2.toString());
                Object attr3 = shapeObj.getAttribute("ino2_rnge1");
                assertEquals("text2", attr3.toString());
                Object attr5 = shapeObj.getAttribute("Double");
                assertEquals("53434", attr5.toString());
                Object attr6 = shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
                assertEquals("POINT (-0.4025974025974026 1.3974025974025972)", attr6.toString());
            }
            featureCollectionIter.close();
            dataStore.dispose();
        }
	}
	
    // Ueberlange Attributnamen (> 10 Zeichen)
    // Gekuerzte Attributnamen (substring) waeren gleich. Algorithmus muss 
    // eindeutige Namen finden.
    @Test
    public void similarLongAttributeName_pointAttribute_Ok() throws IoxException, IOException, Ili2cFailure {
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Point2", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("BodenbedeckungArt", "text1");
        inputObj.setattrvalue("BodenbedeckungPos", "text2");
        inputObj.setattrvalue("BodenbedeckungDatum", "text3");        
        inputObj.setattrvalue("Double", "53434");
        IomObject coordValue = inputObj.addattrobj("attrPoint2", "COORD");
        coordValue.setattrvalue("C1", "-0.4025974025974026");
        coordValue.setattrvalue("C2", "1.3974025974025972");
        ShapeWriter writer = null;
        File file = new File(TEST_OUT, "Point2a.shp");
        try {
            writer = new ShapeWriter(file);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1", "bid1"));
            writer.write(new ObjectEvent(inputObj));
            writer.write(new EndBasketEvent());
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
        {
            // Open the file for reading
            FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT, "Point2a.shp"));
            SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
            SimpleFeatureIterator featureCollectionIter = featuresSource.getFeatures().features();
            if (featureCollectionIter.hasNext()) {
                // feature object
                SimpleFeature shapeObj = (SimpleFeature) featureCollectionIter.next();
                
                Object attr1 = shapeObj.getAttribute("id1");
                assertEquals("1", attr1.toString());
                Object attr2 = shapeObj.getAttribute("BodngDtum");
                assertEquals("text3", attr2.toString());
                Object attr3 = shapeObj.getAttribute("BodnngArt");
                assertEquals("text1", attr3.toString());
                Object attr4 = shapeObj.getAttribute("BodnngPos");
                assertEquals("text2", attr4.toString());
                Object attr5 = shapeObj.getAttribute("Double");
                assertEquals("53434", attr5.toString());
                Object attr6 = shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
                assertEquals("POINT (-0.4025974025974026 1.3974025974025972)", attr6.toString());
            }
            featureCollectionIter.close();
            dataStore.dispose();
        }
    }
    
    // Ueberlanger Attributnamen (> 10 Zeichen)
    // Modell wird gesetzt
    @Test
    public void setModel_longAttributeName_pointAttribute_Ok() throws IoxException, IOException, Ili2cFailure {
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Point3", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("SehrLangerText", "text1");
        inputObj.setattrvalue("Double", "53434");
        IomObject coordValue = inputObj.addattrobj("attrPoint2", "COORD");
        coordValue.setattrvalue("C1", "-0.4025974025974026");
        coordValue.setattrvalue("C2", "1.3974025974025972");
        ShapeWriter writer = null;
        File file = new File(TEST_OUT, "Point2.shp");
        try {
            writer = new ShapeWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1", "bid1"));
            writer.write(new ObjectEvent(inputObj));
            writer.write(new EndBasketEvent());
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
        {
            // Open the file for reading
            FileDataStore dataStore = FileDataStoreFinder.getDataStore(new java.io.File(TEST_OUT, "Point2.shp"));
            SimpleFeatureSource featuresSource = dataStore.getFeatureSource();
            SimpleFeatureIterator featureCollectionIter = featuresSource.getFeatures().features();
            if (featureCollectionIter.hasNext()) {
                // feature object
                SimpleFeature shapeObj = (SimpleFeature) featureCollectionIter.next();
                Object attr1 = shapeObj.getAttribute("id1");
                assertEquals("1", attr1.toString());
                Object attr2 = shapeObj.getAttribute("SehrrText"); // Shapefile-tauglicher Attributnamen (9 oder 10 Zeichen)
                assertEquals("text1", attr2.toString());
                Object attr3 = shapeObj.getAttribute("Double");
                assertEquals("53434", attr3.toString());
                Object attr4 = shapeObj.getAttribute(ShapeReader.GEOTOOLS_THE_GEOM);
                assertEquals("POINT (-0.4025974025974026 1.3974025974025972)", attr4.toString());
            }
            featureCollectionIter.close();
            dataStore.dispose();
        }
    }
}