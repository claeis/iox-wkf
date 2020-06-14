package ch.interlis.ioxwkf.shp;

import org.junit.Before;
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
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;

public class ShapeReaderTest {
	
	private static final String TEST_IN="src/test/data/ShapeReader/";
	
	@Before
	public void setup() throws Ili2cFailure
	{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry(TEST_IN+"Point/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		assertNotNull(td);
	}
	
	// Es wird getestet ob ein Point Element in ein Interlis IomObject convertiert werden kann.
	@Test
	public void singlePoint_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Point/Point.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.5688311688311687"));
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob ein Model an den Reader gegeben werden kann und die objecttag Informationen des IomObjects
	// mit den Informationen des Models uebereinstimmen.
	@Test
	public void setModel_singlePoint_Ok() throws IoxException, IOException, Ili2cFailure{
		ShapeReader reader=null;
		try {
			// compile model
			Configuration ili2cConfig=new Configuration();
			FileEntry fileEntry=new FileEntry(TEST_IN+"Point/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
			ili2cConfig.addFileEntry(fileEntry);
			TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
			assertNotNull(td);
			reader=new ShapeReader(new File(TEST_IN+"Point/Point.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject attrObj=iomObj.getattrobj("geometry", 0);
			assertTrue(iomObj.getobjecttag().equals("ShapeModel.Topic1.Point"));
			assertTrue(attrObj.getattrvalue("C1").equals("-0.22857142857142854"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.5688311688311687"));
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob der Model Name dem Shapefile Namen entspricht.
	@Test
	public void modelName_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Point/Point.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getobjecttag().contains("Point"));
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
	
	// Es wird getestet ob der Model Name dem Model Name entspricht.
	@Test
	public void setModel_modelName_Ok() throws IoxException, IOException, Ili2cFailure{
		ShapeReader reader=null;
		try {
			// compile model
			Configuration ili2cConfig=new Configuration();
			FileEntry fileEntry=new FileEntry(TEST_IN+"Point/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
			ili2cConfig.addFileEntry(fileEntry);
			TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
			assertNotNull(td);
			reader=new ShapeReader(new File(TEST_IN+"Point/Point.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getobjecttag().contains("ShapeModel"));
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
	
	// Es wird getestet ob der Topic Name "Topic" entspricht.
	@Test
	public void topicName_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Point/Point.shp"));
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
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob der Topic Name dem sich im Model befindenden Topic Namen entspricht.
	@Test
	public void setModel_topicName_Ok() throws IoxException, IOException, Ili2cFailure{
		ShapeReader reader=null;
		try {
			// compile model
			Configuration ili2cConfig=new Configuration();
			FileEntry fileEntry=new FileEntry(TEST_IN+"Point/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
			ili2cConfig.addFileEntry(fileEntry);
			TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
			assertNotNull(td);
			reader=new ShapeReader(new File(TEST_IN+"Point/Point.shp"));
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
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob der Name der Klasse "Class" enthaltet.
	@Test
	public void className_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Point/Point.shp"));
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
		}catch(Exception e) {
			throw new IoxException(e);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob der Name der Klasse dem Model Klassen Name entspricht.
	@Test
	public void setModel_className_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Point/Point.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			assertTrue(iomObj.getobjecttag().contains("Point"));
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
	
	// Es wird getestet ob die oid der Objekte unterschiedlich sind.
	@Test
	public void oidsAreUnique_Ok() throws IoxException, IOException{
		HashSet<String> objectIds=new HashSet<String>();
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"MultiPoint/MultiPoint.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			while(!(event instanceof EndBasketEvent)) {
				IomObject iomObj=((ObjectEvent)event).getIomObject();
				if(!objectIds.contains(iomObj.getobjectoid())){
					objectIds.add(iomObj.getobjectoid());
					event=reader.read();
				}else {
					fail();
				}
			}
			assertTrue(event instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}catch(Exception ex){
			ex.getMessage().contains("expected unique object id.");
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob ein MultiPoint Element in ein Interlis IomObject convertiert werden kann.
	@Test
	public void multiPoint_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"MultiPoint/MultiPoint.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.38701298701298703"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.8259740259740259"));
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.19220779220779216"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.6935064935064934"));
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.48831168831168836"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.32727272727272716"));
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.6649350649350649"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.5116883116883116"));
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.4233766233766234"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.5402597402597402"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob ein MultiPoint Element in ein Interlis IomObject convertiert werden kann.
	// Model wurde gesetzt.
	@Test
	public void setModel_MultiPoint_Ok() throws IoxException, IOException, Ili2cFailure{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry(TEST_IN+"MultiPoint/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"MultiPoint/MultiPoint.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.38701298701298703"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.8259740259740259"));
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.19220779220779216"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.6935064935064934"));
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.48831168831168836"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.32727272727272716"));
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.6649350649350649"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.5116883116883116"));
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			attrObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(attrObj.getattrvalue("C1").equals("-0.4233766233766234"));
			assertTrue(attrObj.getattrvalue("C2").equals("0.5402597402597402"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob ein LineString Element in ein Interlis IomObject convertiert werden kann.
	@Test
	public void singleLineString_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"LineString/LineString.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multiPolylineObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			IomObject sequence=multiPolylineObj.getattrobj("sequence", 0);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-1.0462287104622872"));
			assertTrue(segment.getattrvalue("C2").equals("0.47688564476885653"));
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("0.5547445255474452"));
			assertTrue(segment2.getattrvalue("C2").equals("0.15328467153284664"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob ein LineString Element in ein Interlis IomObject convertiert werden kann.
	// Model wurde gesetzt.
	@Test
	public void setModel_SingleLineString_Ok() throws IoxException, IOException, Ili2cFailure{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry(TEST_IN+"LineString/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"LineString/LineString.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multiPolylineObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			IomObject sequence=multiPolylineObj.getattrobj("sequence", 0);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-1.0462287104622872"));
			assertTrue(segment.getattrvalue("C2").equals("0.47688564476885653"));
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("0.5547445255474452"));
			assertTrue(segment2.getattrvalue("C2").equals("0.15328467153284664"));
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob zwei LineString Elemente in zwei Interlis IomObjects convertiert werden koennen.
	@Test
	public void singleParallelLineString_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"LineString2/LineString.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multiPolylineObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			IomObject sequence=multiPolylineObj.getattrobj("sequence", 0);
			IomObject segment=sequence.getattrobj("segment", 0);
			assertTrue(segment.getattrvalue("C1").equals("-2.2610421208961364"));
			assertTrue(segment.getattrvalue("C2").equals("1.4415521218440928"));
			IomObject segment2=sequence.getattrobj("segment", 1);
			assertTrue(segment2.getattrvalue("C1").equals("0.19112712105412832"));
			assertTrue(segment2.getattrvalue("C2").equals("0.962795841627959"));
			
			assertTrue(reader.read() instanceof ObjectEvent);
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob ein MultiLineString Element in ein Interlis IomObject convertiert werden kann.
	@Test
	public void multiLineString_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"MultiLineString/MultiLineString.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multiPolylineObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
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
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob ein MultiLineString Element in ein Interlis IomObject convertiert werden kann.
	// Model wurde gesetzt.
	@Test
	public void setModel_MultiLineString_Ok() throws IoxException, IOException, Ili2cFailure{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry(TEST_IN+"MultiLineString/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"MultiLineString/MultiLineString.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multiPolylineObj=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
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
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob ein SinglePolygon Element in ein Interlis IomObject convertiert werden kann.
	@Test
	public void singlePolygon_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Polygon/Polygon.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multisurface=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			
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
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
		}
	}
	
	// Es wird getestet ob ein SinglePolygon Element in ein Interlis IomObject convertiert werden kann.
	// Model wurde gesetzt.
	@Test
	public void setModel_SinglePolygon_Ok() throws IoxException, IOException, Ili2cFailure{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry(TEST_IN+"Polygon/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Polygon/Polygon.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multisurface=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			
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
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}	
	
	// Es wird getestet ob ein MultiPolygon Element in ein Interlis IomObject convertiert werden kann.
	@Test
	public void multiPolygon_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		IoxEvent event=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"MultiPolygon/MultiPolygon.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			IomObject multisurface=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			multisurface=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			multisurface=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
	    	if(event!=null) {
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
		FileEntry fileEntry=new FileEntry(TEST_IN+"MultiPolygon/ShapeModel.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		IomObject multisurface=null;
		ShapeReader reader=null;
		IoxEvent event=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"MultiPolygon/MultiPolygon.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			multisurface=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			multisurface=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			iomObj=((ObjectEvent)event).getIomObject();
			multisurface=iomObj.getattrobj(ShapeReader.GEOTOOLS_THE_GEOM, 0);
			assertTrue(multisurface.getattrvaluecount("surface")==1);
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		ili2cConfig=null;
	    		fileEntry=null;
	    		multisurface=null;
	    		td2=null;
	    		reader.close();
	    		reader=null;
	    	}
	    	if(event!=null) {
	    		event=null;
	    	}
    	}
	}
	
	// Es wird getestet ob Attribute Elemente in Interlis IomObjects convertiert werden koennen.
	@Test
	public void attributes_Ok() throws IoxException, IOException{
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Attributes/Attributes.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			String attr1=iomObj.getattrvalue("Integer");
			assertTrue(attr1.equals("8"));
			String attr2=iomObj.getattrvalue("Text");
			assertTrue(attr2.equals("text1"));
			String attr3=iomObj.getattrvalue("id");
			assertTrue(attr3.equals("1"));
			String attr4=iomObj.getattrvalue("Double");
			assertTrue(attr4.equals("53434.0"));
			assertEquals("2015-11-03",iomObj.getattrvalue("adate"));
			// TODO boolean
			// TODO timestamp
			
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob Attribute Elemente in Interlis IomObjects convertiert werden koennen und die Attributenamen mit den Model Attribute Namen uebereinstimmen.
	// Model wurde gesetzt.
	@Test
	public void setModel_attributes_Ok() throws IoxException, IOException, Ili2cFailure{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry(TEST_IN+"Attributes/ShapeModelAttrs.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Attributes/Attributes.shp"));
			assertTrue(reader.read() instanceof StartTransferEvent);
			reader.setModel(td2);
			assertTrue(reader.read() instanceof StartBasketEvent);
			
			IoxEvent event=reader.read();
			assertTrue(event instanceof ObjectEvent);
			IomObject iomObj=((ObjectEvent)event).getIomObject();
			String attr1=iomObj.getattrvalue("Integer");
			assertEquals("8",attr1);
			String attr2=iomObj.getattrvalue("Text");
			assertEquals("text1",attr2);
			String attr3=iomObj.getattrvalue("id");
			assertEquals("1",attr3);
			String attr4=iomObj.getattrvalue("Double");
			assertEquals("53434.0",attr4);
			assertTrue(reader.read() instanceof EndBasketEvent);
			assertTrue(reader.read() instanceof EndTransferEvent);
		}finally {
	    	if(reader!=null) {
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
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Attributes/AttributesNull.shp"));
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
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	// Es wird getestet ob die Attributnamen, bei unterschiedlicher Gross/Kleinschreibung in der Shp Datei, gem. ili Modell gelesen werden
	@Test
	public void setModel_similarAttributes_Ok() throws IoxException, IOException, Ili2cFailure{
		// compile model
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntry=new FileEntry(TEST_IN+"Attributes/ShapeModelSimilarAttrs.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td2=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"Attributes/Attributes.shp"));
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
		}finally {
	    	if(reader!=null) {
	    		reader.close();
	    		reader=null;
	    	}
    	}
	}
	
	// Es wird getestet, ob mehrere Klassen mit zutreffenen Attributen zu einer Exception fuehren 
	@Test
    public void multipleClassFoundInModel_Fail() throws IoxException, Ili2cFailure, IOException{
		// ili-datei lesen
		ShapeReader reader=null;
		TransferDescription tdM=null;
		Configuration ili2cConfig=new Configuration();
		FileEntry fileEntryConditionClass=new FileEntry(TEST_IN+"/Polygon/StadtModel.ili", FileEntryKind.ILIMODELFILE); // first input model
		ili2cConfig.addFileEntry(fileEntryConditionClass);
		FileEntry fileEntry=new FileEntry(TEST_IN+"/Polygon/KantonModel.ili", FileEntryKind.ILIMODELFILE); // second input model
		ili2cConfig.addFileEntry(fileEntry);
		FileEntry fileEntry2=new FileEntry(TEST_IN+"/Polygon/BundesModel.ili", FileEntryKind.ILIMODELFILE); // third input model
		ili2cConfig.addFileEntry(fileEntry2);
		tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		assertNotNull(tdM);
		try {
			reader=new ShapeReader(new File(TEST_IN+"Polygon/Polygon.shp"));
			reader.setModel(tdM);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			IoxEvent event=reader.read();
			assertEquals("BundesModel.Topic1.Polygon",((ObjectEvent)event).getIomObject().getobjecttag());
		}finally {
			if(reader!=null) {
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
		FileEntry fileEntry=new FileEntry(TEST_IN+"LineString/ShapeModelDiffAttrCount.ili", FileEntryKind.ILIMODELFILE);
		ili2cConfig.addFileEntry(fileEntry);
		TransferDescription td=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
		assertNotNull(td);
		ShapeReader reader=null;
		try {
			reader=new ShapeReader(new File(TEST_IN+"LineString/LineString.shp"));
			reader.setModel(td);
			assertTrue(reader.read() instanceof StartTransferEvent);
			assertTrue(reader.read() instanceof StartBasketEvent);
			reader.read();
			fail();
		}catch(IoxException ex){
    		assertEquals("attributes 'the_geom' not found in model: 'ShapeModel'.",ex.getMessage());
    	}finally {
	    	if(reader!=null) {
		    	reader.close();
				reader=null;
	    	}
    	}
	}
	
	// Es wird getestet ob ein Point Element in ein Interlis IomObject convertiert werden kann.
	@Test
	public void wrongFormat_Fail() throws IoxException, IOException{
		ShapeReader reader=null;
    	try{
    		reader=new ShapeReader(new File(TEST_IN+"Point/Point.shx"));
    		fail();
    	}catch(Exception ex){
    		assertTrue(ex.getMessage().contains("expected shape file"));
    	}finally {
	    	if(reader!=null) {
		    	reader.close();
				reader=null;
	    	}
    	}
	}
}