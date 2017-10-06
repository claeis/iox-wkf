package ch.interlis.iom_j.shp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.type.AttributeTypeImpl;
import org.geotools.feature.type.GeometryTypeImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import ch.interlis.ili2c.metamodel.DataModel;
import ch.interlis.ili2c.metamodel.Model;
import ch.interlis.ili2c.metamodel.Topic;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.converter.WkfJts2iox;
import ch.interlis.iox_j.jts.Jts2iox;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxReader;
	
public class ShapeReader implements IoxReader{
	// state
	private int state;
	private static final int START=0;
	private static final int INSIDE_TRANSFER=1;
	private static final int INSIDE_BASKET=2;
	private static final int INSIDE_OBJECT=3;
	private static final int END_BASKET=4;
	private static final int END_TRANSFER=5;
	private static final int END=6;
	
	// shape reader
	private SimpleFeatureIterator featureCollectionIter=null;
	private FileDataStore dataStore=null;
	private SimpleFeatureSource featuresSource=null;
	
	// iox
	private TransferDescription td;
	private IoxFactoryCollection factory=new ch.interlis.iox_j.DefaultIoxFactoryCollection();
	private java.io.File inputFile=null;
	private int nextId=1;
	
	// model, topic, class
	private String topicIliQName="Topic";
	private String classIliQName=null;
	
	private List<AttributeDescriptor> shapeAttributeDescriptors;
		
	/** Creates a new reader.
	 * @param shpFile to read from
	 */
	public ShapeReader(java.io.File shpFile) throws IoxException, IOException{
		state=START;
		td=null;
		inputFile=new java.io.File(shpFile.getPath());
		init(inputFile);
	}
	
	/** Initialize file content.
	 * @param java.io.File shapeFile
	 * @throws IoxException
	 */
	private void init(java.io.File shapeFile) throws IOException, IoxException{
		factory=new ch.interlis.iox_j.DefaultIoxFactoryCollection();
		// store data of shape file
		dataStore = FileDataStoreFinder.getDataStore(shapeFile);
		if(dataStore==null) {
			throw new IoxException("expected shape file");
		}
		featuresSource = dataStore.getFeatureSource();
        featureCollectionIter=featuresSource.getFeatures().features();
	}
	
	/**
	 * set model.
	 * @param td, transfer description.
	 */
	public void setModel(TransferDescription td){
		this.td=td;
	}

	/**
	 * read the path of input shape file and get the single name of shape file.
	 * @throws IoxException
	 */
	private String getNameOfDataFile() throws IoxException{
		// get path of the shp file
		String path=inputFile.getPath();
		if(path!=null){
			String[] pathParts=path.split("\\\\");
			int partLength=pathParts.length;
			String file=pathParts[partLength-1];
			String[] fileParts=file.split(".shp");
			file=fileParts[0];
			return file;
		}else{
			throw new IoxException("expected shp file");
		}
	}
	
	/**
	 * read file elements in simple feature file.
	 * @return IoxEvent's.
	 */
	@Override
    public IoxEvent read() throws IoxException{
		IomObject iomObj = null;
		String attrType=null;
		
		if(state==START){
			state=INSIDE_TRANSFER;
			topicIliQName=null;
			classIliQName=null;
			return new ch.interlis.iox_j.StartTransferEvent();
		}
		if(state==INSIDE_TRANSFER){
			state=INSIDE_BASKET;
		}
		if(state==INSIDE_BASKET){
			String featureTypeName=null;
			SimpleFeatureIterator headerCollectionIter=null;
			try {
		        headerCollectionIter=featuresSource.getFeatures().features();
				if(headerCollectionIter.hasNext()) {
					// feature object
					SimpleFeature firstFeature=(SimpleFeature) headerCollectionIter.next();	
					//simple feature type
					SimpleFeatureType featureType=firstFeature.getFeatureType();
					shapeAttributeDescriptors = featureType.getAttributeDescriptors();
					featureTypeName=featureType.getName().getLocalPart();
				}
			} catch (IOException e) {
				throw new IoxException(e);
			}finally {
				if(headerCollectionIter!=null) {
					headerCollectionIter.close();
					headerCollectionIter=null;
				}
			}
			// class name
			Viewable viewable=findViewable(featureTypeName);
			if(viewable==null){
				topicIliQName=getNameOfDataFile()+".Topic";
				classIliQName=topicIliQName+".Class"+getNextId();
			}else{
				// get model data
				topicIliQName=viewable.getContainer().getScopedName();
				classIliQName=viewable.getScopedName();
			}
			if(topicIliQName!=null) {
				String bid="b"+getNextId();
				state=INSIDE_OBJECT;
				return new ch.interlis.iox_j.StartBasketEvent(topicIliQName, bid);
			}
		}
		if(state==INSIDE_OBJECT){
			if(featureCollectionIter.hasNext()) {
				// feature object
				SimpleFeature shapeObj=(SimpleFeature) featureCollectionIter.next();	
				iomObj=createIomObject(classIliQName, null);
				
	        	for(AttributeDescriptor attrDescripter : shapeAttributeDescriptors) {
	        		IomObject subIomObj=null;
	        		// attribute name
	        		String attrName=attrDescripter.getLocalName();
	        		// attribute type
	        		AttributeType typeOfAttribute=attrDescripter.getType();
	        		
	        		if(typeOfAttribute instanceof GeometryTypeImpl) {
	        			GeometryTypeImpl geoTypeImpl=(GeometryTypeImpl)typeOfAttribute;
	        			attrType=geoTypeImpl.getName().toString();
		        		// attribute value of feature object
		        		Object attrValue=shapeObj.getAttribute(attrName);
	    				
		        		if(attrValue instanceof MultiLineString) {
	    					// multiLineString
	        				MultiLineString multiLineString=(MultiLineString)attrValue;
	        				subIomObj=WkfJts2iox.JTS2multipolyline(multiLineString);
	        				iomObj.addattrobj(attrName, subIomObj);	            				
	    				
	    				}else if(attrValue instanceof MultiPoint) {
	    					// multiPoint
	        				MultiPoint multiPointObj=(MultiPoint)attrValue;
	        				subIomObj=WkfJts2iox.JTS2multicoord(multiPointObj);
	        				iomObj.addattrobj(attrName, subIomObj);
	        			
	    				}else if(attrValue instanceof MultiPolygon) {
	        				// multiPolygon
	        				MultiPolygon multiPolygonObj=(MultiPolygon)attrValue;
							subIomObj=WkfJts2iox.JTS2multisurface(multiPolygonObj);
	        				iomObj.addattrobj(attrName, subIomObj);
	
	        			}else if(attrValue instanceof LineString) {
	        				// lineString
	        				LineString lineStringObj=(LineString)attrValue;
							subIomObj=Jts2iox.JTS2polyline(lineStringObj);
	        				iomObj.addattrobj(attrName, subIomObj);
	        				
	        			}else if(attrValue instanceof Point) {
	        				// point
	        				Point pointObj=(Point)attrValue;
	        				Coordinate coord=pointObj.getCoordinate();
							subIomObj=Jts2iox.JTS2coord(coord);
							iomObj.addattrobj(attrName, subIomObj);
							
	        			}else if(attrValue instanceof Polygon) {
	        				// polygon
	        				Polygon polygonObj=(Polygon)attrValue;
							subIomObj=Jts2iox.JTS2surface(polygonObj);
	        				iomObj.addattrobj(attrName, subIomObj);
	        			}
		        		
	        		}else if(typeOfAttribute instanceof AttributeTypeImpl) {
	        			AttributeTypeImpl attrTypeImpl=(AttributeTypeImpl)typeOfAttribute;
	        			attrType=attrTypeImpl.getBinding().getSimpleName();
	        			// attribute value
		        		Object attrValue=shapeObj.getAttribute(attrName);
		        		if(attrValue!=null) {
		        			iomObj.setattrvalue(attrName, attrValue.toString());
		        		}
	        		}
	        	}
	        	// return each simple feature object.
	        	return new ch.interlis.iox_j.ObjectEvent(iomObj);
	        }
			featureCollectionIter.close();
			featureCollectionIter=null;
			state=END_BASKET;
	    }
		if(state==END_BASKET){
			state=END_TRANSFER;
			return new ch.interlis.iox_j.EndBasketEvent();
		}
		if(state==END_TRANSFER){
			state=END;
			return new ch.interlis.iox_j.EndTransferEvent();
		}
		return null;
	}

	private Viewable findViewable(String featureTypeName) {
		if(td!=null) {
			List<HashMap<String,Viewable>> allModels=setupNameMapping();
			for(HashMap<String,Viewable> map : allModels) {
				Viewable ret= map.get(featureTypeName);
				if(ret!=null) {
					return ret;
				}
			}
		}
		return null;
	}

	/** Iterate through ili file and set all models with class names and appropriate class object.
	 */
    private List<HashMap<String, Viewable>> setupNameMapping(){
    	List<HashMap<String, Viewable>> allModels=new ArrayList<HashMap<String, Viewable>>();
    	HashMap<String, Viewable> allClassesOfModel=null;
		Iterator tdIterator = td.iterator();
		while(tdIterator.hasNext()){
			allClassesOfModel=new HashMap<String, Viewable>();
			Object modelObj = tdIterator.next();
			if(!(modelObj instanceof DataModel)){
				continue;
			}
			// iliModel
			DataModel model = (DataModel) modelObj;
			Iterator modelIterator = model.iterator();
			while(modelIterator.hasNext()){
				Object topicObj = modelIterator.next();
				if(!(topicObj instanceof Topic)){
					continue;
				}
				// iliTopic
				Topic topic = (Topic) topicObj;
				// iliClass
				Iterator classIter=topic.iterator();
		    	while(classIter.hasNext()){
		    		Object classObj=classIter.next();
		    		if(!(classObj instanceof Viewable)){
    					continue;
    				}
		    		Viewable viewable = (Viewable) classObj;
	    			allClassesOfModel.put(viewable.getName(), viewable);
		    	}
			}
			allModels.add(0,allClassesOfModel);
		}
		return allModels;
    }
    
    /**
     * increase count is used to increase oid and bid.
     * @return increasing number.
     */
    private String getNextId(){
    	int count=nextId;
    	nextId+=1;
    	return String.valueOf(count);
    }
    
    /**
     * create a new IomObject with a unique object id.
     * @return created IomObject.
     */
    @Override
	public IomObject createIomObject(String type, String oid)throws IoxException{
    	if(oid==null){
			oid="o"+getNextId();
    	}
		return factory.createIomObject(type, oid);
	}
    
    /**
     * delete Path of inputFile.
     */
	@Override
	public void close() throws IoxException {
		if(inputFile!=null){
			inputFile=null;
		}
		if(featuresSource!=null){
			featuresSource=null;
		}
		if(dataStore!=null){
			dataStore.dispose();
			dataStore=null;
		}
		if(featureCollectionIter!=null) {
			featureCollectionIter=null;
		}
		if(td!=null) {
			td.clear();
			td=null;
		}
		if(factory!=null) {
			factory=null;
		}
	}
	
	/** get set factory.
	 */
	@Override
	public IoxFactoryCollection getFactory() throws IoxException{
		return factory;
	}
	
	/** set content of IoxFactoryCollection.
	 */
	@Override
	public void setFactory(IoxFactoryCollection factory) throws IoxException{
		this.factory=factory;
	}
}