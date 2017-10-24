/* This file is part of the iox-wkf project.
 * For more information, please see <http://www.eisenhutinformatik.ch/iox-wkf/>.
 *
 * Copyright (c) 2006 Eisenhut Informatik AG
 * Permission is hereby granted, free of charge, to any person obtaining a 
 * copy of this software and associated documentation files (the "Software"), 
 * to deal in the Software without restriction, including without limitation 
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included 
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
 * DEALINGS IN THE SOFTWARE.
 */
package ch.interlis.ioxwkf.shp;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.NameImpl;
import org.geotools.feature.PropertyImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeImpl;
import org.geotools.feature.type.GeometryDescriptorImpl;
import org.geotools.feature.type.GeometryTypeImpl;
import org.geotools.referencing.CRS;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.filter.identity.FeatureId;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import ch.interlis.iom.*;
import ch.interlis.iox.EndBasketEvent;
import ch.interlis.iox.EndTransferEvent;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.StartBasketEvent;
import ch.interlis.iox.StartTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.jts.Iox2jtsException;
import ch.interlis.ioxwkf.converter.Iox2multijts;
import ch.interlis.iom_j.ViewableProperties;
import ch.interlis.iom_j.ViewableProperty;
import ch.interlis.iom_j.xtf.Ili2cUtility;
import ch.interlis.ili2c.generator.Iligml20Generator;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.Element;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.ili2c.metamodel.ViewableTransferElement;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ShapeWriter implements ch.interlis.iox.IoxWriter {
	
	private DataStore dataStore=null; // --> data access
    private ViewableProperties mapping =null; // --> model data
    private List<AttributeDescriptor> attrDesc=null; // --> attribute type data
    private List<Object> attributes=null; // --> attribute name and value
	// geometry types
	private static final String POINT="pointProperty";
	private static final String MULTIPOINT="multipointProperty";
	private static final String LINESTRING="lineProperty";
	private static final String MULTILINESTRING="multilineProperty";
	private static final String POLYGON="polygonProperty";
	private static final String MULTIPOLYGON="multipolygonProperty";
	// geom type name
	private static final String GEOM="the_geom";
	// srid
	private String sridCode="2056"; // --> coordinate reference code. if null, default code will be set
	// model
	private static TransferDescription td=null;
	
	/** initialize file and model
	 * @param file
	 * @throws IoxException
	 */
    public ShapeWriter(java.io.File file) throws IoxException { 
		init(file);
    }
    
	private void init(File file) throws IoxException{
        Map<String, URL> map = new HashMap<String, URL>();
        // get file path
        try {
			map.put("url", file.toURL());
		} catch (MalformedURLException e2) {
			throw new IoxException(e2);
		}
        // create data store
        try {
			dataStore = DataStoreFinder.getDataStore(map);
		} catch (IOException e2) {
			throw new IoxException(e2);
		}
        // set access to file
		file.setExecutable(true);
		file.setWritable(true);
	}
	
	/** iterate through model/models and store data to map
	 * @param td
	 * @return
	 */
    private ViewableProperties createMapping(TransferDescription td) {
		ViewableProperties mapping=new ViewableProperties();
    	java.util.HashMap tagv=Iligml20Generator.createDef2NameMapping(td);
		Iterator tagi=tagv.keySet().iterator();
		for(;tagi.hasNext();){
			Element ili2cEle=(Element)tagi.next();
			String tag=null;
			ArrayList propv=null; // ViewableProperty
			if(ili2cEle instanceof AttributeDef){
				AttributeDef attr=(AttributeDef)ili2cEle;
				tag=ili2cEle.getContainer().getScopedName(null)+"."+ili2cEle.getName();
				propv=Ili2cUtility.mapLinetable(attr);
			}else if(ili2cEle instanceof Viewable){
				propv=new ArrayList(); // ViewableProperty
				Viewable v=(Viewable)ili2cEle;
				tag=v.getScopedName(null);
				Iterator iter = Iligml20Generator.getAttributesAndRoles2(v);
				while (iter.hasNext()) {
					ViewableTransferElement obj = (ViewableTransferElement)iter.next();
					ViewableProperty prop=Ili2cUtility.mapViewableTransferElement( v, obj);
					propv.add(prop);
				}
			}
			if(tag!=null){
				mapping.defineClass(tag, (ViewableProperty[])propv.toArray(new ViewableProperty[propv.size()]));
			}
		}
		return mapping;
	}
    
    /** get data of ioxEvent and validate data
     */
    @Override
	public void write(IoxEvent event) throws IoxException {
		if(event instanceof StartTransferEvent){
			// ignore
		}else if(event instanceof StartBasketEvent){
			if(td!=null) {
				mapping = createMapping(td);
			}
		}else if(event instanceof ObjectEvent){
			attributes=new ArrayList<Object>();
			attrDesc=new ArrayList<AttributeDescriptor>();
			ObjectEvent obj=(ObjectEvent) event;
			IomObject iomObj=(IomObject)obj.getIomObject();
			String tag = iomObj.getobjecttag();
			// check if class exist in model/models
			ViewableProperty[] attrv = null;
            if (mapping!=null && mapping.existsClass(tag)){
                attrv = mapping.getClassVProperties(tag);
            }else {
            	if(td==null) {
            		attrv = new ViewableProperty[iomObj.getattrcount()];
            		for(int u=0;u<iomObj.getattrcount();u++) {
            			String attrName=iomObj.getattrname(u);
            			ViewableProperty prop=new ViewableProperty(attrName);
            			attrv[u]=prop;
            		}
            	}else {
            		throw new IoxException("class "+iomObj.getobjecttag()+" not found in model "+td.getLastModel().getName());
            	}
            }
            storeAttributes(iomObj, attrv);
        	// write object attribute-values of model attribute-names
        	try {
        		FeatureCollection<SimpleFeatureType, SimpleFeature> features=convertObjects(iomObj, attrv);
        		if(features!=null) {
        			writeFeatureCollectionToShapefile(features);
        		}else {
        			throw new IoxException("no feature found in "+iomObj.toString());
        		}
			} catch (IOException e) {
				throw new IoxException("failed to write object "+iomObj.toString(),e);
			} catch (Iox2jtsException e) {
				throw new IoxException("failed to convert "+iomObj.toString()+" in jts",e);
			}
            close();
		}else if(event instanceof EndBasketEvent){
			// ignore
		}else if(event instanceof EndTransferEvent){
			// ignore
		}
	}
    
    /** iterate through attributes and store type-data, attrNames and attrValues
     * @param iomObj
     * @param attrv
     */
	private void storeAttributes(IomObject iomObj, ViewableProperty[] attrv) {
		for(int i=0;i<attrv.length;i++) {
			if(iomObj.getattrvalue(attrv[i].getName())!=null){
				String attrName=attrv[i].getName();
				String attrValue=iomObj.getattrvalue(attrName);
				//create the builder
				AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
				attributeBuilder.setName(attrName);
				attributeBuilder.setBinding(String.class);
				attributeBuilder.setMinOccurs(0);
				attributeBuilder.setMaxOccurs(1);
				attributeBuilder.setNillable(true);
				//build the descriptor
				AttributeDescriptor descriptor = attributeBuilder.buildDescriptor(attrName);
				// add descriptor to descriptor list
				attrDesc.add(descriptor);
				// create the property
				Property property=new PropertyImpl(attrValue, descriptor) {};
				attributes.add(property);
			}
		}
	}
	
	/** create simple feature type
	 * @param typeProperty
	 * @return type
	 * @throws IoxException
	 */
	private SimpleFeatureType getFeatureType(String typeProperty) throws IoxException {
		//create the builder
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName("shpType");
		builder.setNamespaceURI("http://www.geotools.org/");
		CoordinateReferenceSystem crs = null;
		CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
		try {
			crs = factory.createCoordinateReferenceSystem("EPSG:"+getSridCode());
		} catch (NoSuchAuthorityCodeException e) {
			throw new IoxException("coordinate reference: EPSG:"+getSridCode()+" not found",e);
		} catch (FactoryException e) {
			throw new IoxException(e);
		}
		// if no crs was set, set to the global
        if (builder.getCRS()==null) {
            builder.setCRS(crs);
        }
        builder.setDefaultGeometry(GEOM);
		if(typeProperty.equals(POINT)) {
			builder.add(GEOM, Point.class);
		}else if(typeProperty.equals(MULTIPOINT)) {
			builder.add(GEOM, MultiPoint.class );
		}else if(typeProperty.equals(LINESTRING)) {
			builder.add(GEOM, LineString.class );
		}else if(typeProperty.equals(MULTILINESTRING)) {
			builder.add(GEOM, MultiLineString.class );
		}else if(typeProperty.equals(POLYGON)) {
			builder.add(GEOM, Polygon.class );
		}else if(typeProperty.equals(MULTIPOLYGON)) {
			builder.add(GEOM, MultiPolygon.class );
		}else {
			// ignore --> attributes are already set at this point
		}
		// add index --> count of index is used to add multiple values
		builder.addAll(attrDesc);
		// build type
		SimpleFeatureType simpleFeatureType=builder.buildFeatureType();
		return simpleFeatureType;
	}
    
	/** convert IomObject to jts format
	 * @param obj
	 * @param attrv
	 * @return collection of all features and attributes in feature collection
	 * @throws IoxException
	 * @throws IOException
	 * @throws Iox2jtsException
	 */
    private FeatureCollection<SimpleFeatureType, SimpleFeature> convertObjects(IomObject obj, ViewableProperty[] attrv) throws IoxException, IOException, Iox2jtsException {
    	SimpleFeature feature=null;
		SimpleFeatureType type=null;
		FeatureCollection<SimpleFeatureType, SimpleFeature> features = null;
		List<SimpleFeature> simpleFeatureList=new ArrayList<SimpleFeature>();
		SimpleFeatureBuilder featureBuilder=null;
    	for (int i = 0; i < attrv.length; i++){
	    	GeometryFactory geometryFactory=new GeometryFactory();
	    	String attrName=attrv[i].getName();
	    	int valueCount=obj.getattrvaluecount(attrName);
			if(valueCount>0){
				String val=obj.getattrprim(attrName,0);
				// not a primitive
				if(val==null){
					IomObject child=obj.getattrobj(attrName,0);
					if (child != null){
						if (child.getobjecttag().equals("COORD")){
							// COORD
							Coordinate jtsCoord=null;
							try {
								// convert ili to jts
								jtsCoord=ch.interlis.iox_j.jts.Iox2jts.coord2JTS(child);
							} catch (Iox2jtsException e) {
								throw new IoxException("failed to convert "+child.toString()+" to jts",e);
							}
							if(valueCount > 1){
								throw new IoxException("max one COORD value allowed ("+attrName+")");
							}
							Geometry geometry=(geometryFactory.createPoint(jtsCoord));
							type=getFeatureType(POINT);
							geometry.setSRID(Integer.parseInt(getSridCode()));
							
							featureBuilder = new SimpleFeatureBuilder(type);
							featureBuilder.set(0, geometry);
							// add attribute-values
							if(attributes.size()>0) {
								writeAttrValuesToGeom(featureBuilder);
							}
							feature = featureBuilder.buildFeature(null);
							simpleFeatureList.add(feature);
						}else if (child.getobjecttag().equals("MULTICOORD")){
							// MULTICOORD
							CoordinateList jtsCoords=null;
							// convert ili to jts
							try {
								jtsCoords=Iox2multijts.multiCoord2JTS(child);
							} catch (Iox2jtsException e) {
								throw new IoxException("failed to convert "+child.toString()+" to jts",e);
							}
							if(valueCount > 1){
								throw new IoxException("max one MULTICOORD value allowed ("+attrName+")");
							}
							// write jtsCoord to shapefile
							Coordinate[] coords=jtsCoords.toCoordinateArray();
							Geometry geometry=(geometryFactory.createMultiPoint(coords));
							type=getFeatureType(MULTIPOINT);
							geometry.setSRID(Integer.parseInt(getSridCode()));
							
							featureBuilder = new SimpleFeatureBuilder(type);
							featureBuilder.set(0, geometry);
							// add attribute-values
							if(attributes.size()>0) {
								writeAttrValuesToGeom(featureBuilder);
							}
							feature = featureBuilder.buildFeature(null);
							simpleFeatureList.add(feature);
						}else if(child.getobjecttag().equals("POLYLINE")){
							// POLYLINE
							CoordinateList jtsLineString=null;
							try{
								jtsLineString=ch.interlis.iox_j.jts.Iox2jts.polyline2JTS(child, true, 0.00);
							}catch (Iox2jtsException e){
								throw new IoxException("failed to convert "+child.toString()+" to jts",e);
							}
							if(valueCount > 1){
								throw new IoxException("max one POLYLINE value allowed ("+attrName+")");
							}
							// convert list to array
							Coordinate[] coordArray = new Coordinate[jtsLineString.size()];
							coordArray = (Coordinate[]) jtsLineString.toArray(coordArray);
							// convert ili to jts
							Geometry geometry=(geometryFactory.createLineString(coordArray));
							type=getFeatureType(LINESTRING);
							geometry.setSRID(Integer.parseInt(getSridCode()));
							
							featureBuilder = new SimpleFeatureBuilder(type);
							featureBuilder.set(0, geometry);
							// add attribute-values
							if(attributes.size()>0) {
								writeAttrValuesToGeom(featureBuilder);
							}
							feature = featureBuilder.buildFeature(null);
							simpleFeatureList.add(feature);
						}else if (child.getobjecttag().equals("MULTIPOLYLINE")){
							// MULTIPOLYLINE
							LineString[] lineStrings=null;
							// convert ili to jts
							try {
								lineStrings=Iox2multijts.multiLineString2JTS(child);
							} catch (Iox2jtsException e) {
								throw new IoxException("failed to convert "+child.toString()+" to jts",e);
							}
							if(valueCount > 1){
								throw new IoxException("max one MULTIPOLYLINE value allowed ("+attrName+")");
							}
							// write jtsMultiLineString to shapefile
							Geometry geometry=(geometryFactory.createMultiLineString(lineStrings));
							type=getFeatureType(MULTILINESTRING);
							geometry.setSRID(Integer.parseInt(getSridCode()));
							
							featureBuilder = new SimpleFeatureBuilder(type);
							featureBuilder.set(0, geometry);
							// add attribute-values
							if(attributes.size()>0) {
								writeAttrValuesToGeom(featureBuilder);
							}
							feature = featureBuilder.buildFeature(null);
							simpleFeatureList.add(feature);
						}else if (child.getobjecttag().equals("MULTISURFACE")){
							if(valueCount > 1){
								throw new IoxException("max one MULTISURFACE value allowed ("+attrName+")");
							}
							int surfaceCount=child.getattrvaluecount("surface");
							type=getFeatureType(POLYGON);
							
							featureBuilder = new SimpleFeatureBuilder(type);
							try{
								// convert ili to jts
								for(int m=0;m<surfaceCount;m++) {
									IomObject iomObj=child.getattrobj("surface", m);
									Polygon jtsSurface=Iox2multijts.surface2JTS(iomObj, 0.00);
									jtsSurface.setSRID(Integer.parseInt(getSridCode()));
									featureBuilder.set(0, jtsSurface);
									// add attribute-values
									if(attributes.size()>0) {
										writeAttrValuesToGeom(featureBuilder);
									}
									feature = featureBuilder.buildFeature(null);
									simpleFeatureList.add(feature);
								}
							}catch (Iox2jtsException e) {
								throw new IoxException("failed to convert "+child.toString()+" to jts",e);
							}
						}
					}else {
						// attributes continue
					}
				}else{
					// nothing to write
				}
	    	}
    	}
    	if(simpleFeatureList.size()>0) {
    		features = new ListFeatureCollection(type, simpleFeatureList);
    	}
    	return features;
	}

    /** write attributes to feature builder if available
     * @param featureBuilder
     */
	private void writeAttrValuesToGeom(SimpleFeatureBuilder featureBuilder) {
		int indexCount=1;
		for(int h=0;h<attributes.size();h++) {
			Object currentObj=attributes.get(h);
			Property obj=(Property) currentObj;
			// set description of containing attributes
			featureBuilder.setUserData(indexCount, obj.getName(), obj.getValue());
			// set values of attributes
			featureBuilder.set(indexCount, obj.getValue());
			indexCount+=1;
		}
	}
    
	/** write created features to shape-file
	 * @param features
	 * @throws IoxException
	 */
	private void writeFeatureCollectionToShapefile(FeatureCollection<SimpleFeatureType, SimpleFeature> features) throws IoxException {
	    if (dataStore == null) {
	        throw new IoxException("datastore null");
	    }
	    SimpleFeatureType schema = features.getSchema();
	    GeometryDescriptor geom = schema
	            .getGeometryDescriptor();
	    try {
	        Transaction transaction = new DefaultTransaction(
	                "create");

	        String typeName = dataStore.getTypeNames()[0];
	        List<AttributeDescriptor> attributes = schema.getAttributeDescriptors();
	        List<AttributeDescriptor> attribs = new ArrayList<AttributeDescriptor>();
	        
	        GeometryType geomType = null;
	        for (AttributeDescriptor attributeDescriptor : attributes) {
	            AttributeType type = attributeDescriptor.getType();
	            if (type instanceof GeometryType) {
	                geomType = (GeometryType) type;

	            } else {
	                attribs.add(attributeDescriptor);
	            }
	        }
	        GeometryTypeImpl geomTypeImpl = new GeometryTypeImpl(new NameImpl(GEOM), geomType.getBinding(),geomType.getCoordinateReferenceSystem(),geomType.isIdentified(),geomType.isAbstract(),geomType.getRestrictions(), geomType.getSuper(),geomType.getDescription());
	        GeometryDescriptor geomDesc = new GeometryDescriptorImpl(geomTypeImpl, new NameImpl(GEOM),geom.getMinOccurs(), geom.getMaxOccurs(),geom.isNillable(), geom.getDefaultValue());
	        attribs.add(0, geomDesc);
	        SimpleFeatureType shpType = new SimpleFeatureTypeImpl(schema.getName(), attribs, geomDesc,schema.isAbstract(), schema.getRestrictions(),schema.getSuper(), schema.getDescription());
	        dataStore.createSchema(shpType);
	        SimpleFeatureSource featureSource = dataStore
	                .getFeatureSource(typeName);
	        if (featureSource instanceof SimpleFeatureStore) {
	            SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
	            List<SimpleFeature> allFeatures = new ArrayList<SimpleFeature>();
	            FeatureIterator<SimpleFeature> featureIter = features
	                    .features();
	            while (featureIter.hasNext()) {
	                SimpleFeature aSimpleFeature = featureIter.next();
	                SimpleFeature reType = SimpleFeatureBuilder
	                        .build(shpType, aSimpleFeature.getAttributes(), null);

	                allFeatures.add(reType);
	            }
	            featureIter.close();
	            SimpleFeatureCollection collection = new ListFeatureCollection(
	                    shpType, allFeatures);
	            featureStore.setTransaction(transaction);
	            try {
	                List<FeatureId> ids = featureStore
	                        .addFeatures(collection);
	                transaction.commit();
	            } catch (Exception e) {
	            	transaction.rollback();
	            	throw new IoxException(e);
	            } finally {
	                transaction.close();
	            }
	            // shape file store is writable
	            
	        } else {
	        	// shape file store not writable
	            
	        }
	    } catch (IOException e) {
	        throw new IoxException("no data written to shapefile",e);
	    }
	}

	@Override
    public void close() throws IoxException{
			dataStore=null;
    	if(mapping!=null){
    		mapping=null;
    	}
    	if(attrDesc!=null){
    		attrDesc=null;
    	}
    	if(attributes!=null){
    		attributes=null;
    	}
    	if(td!=null) {
    		td.clear();
    		td=null;
    	}
    }
	
	@Override
	public void flush() throws IoxException {}
	@Override
	public IomObject createIomObject(String arg0, String arg1) throws IoxException {return null;}
	@Override
	public IoxFactoryCollection getFactory() throws IoxException {return null;}
	@Override
	public void setFactory(IoxFactoryCollection arg0) throws IoxException {}

	private String getSridCode() {
		return sridCode;
	}

	public void setSridCode(String sridCode) {
		this.sridCode = sridCode;
	}

	private TransferDescription getModel() {
		return td;
	}

	public void setModel(TransferDescription td) {
		this.td = td;
	}
}