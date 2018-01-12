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
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
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
import org.opengis.feature.type.Name;
import org.opengis.filter.identity.FeatureId;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.ReferenceIdentifier;
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
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;
import ch.interlis.iox_j.wkb.Wkb2iox;
import ch.interlis.ioxwkf.dbtools.IoxWkfConfig;
import ch.interlis.iom_j.ViewableProperties;
import ch.interlis.iom_j.ViewableProperty;
import ch.interlis.iom_j.xtf.Ili2cUtility;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.generator.Iligml20Generator;
import ch.interlis.ili2c.generator.XSDGenerator;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.Element;
import ch.interlis.ili2c.metamodel.LocalAttribute;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.ili2c.metamodel.ViewableTransferElement;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**<b>ShapeWriter</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * Writing data to a ShapeFile.<br>
 * <li>If the file does not already exist, the file will be created by the defined name within the defined path.</li>
 * <li>If the file already exists, the existing file will be replaced by the new file.</li>
 * <p>
 * 
 * <b>(Optional) Setting possibilities</b><br>
 * There is only one Setting to use:<br>
 * <li>ShapeReader.ENCODING</li><br>
 * {@code ENCODING} is the name of the setting to define the text used to encode a DBF file.<br>
 * <p>
 * example: Settings settings.setValue(ShapeReader.ENCODING, "Code");
 * <p>
 * 
 * <b>(Optional) Set Model</b><br>
 * <li>If a model is set, make shore that the file content matches the class in the model. If the class can not be found, an exception will be thrown.</li>
 * <li>If no model is set, the first object will be used to create the feature type.</li>
 * <li>If no model is set, there is the possibility to use your own AttributeDescriptor. For more information see: {@code AttrDesc}.</li>
 * <p>
 * example:<br>
 * File file = new File("file.shp");<br>
 * ShapeWriter writer = new ShapeWriter(file);<br>
 * writer.setModel(td);<br>
 * <p>
 * 
 * <b>(Optional) Set Epsg/Srs</b><br>
 * The {@code srId} is an integer value that uniquely identifies the Spatial Referencing System (SRS) within the database.<br>
 * <li>If no default-SRS code is set, the SRS code of the database is adopted.</li>
 * <li>If an SRS code is set, the set SRS code is adopted.</li>
 * <p>
 * example:<br>
 * File file = new File("file.shp");<br>
 * ShapeWriter writer = new ShapeWriter(file);<br>
 * writer.setDefaultSridCode("SRS Code");<br>
 * <p>
 * 
 * <b>(Optional) AttrDesc</b><br>
 * As an alternative to setting a model, a separate AttributeDescriptor can be used.<br>
 * An AttributeDescriptor describes an attribute and shows possibilities which can be set.<br>
 * <li>If an AttributeDescriptor is set, the first object of AttributeDescriptor will be used to define the {@code createFeatureType}.</li>
 * <li>If no AttributeDescriptor is set, depends on model is set or not, see: Set Model</li>
 * <p>
 * example:<br>
 * File file = new File("file.shp");<br>
 * ShapeWriter writer = new ShapeWriter(file);<br>
 * writer.setAttributeDescriptors(AttributeDescriptor attrDescs[]);<br>
 * Definition of an AttributeDescriptor, see: {@code AttributeDescriptor}<br>
 * <p>
 * 
 * <b>Supported INTERLIS data types</b><br>
 * <table border="1">
 * <tr>
 *   <th>INTERLIS Type</th>
 *   <th>Shape Type</th> 
 *   <th>Format</th>
 * </tr>
 * <tr>
 *   <td>EnumerationType</td>
 *   <td>String</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 *  <tr>
 *   <td>EnumTreeValueType</td>
 *   <td>String</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>AlignmentType</td>
 *   <td>String</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 *  <tr>
 *   <td>BooleanType</td>
 *   <td>There are sereval ways in Shape type:<br>
 *       <li>String: ('Y' 'N'), ('T' 'F'), ('True' 'False')</li>
 *       <li>Binary Numbers: ('1' '0')</li>
 *       <li>Boolean: if dataTypeName=<br>
 *           AttributeDescriptor.DBCOLUMN_TYPENAME_BOOL: (true false)</li>
 *   </td>
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>NumericType</td>
 *   <td>Integer</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>DoubleType</td>
 *   <td>Double</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>FormattedType</td>
 *   <td>String</td>
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>CoordinateType</td>
 *   <td>Point</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>OIDType</td>
 *   <td>(OTHER) uuid</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>BlackboxType</td>
 *   <td>(OTHER) xml</td>
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>ClassType</td>
 *   <td>String</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>AttributePathType</td>
 *   <td>String</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>TextType</td>
 *   <td>String</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>xtfDate</td>
 *   <td>(SimpleDateFormat) DateFormat</td> 
 *   <td>
 * 		<li>IoxWkfConfig.SETTING_DATEFORMAT:<br>
 *          Default DateFormat for Date: yyyy-MM-dd<br>
 *          IoxWkfConfig.SETTING_DEFAULTFORMAT_DATE.
 *      </li>
 * 		<li>Own DateFormat, see link.<br>
 * 		    See Attachement DateFormatDefinition
 *      </li>
 *   </td>
 * </tr>
 * <tr>
 *   <td>xtfTime</td>
 *   <td>(SimpleDateFormat) DateFormat</td>
 *   <td>
 * 		<li>IoxWkfConfig.SETTING_TIMEFORMAT:<br>
 *          Default DateFormat for Time: HH:mm:ss<br>
 *          IoxWkfConfig.SETTING_DEFAULTFORMAT_TIME.
 *      </li>
 * 		<li>Own DateFormat, see link.<br>
 * 		    See Attachement DateFormatDefinition
 *      </li>
 *   </td>
 * </tr>
 * <tr>
 *   <td>xtfDateTime</td>
 *   <td>(SimpleDateFormat) DateFormat</td>
 *   <td>
 * 		<li>IoxWkfConfig.SETTING_TIMESTAMPFORMAT:<br>
 *          Default DateFormat for TimeStamp:<br>
 *          yyyy-MM-dd'T'HH:mm:ss.SSS<br>
 *          IoxWkfConfig.SETTING_DEFAULTFORMAT_TIMESTAMP.
 *      </li>
 * 		<li>Own DateFormat, see link.<br>
 * 		    See Attachement DateFormatDefinition
 *      </li>
 *   </td>
 * </tr>
 * <tr>
 *   <td>Coord</td>
 *   <td>Point</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>MultiCoord</td>
 *   <td>MultiPoint</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>Surface</td>
 *   <td>Polygon</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>MultiSurface</td>
 *   <td>MultiPolygon</td> 
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>Polyline</td>
 *   <td>LineString</td> 
 *    <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * <tr>
 *   <td>MultiPolyline</td>
 *   <td>MultiLineString</td>
 *   <td>See attachement: Interlisspecification</a></td>
 * </tr>
 * </table>
 * <p>
 * 
 * <b>Not Supported INTERLIS data types</b><br>
 * <li>StructureType</li>
 * <li>ReferenceType</li>
 * <li>AssociationType</li>
 * <p>
 * 
 * <b>Attachement</b><br>
 * <li><a href="https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf">Shapespecification</a></li>
 * <li><a href="https://www.ech.ch/vechweb/page?p=dossier&documentNumber=eCH-0031&documentVersion=2.0">Interlisspecification</a></li>
 * <li><a href="https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html">DateFormatDefinition</a></li>
 */
public class ShapeWriter implements ch.interlis.iox.IoxWriter {
	
	private static final String CRS_CODESPACE_EPSG = "EPSG";
	private DataStore dataStore=null; // --> data access
    private List<AttributeDescriptor> attrDescs=null; // --> attribute type data
	private SimpleFeatureType featureType=null;
	private SimpleFeatureBuilder featureBuilder=null;
	private SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd"); // XTF format
	
	private static final String POINT="pointProperty";
	// geometry type properties
	private static final String MULTIPOINT="multipointProperty";
	private static final String LINESTRING="lineProperty";
	private static final String MULTILINESTRING="multilineProperty";
	private static final String POLYGON="polygonProperty";
	private static final String MULTIPOLYGON="multipolygonProperty";
	// ili types
	private static final String COORD="COORD";
	private static final String MULTICOORD="MULTICOORD";
	private static final String POLYLINE="POLYLINE";
	private static final String MULTIPOLYLINE="MULTIPOLYLINE";
	private static final String MULTISURFACE="MULTISURFACE";

	private Integer srsId=null;
	private Integer defaultSrsId;
	// model
	private TransferDescription td=null;
	private String iliGeomAttrName=null;
	private Name featureTypeName=new NameImpl("http://www.geotools.org/","shpType");
	private SimpleFeatureStore featureStore=null;
	private Transaction transaction=null;
	
	/** initialize shape writer.
	 * @param file
	 * @throws IoxException
	 */
    public ShapeWriter(java.io.File file) throws IoxException {
    	this(file,null);
    }
    
    /** initialize shape writer with settings.
     * @param file
     * @param settings
     * There is only one Setting to use:<br>
     * <li>ShapeReader.ENCODING</li><br>
     * ENCODING is the name of the setting to define the text used to encode a DBF file.<br>
     * example: Settings settings.setValue(ShapeReader.ENCODING, "Code");
     *
     * @throws IoxException
     */
    public ShapeWriter(java.io.File file,Settings settings) throws IoxException { 
		init(file,settings);
    }
    
	private void init(File file,Settings settings) throws IoxException{
        Map<String, Serializable> params = new HashMap<String, Serializable>();
        // get file path
        try {
			params.put(org.geotools.data.shapefile.ShapefileDataStoreFactory.URLP.key, file.toURL());
			String encoding=settings!=null?settings.getValue(ShapeReader.ENCODING):null;
			if(encoding!=null) {
				params.put(org.geotools.data.shapefile.ShapefileDataStoreFactory.DBFCHARSET.key, encoding);
			}
		} catch (MalformedURLException e2) {
			throw new IoxException(e2);
		}
        // create data store
        try {
            ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
            dataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
		} catch (IOException e2) {
			throw new IoxException(e2);
		}
        // set access to file
		file.setExecutable(true);
		file.setWritable(true);
	}
    
	/** write content in IomObjects in IoxEvents to shape file.
	 * @param event
	 * @exception IoxException
	 */
    @Override
	public void write(IoxEvent event) throws IoxException {
		if(event instanceof StartTransferEvent){
			// ignore
		}else if(event instanceof StartBasketEvent){
		}else if(event instanceof ObjectEvent){
			ObjectEvent obj=(ObjectEvent) event;
			IomObject iomObj=(IomObject)obj.getIomObject();
			String tag = iomObj.getobjecttag();
			// check if class exist in model/models
			if(attrDescs==null) {
				attrDescs=new ArrayList<AttributeDescriptor>();
				if(td!=null) {
					Viewable aclass=(Viewable) XSDGenerator.getTagMap(td).get(tag);
					if (aclass==null){
	            		throw new IoxException("class "+iomObj.getobjecttag()+" not found in model");
					}
					Iterator viewableIter=aclass.getAttributes();
					while(viewableIter.hasNext()) {
						Object attrObj=viewableIter.next();
						if(attrObj instanceof LocalAttribute) {
							LocalAttribute localAttr= (LocalAttribute)attrObj;
							String attrName=localAttr.getName();
	    					//create the builder
	    					AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
	    					ch.interlis.ili2c.metamodel.Type iliType=localAttr.getDomainResolvingAliases();
	    					if(iliType instanceof ch.interlis.ili2c.metamodel.CoordType) {
	    						iliGeomAttrName=attrName;
		    					attributeBuilder.setBinding(Point.class);
    							if(defaultSrsId!=null) {
    								attributeBuilder.setCRS(createCrs(defaultSrsId));
    							}
	    					}else if(iliType instanceof ch.interlis.ili2c.metamodel.PolylineType) {
	    						iliGeomAttrName=attrName;
		    					attributeBuilder.setBinding(LineString.class);
    							if(defaultSrsId!=null) {
    								attributeBuilder.setCRS(createCrs(defaultSrsId));
    							}
	    					}else if(iliType instanceof ch.interlis.ili2c.metamodel.SurfaceOrAreaType) {
	    						iliGeomAttrName=attrName;
		    					attributeBuilder.setBinding(Polygon.class);
    							if(defaultSrsId!=null) {
    								attributeBuilder.setCRS(createCrs(defaultSrsId));
    							}
	    					}else {
		    					attributeBuilder.setBinding(String.class);
	    					}
	    					attributeBuilder.setName(attrName);
	    					attributeBuilder.setMinOccurs(0);
	    					attributeBuilder.setMaxOccurs(1);
	    					attributeBuilder.setNillable(true);
	    					//build the descriptor
	    					AttributeDescriptor descriptor = attributeBuilder.buildDescriptor(attrName);
	    					// add descriptor to descriptor list
	    					attrDescs.add(descriptor);
						}
					}
	            }else {
            		for(int u=0;u<iomObj.getattrcount();u++) {
            			String attrName=iomObj.getattrname(u);
    					//create the builder
    					AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
    					if(attrName.equals(iliGeomAttrName)) {
    						iliGeomAttrName=attrName;
    						IomObject iomGeom=iomObj.getattrobj(attrName,0);
    						if (iomGeom != null){
    							if (iomGeom.getobjecttag().equals(COORD)){
    	        					attributeBuilder.setBinding(Point.class);
    							}else if (iomGeom.getobjecttag().equals(MULTICOORD)){
    	        					attributeBuilder.setBinding(MultiPoint.class);
    							}else if(iomGeom.getobjecttag().equals(POLYLINE)){
    	        					attributeBuilder.setBinding(LineString.class);
    							}else if (iomGeom.getobjecttag().equals(MULTIPOLYLINE)){
    	        					attributeBuilder.setBinding(MultiLineString.class);
    							}else if (iomGeom.getobjecttag().equals(MULTISURFACE)){
    								int surfaceCount=iomGeom.getattrvaluecount("surface");
    								if(surfaceCount<=1) {
    		        					attributeBuilder.setBinding(Polygon.class);
    								}else if(surfaceCount>1){
    		        					attributeBuilder.setBinding(MultiPolygon.class);
    								}
    							}else {
    	        					attributeBuilder.setBinding(Point.class);
    							}
    							if(defaultSrsId!=null) {
    								attributeBuilder.setCRS(createCrs(defaultSrsId));
    							}
    						}
    					}else {
    						if(iliGeomAttrName==null && iomObj.getattrvaluecount(attrName)>0 && iomObj.getattrobj(attrName,0)!=null) {
        						iliGeomAttrName=attrName;
        						IomObject iomGeom=iomObj.getattrobj(attrName,0);
        						if (iomGeom != null){
        							if (iomGeom.getobjecttag().equals(COORD)){
        	        					attributeBuilder.setBinding(Point.class);
        							}else if (iomGeom.getobjecttag().equals(MULTICOORD)){
        	        					attributeBuilder.setBinding(MultiPoint.class);
        							}else if(iomGeom.getobjecttag().equals(POLYLINE)){
        	        					attributeBuilder.setBinding(LineString.class);
        							}else if (iomGeom.getobjecttag().equals(MULTIPOLYLINE)){
        	        					attributeBuilder.setBinding(MultiLineString.class);
        							}else if (iomGeom.getobjecttag().equals(MULTISURFACE)){
        								int surfaceCount=iomGeom.getattrvaluecount("surface");
        								if(surfaceCount==1) {
        		        					attributeBuilder.setBinding(Polygon.class);
        								}else if(surfaceCount>1){
        		        					attributeBuilder.setBinding(MultiPolygon.class);
        								}
        							}else {
        	        					attributeBuilder.setBinding(Point.class);
        							}
        							if(defaultSrsId!=null) {
        								attributeBuilder.setCRS(createCrs(defaultSrsId));
        							}
        						}
    						}else {
            					attributeBuilder.setBinding(String.class);
    						}
    					}
    					attributeBuilder.setName(attrName);
    					attributeBuilder.setMinOccurs(0);
    					attributeBuilder.setMaxOccurs(1);
    					attributeBuilder.setNillable(true);
    					//build the descriptor
    					AttributeDescriptor descriptor = attributeBuilder.buildDescriptor(attrName);
    					// add descriptor to descriptor list
    					attrDescs.add(descriptor);
            		}
	            }
			}
			if(featureType==null) {
				featureType=createFeatureType(attrDescs);
				featureBuilder = new SimpleFeatureBuilder(featureType);
		        try {
					dataStore.createSchema(featureType);
					
					String typeName = dataStore.getTypeNames()[0];
					featureStore = (SimpleFeatureStore) dataStore
					        .getFeatureSource(typeName);
					
					transaction = new DefaultTransaction(
					        "create");

					featureStore.setTransaction(transaction);
				} catch (IOException e) {
			        throw new IoxException(e);
				}
			}
        	// write object attribute-values of model attribute-names
        	try {
        		SimpleFeature feature=convertObject(iomObj);
    			writeFeatureToShapefile(feature);
			} catch (IOException e) {
				throw new IoxException("failed to write object "+iomObj.getobjecttag(),e);
			} catch (Iox2jtsException e) {
				throw new IoxException("failed to convert "+iomObj.getobjecttag()+" in jts",e);
			}
		}else if(event instanceof EndBasketEvent){
			// ignore
		}else if(event instanceof EndTransferEvent){
			if(featureStore==null) {
				// write dummy file
		        SimpleFeatureType featureType=null;
				if(attrDescs!=null) {
					featureType=createFeatureType(attrDescs);
				}else {
			        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
			        builder.setName(featureTypeName);
			        builder.add(ShapeReader.GEOTOOLS_THE_GEOM,Point.class);
			        if(defaultSrsId!=null) {
			        	builder.setCRS(createCrs(defaultSrsId));
			        }
			        featureType=builder.buildFeatureType();
				}
				try {
					dataStore.createSchema(featureType);
				} catch (IOException e) {
			        throw new IoxException(e);
				}
			}
			if(transaction!=null) {
	            try {
					transaction.commit();
		            transaction.close();
		            transaction=null;
				} catch (IOException e) {
			        throw new IoxException(e);
				}
			}
		}
	}
	
	private CoordinateReferenceSystem createCrs(int srsId) throws IoxException {
		CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
		try {
			return factory.createCoordinateReferenceSystem("EPSG:"+srsId);
		} catch (NoSuchAuthorityCodeException e) {
			throw new IoxException("coordinate reference: EPSG:"+srsId+" not found",e);
		} catch (FactoryException e) {
			throw new IoxException(e);
		}
		
	}
	private SimpleFeatureType createFeatureType(List<AttributeDescriptor> attrDescs) throws IoxException {
		//create the builder
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(featureTypeName);
		boolean hasGeometry=false;
        for(AttributeDescriptor attrDesc:attrDescs) {
        	if(attrDesc.getLocalName().equals(iliGeomAttrName)) {
				AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
				attributeBuilder.init(attrDesc);
				attributeBuilder.setName(ShapeReader.GEOTOOLS_THE_GEOM);
				builder.add(attributeBuilder.buildDescriptor(ShapeReader.GEOTOOLS_THE_GEOM));
	            builder.setDefaultGeometry(ShapeReader.GEOTOOLS_THE_GEOM);
	    		CoordinateReferenceSystem crs =((GeometryType)attrDesc.getType()).getCoordinateReferenceSystem();
	    		if(crs!=null) {
	    	        builder.setCRS(crs);
	    	        srsId=getEPSGCode(crs);
	    		}
	    		hasGeometry=true;
        	}else {
        		builder.add(attrDesc);
        	}
        }
        if(!hasGeometry) {
	        builder.add(ShapeReader.GEOTOOLS_THE_GEOM,Point.class);
	        if(defaultSrsId!=null) {
	        	builder.setCRS(createCrs(defaultSrsId));
	        }
        }
		// build type
		SimpleFeatureType simpleFeatureType=builder.buildFeatureType();
		return simpleFeatureType;
	}
    
	private Integer getEPSGCode(CoordinateReferenceSystem crs) {
		for(ReferenceIdentifier id:crs.getIdentifiers()) {
			if(CRS_CODESPACE_EPSG.equals(id.getCodeSpace())) {
				return Integer.parseInt(id.getCode());
			}
		}
		return null;
	}
	
    private SimpleFeature convertObject(IomObject obj) throws IoxException, IOException, Iox2jtsException {
    	for (int i = 0; i < attrDescs.size(); i++){
	    	GeometryFactory geometryFactory=new GeometryFactory();
	    	String attrName=attrDescs.get(i).getLocalName();
	    	if(!attrName.equals(iliGeomAttrName)) {
				String val=obj.getattrprim(attrName,0);
				if(val!=null){
					if(attrDescs.get(i).getType().getBinding().equals(java.util.Date.class)){
						try {
							// match attrValue to format.
							java.util.Date utilDate=dateFormat.parse(val);
							featureBuilder.set(attrName, utilDate);
						} catch (ParseException e) {
							throw new IoxException(val+" does not match format: "+dateFormat.toPattern());
						}
					}else {
						featureBuilder.set(attrName, val);
					}
				}
	    	}else {
	    		int iomValueCount=obj.getattrvaluecount(iliGeomAttrName);
				IomObject iomGeom=obj.getattrobj(iliGeomAttrName,0);
				if (iomGeom != null){
					if (iomGeom.getobjecttag().equals(COORD)){
						// COORD
						Coordinate jtsCoord=null;
						try {
							// convert ili to jts
							jtsCoord=ch.interlis.iox_j.jts.Iox2jts.coord2JTS(iomGeom);
						} catch (Iox2jtsException e) {
							throw new IoxException("failed to convert "+iomGeom.getobjecttag()+" to jts",e);
						}
						if(iomValueCount > 1){
							throw new IoxException("max one COORD value allowed ("+attrName+")");
						}
						Geometry geometry=(geometryFactory.createPoint(jtsCoord));
						if(srsId!=null) {
							geometry.setSRID(srsId);
						}
						featureBuilder.set(ShapeReader.GEOTOOLS_THE_GEOM, geometry);
					}else if (iomGeom.getobjecttag().equals(MULTICOORD)){
						try {
							Geometry geometry = Iox2jts.multicoord2JTS(iomGeom);
							featureBuilder.set(ShapeReader.GEOTOOLS_THE_GEOM, geometry);
						}catch(Exception e) {
							throw new IoxException("failed to convert "+iomGeom.getobjecttag()+" to jts",e);
						}
					}else if(iomGeom.getobjecttag().equals(POLYLINE)){
						// POLYLINE
						CoordinateList jtsLineString=null;
						try{
							jtsLineString=ch.interlis.iox_j.jts.Iox2jts.polyline2JTS(iomGeom, true, 0.0);
						}catch (Iox2jtsException e){
							throw new IoxException("failed to convert "+iomGeom.getobjecttag()+" to jts",e);
						}
						if(iomValueCount > 1){
							throw new IoxException("max one POLYLINE value allowed ("+attrName+")");
						}
						// convert list to array
						Coordinate[] coordArray = new Coordinate[jtsLineString.size()];
						coordArray = (Coordinate[]) jtsLineString.toArray(coordArray);
						// convert ili to jts
						Geometry geometry=(geometryFactory.createLineString(coordArray));
						if(srsId!=null) {
							geometry.setSRID(srsId);
						}
						
						featureBuilder.set(ShapeReader.GEOTOOLS_THE_GEOM, geometry);
					}else if (iomGeom.getobjecttag().equals(MULTIPOLYLINE)){
						// MULTIPOLYLINE
						try {
							Geometry geometry = Iox2jts.multipolyline2JTS(iomGeom, 0.0);
							featureBuilder.set(ShapeReader.GEOTOOLS_THE_GEOM, geometry);
						}catch(Exception e) {
							throw new IoxException("failed to convert "+iomGeom.getobjecttag()+" to jts",e);
						}
					}else if (iomGeom.getobjecttag().equals(MULTISURFACE)){
						if(iomValueCount > 1){
							throw new IoxException("max one MULTISURFACE value allowed ("+attrName+")");
						}
						int surfaceCount=iomGeom.getattrvaluecount("surface");
						if(surfaceCount==1) {
							try {
								Polygon jtsSurface=Iox2jts.surface2JTS(iomGeom, 0.00);
								if(srsId!=null) {
									jtsSurface.setSRID(srsId);
								}
								featureBuilder.set(ShapeReader.GEOTOOLS_THE_GEOM, jtsSurface);
							}catch (Iox2jtsException e) {
								throw new IoxException("failed to convert "+iomGeom.getobjecttag()+" to jts",e);
							}
						}else if(surfaceCount>1){
							// MULTIPOLYGON
							try {
								Geometry geometry = Iox2jts.multisurface2JTS(iomGeom, 0,0); 
								if(srsId!=null) {
									geometry.setSRID(srsId);
								}
								featureBuilder.set(ShapeReader.GEOTOOLS_THE_GEOM, geometry);
							}catch(Exception e) {
								throw new IoxException("failed to convert "+iomGeom.getobjecttag()+" to jts",e);
							}
						}
					}else {
						throw new IoxException("unexpected geometry type "+iomGeom.getobjecttag());
					}
				}else {
					featureBuilder.set(ShapeReader.GEOTOOLS_THE_GEOM, null);
				}
	    		
	    	}
    	}
    	SimpleFeature feature=featureBuilder.buildFeature(null);
    	return feature;
	}
    
	/** 
	 * Add feature to ListFeatureCollection.<br>
	 * Add all features inside ListFeatureCollection to the SimpleFeatureStore, alias featureStore.
	 * @param features
	 * @throws IoxException
	 */
	private void writeFeatureToShapefile(SimpleFeature feature) throws IoxException {
	    if (dataStore == null) {
	        throw new IoxException("datastore null");
	    }
		ListFeatureCollection features = new ListFeatureCollection(featureType);
		features.add(feature);
	    try {
	        featureStore.addFeatures(features);
	    } catch (IOException e) {
	        throw new IoxException("no data written to shapefile",e);
	    }
	}

	/** Close all open objects.
	 * @exception IoxException
	 */
	@Override
    public void close() throws IoxException
	{
		if(transaction!=null) {
			try {
				transaction.rollback();
				transaction.close();
				transaction=null;
			} catch (IOException e) {
				throw new IoxException(e);
			}
		}
		if(dataStore!=null) {
			dataStore.dispose();
			dataStore=null;
		}
    }
    
	/** flush.
	 * @exception IoxException
	 */
	@Override
	public void flush() throws IoxException
	{
	}
	
	/** create IomObject.
	 * @param arg0
	 * @param arg1
	 * @exception IoxException
	 */
	@Override
	public IomObject createIomObject(String arg0, String arg1) throws IoxException 
	{
		return null;
	}
	
	/** get iox factory collection
	 * @return IoxFactoryCollection
	 * @exception IoxException
	 */
	@Override
	public IoxFactoryCollection getFactory() throws IoxException 
	{
		return null;
	}
	
	/** set iox factory collection.
	 * @param ar0
	 * @exception IoxException
	 */
	@Override
	public void setFactory(IoxFactoryCollection arg0) throws IoxException 
	{	
	}
	
	/** The default srid code.
	 * The SRS is an integer value that uniquely identifies the Spatial Referencing System (SRS) within the database.<br>
	 * <li>If no default-SRS code is set, the SRS code of the database is adopted.</li>
	 * <li>If an SRS code is set, the set SRS code is adopted.</li>
	 * example:<br>
	 * File file = new File("file.shp");<br>
	 * ShapeWriter writer = new ShapeWriter(file);<br>
	 * writer.setDefaultSridCode("SRS Code");<br>
	 * @param sridCode
	 */
	public void setDefaultSridCode(String sridCode) {
		defaultSrsId = Integer.parseInt(sridCode);
	}
	
	/** The model.
	 * <li>If a model is set, the Shapefile will match the class in the model. If the class can not be found, an error message will be displayed. It is important to make sure the Shapefile matches the set model.</li>
	 * <li>If no model is set, the first object will be used.</li>
	 * <li>If no model is set, there is the possibility to use own AttributeDescriptor. For more information see: AttrDesc.</li>
	 * example:<br>
	 * writer.getModel();<br>
	 * @param td
	 */
	private TransferDescription getModel() {
		return td;
	}
	
	/** The model.
	 * <li>If a model is set, the Shapefile will match the class in the model. If the class can not be found, an error message will be displayed. It is important to make sure the Shapefile matches the set model.</li>
	 * <li>If no model is set, the first object will be used.</li>
	 * <li>If no model is set, there is the possibility to use own AttributeDescriptor. For more information see: AttrDesc.</li>
	 * example:<br>
	 * File file = new File("file.shp");<br>
	 * ShapeWriter writer = new ShapeWriter(file);<br>
	 * writer.setModel(td);<br>
	 * @param td
	 */
	public void setModel(TransferDescription td) {
		this.td = td;
	}
	
	/** Get AttributeDescriptor.
	 * As an alternative to setting a model, a separate AttributeDescriptor can be used.<br>
	 * An AttributeDescriptor describes an attribute and shows possibilities which can be set.<br>
	 * <li>If an AttributeDescriptor is set, AttributeDescriptor Objects will be used.</li>
	 * <li>If no AttributeDescriptor is set, depends on model is set or not, see: Set Model</li>
	 * example:<br>
	 * writer.getAttributeDescriptors();<br>
	 * Definition of an AttributeDescriptor, see: #AttributeDescriptor<br>
	 * @return AttributeDescriptor[]
	 */
	public AttributeDescriptor[] getAttributeDescriptors() {
		return attrDescs.toArray(new AttributeDescriptor[attrDescs.size()]);
	}
	
	/** Set the AttributeDescriptor
	 * As an alternative to setting a model, a separate AttributeDescriptor can be used.<br>
	 * An AttributeDescriptor describes an attribute and shows possibilities which can be set.<br>
	 * <li>If an AttributeDescriptor is set, AttributeDescriptor Objects will be used.</li>
	 * <li>If no AttributeDescriptor is set, depends on model is set or not, see: Set Model</li>
	 * example:<br>
	 * File file = new File("file.shp");<br>
	 * ShapeWriter writer = new ShapeWriter(file);<br>
	 * writer.setAttributeDescriptors(AttributeDescriptor attrDescs[]);<br>
	 * Definition of an AttributeDescriptor, see: #AttributeDescriptor<br>
	 * @param attrDescs[]
	 */
	public void setAttributeDescriptors(AttributeDescriptor attrDescs[]) {
		this.attrDescs = new ArrayList<AttributeDescriptor>();
		for(AttributeDescriptor attrDesc:attrDescs) {
			if(attrDesc.getType() instanceof GeometryType) {
				iliGeomAttrName=attrDesc.getLocalName();
			}
			this.attrDescs.add(attrDesc);
		}
	}
}