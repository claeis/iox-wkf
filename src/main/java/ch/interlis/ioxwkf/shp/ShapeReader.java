package ch.interlis.ioxwkf.shp;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.type.AttributeTypeImpl;
import org.geotools.feature.type.BasicFeatureTypes;
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
import com.vividsolutions.jts.geom.PrecisionModel;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.metamodel.DataModel;
import ch.interlis.ili2c.metamodel.Element;
import ch.interlis.ili2c.metamodel.LocalAttribute;
import ch.interlis.ili2c.metamodel.Model;
import ch.interlis.ili2c.metamodel.PredefinedModel;
import ch.interlis.ili2c.metamodel.Table;
import ch.interlis.ili2c.metamodel.Topic;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.iom.IomObject;
import ch.interlis.iox_j.jts.Jts2iox;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxReader;

/**<b>ShapeReader</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * Reading data from a ShapeFile.<br>
 * <li>If the file to read from not found, a exception will be thrown.</li>
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
 * <li>If no model is set, the first object will be used to create the IomObject.</li>
 * <p>
 * example:<br>
 * File file = new File("file.shp");<br>
 * ShapeReader reader = new ShapeReader(file);<br>
 * reader.setModel(td);<br>
 * <p>
 * 
 * <b>Geometry Attributenames</b><br>
 * Important to know is that all the geometry names are equal. They call: 'the_geom'.<br>
 * If a model is set, the geometry names are made from the model attribute names.
 * <p>
 * 
 * <b>IomObject</b><br>
 * IomObject iomObj=createIomObject("{@code type}", "{@code oid}");<br>
 * Define param:<br>
 * <br>
 * if model is set:
 * <li>type=="modelname.topicname.classname"</li>
 * <li>oid==Start counting by 1, go on adding 1 to existing count. model, basket and class id's are unique.</li>
 * <p>
 * if no model set:
 * <li>The default modelname is: 'The name of the shapefile'</li>
 * <li>The default topicname is: 'Topic(1,2,3,...)'</li>
 * <li>The default classname is: 'Class(1,2,3,...)'</li>
 * <li>type=="modelname.topicname.classname"</li>
 * <li>oid==Start counting by 1, go on adding 1 to existing count. model, basket and class id's are unique.</li>
 * <p>
 * 
 * <b>Supported INTERLIS data types</b><br>
 * <table border="1">
 * <tr>
 *   <th>Shape Type</th> 
 *   <th>INTERLIS Type</th>
 *   <th>Format</th>
 * </tr>
 * <tr>
 *   <td>String</td> 
 *   <td>EnumerationType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 *  <tr>
 *   <td>String</td> 
 *   <td>EnumTreeValueType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>String</td> 
 *   <td>AlignmentType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>Boolean</td>
 *   <td>BooleanType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>Double</td>
 *   <td>DoubleType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>Integer</td> 
 *   <td>NumericType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>String</td>
 *   <td>FormattedType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>Point</td> 
 *   <td>CoordinateType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>(OTHER) uuid</td> 
 *   <td>OIDType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>(OTHER) xml</td>
 *   <td>BlackboxType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>String</td> 
 *   <td>ClassType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>String</td> 
 *   <td>AttributePathType</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>(SimpleDateFormat) DateFormat</td>
 *   <td>java.util.Date xtfDate</td>
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
 *   <td>(SimpleDateFormat) DateFormat</td>
 *   <td>java.util.Date xtfTime</td>
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
 *   <td>(SimpleDateFormat) DateFormat</td>
 *   <td>java.util.Date xtfDateTime</td>
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
 *   <td>Point</td> 
 *   <td>Coord</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>MultiPoint</td> 
 *   <td>MultiCoord</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>Polygon</td> 
 *   <td>Surface</td>
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>MultiPolygon</td>
 *   <td>MultiSurface</td> 
 *   <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>LineString</td> 
 *   <td>Polyline</td>
 *    <td>See attachement: Shapespecification</a></td>
 * </tr>
 * <tr>
 *   <td>MultiLineString</td>
 *   <td>MultiPolyline</td>
 *   <td>See attachement: Shapespecification</a></td>
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
public class ShapeReader implements IoxReader{
	
	/** the_geom is used for the geometry attribute name of geotools.
     */
	public static final String GEOTOOLS_THE_GEOM = BasicFeatureTypes.GEOMETRY_ATTRIBUTE_NAME;
	
	/** the name of the setting to define the text which is used to encode a DBF file.
	 */
	public static final String ENCODING = "ch.interlis.ioxwkf.shp.encoding";
	
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
	private SimpleFeature pendingShapeObj=null;
	private ShapefileDataStore dataStore=null;
	
	// iox
	private TransferDescription td;
	private IoxFactoryCollection factory=new ch.interlis.iox_j.DefaultIoxFactoryCollection();
	private java.io.File inputFile=null;
	private int nextId=1;
	
	// model, topic, class
	private String topicIliQName="Topic";
	private String classIliQName=null;
	
	// attributes, as read from the underlying geotools library
	private List<AttributeDescriptor> shapeAttributes=null;
	
	// attributes, as returned from this reader (as values of IomObjects).
	// List is in the same order as shapeAttributes, but case of attribute name might be different.
	// And name of geometry attribute might be different.
	private List<String> iliAttributes=null;
	
	// Name of the geometry attribute in the IomObject
	private String theGeomAttr=GEOTOOLS_THE_GEOM;

	private SimpleDateFormat xtfDate=new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat xtfDateTime=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	/** Creates a new shape reader.
	 * @param shpFile to read from
	 * @throws IoxException
	 */
	public ShapeReader(java.io.File shpFile) throws IoxException{
		this(shpFile,null);
	}
	
	/** Creates a new shape reader, which contains settings.<br>
	 * There is only one Setting to use:<br>
     * <li>ShapeReader.ENCODING</li><br>
     * ENCODING is the name of the setting to define the text used to encode a DBF file.<br>
     * example: Settings settings.setValue(ShapeReader.ENCODING, "Code");
     * <p>
	 * @param shpFile to read from
	 * @param settings
	 * @throws IoxException
	 */
	public ShapeReader(java.io.File shpFile,Settings settings) throws IoxException{
		state=START;
		td=null;
		inputFile=shpFile;
		init(inputFile,settings);
	}
	
	/** initialize file content.
	 * @param shapeFile
	 * @param settings
	 * @throws IoxException
	 */
	private void init(java.io.File shapeFile,Settings settings) throws IoxException{
		factory=new ch.interlis.iox_j.DefaultIoxFactoryCollection();
		try {
	        Map<String, Serializable> map = new HashMap<String, Serializable>();
	        // get file path
	        try {
				map.put(org.geotools.data.shapefile.ShapefileDataStoreFactory.URLP.key, shapeFile.toURL());
				String encoding=settings!=null?settings.getValue(ShapeReader.ENCODING):null;
				if(encoding!=null) {
					map.put(org.geotools.data.shapefile.ShapefileDataStoreFactory.DBFCHARSET.key, encoding);
				}
			} catch (MalformedURLException e2) {
				throw new IoxException(e2);
			}
			
			dataStore = (ShapefileDataStore) DataStoreFinder.getDataStore(map);
			if(dataStore==null) {
				throw new IoxException("expected shape file");
			}
		}catch(IOException e) {
			throw new IoxException(e);
		}
	}
	
	/** The model.
	 * <li>If a model is set, the Shapefile will match the class in the model. If the class can not be found, an error message will be displayed. It is important to make sure the Shapefile matches the set model.</li>
	 * <li>If no model is set, the first object will be used.</li>
	 * example:<br>
	 * File file = new File("file.shp");<br>
	 * ShapeReader reader = new ShapeReader(file);<br>
	 * reader.setModel(td);<br>
	 * @param td
	 */
	public void setModel(TransferDescription td){
		this.td=td;
	}

	/** read the path of input shape file and return the single name of shape file.
	 * @return file path to read from.
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
	
	/** read file elements in simple feature file.
	 * @return IoxEvent
	 * @exception IoxException
	 */
	@Override
    public IoxEvent read() throws IoxException{
		IomObject iomObj = null;		
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
			try {
				featureCollectionIter=dataStore.getFeatureSource().getFeatures().features();
				if(featureCollectionIter.hasNext()) {
					// feature object
					pendingShapeObj=(SimpleFeature) featureCollectionIter.next();	
					//simple feature type
					SimpleFeatureType featureType=pendingShapeObj.getFeatureType();
					shapeAttributes = featureType.getAttributeDescriptors();
					featureTypeName=featureType.getName().getLocalPart();
				}
			} catch (IOException e) {
				throw new IoxException(e);
			}
			if(td!=null) {
				iliAttributes=new ArrayList<String>();
				Viewable viewable=getViewableByShapeAttributes(shapeAttributes, iliAttributes);
				if(viewable==null){
					throw new IoxException("attributes '"+getNameList(shapeAttributes)+"' not found in model: '"+td.getLastModel().getName()+"'.");
				}
				// get model data
				topicIliQName=viewable.getContainer().getScopedName();
				classIliQName=viewable.getScopedName();
			}else {
				topicIliQName=getNameOfDataFile()+".Topic";
				classIliQName=topicIliQName+".Class"+getNextId();
				iliAttributes=new ArrayList<String>();
				for(AttributeDescriptor shapeAttribute:shapeAttributes) {
					iliAttributes.add(shapeAttribute.getLocalName());
				}
			}
			String bid="b"+getNextId();
			state=INSIDE_OBJECT;
			return new ch.interlis.iox_j.StartBasketEvent(topicIliQName, bid);
		}
		if(state==INSIDE_OBJECT){
			SimpleFeature shapeObj=null;
			if(pendingShapeObj!=null) {
				shapeObj=pendingShapeObj;
				pendingShapeObj=null;
			}else if(featureCollectionIter.hasNext()) {
				shapeObj=(SimpleFeature) featureCollectionIter.next();	
			}
			if(shapeObj!=null) {
				// feature object
				iomObj=createIomObject(classIliQName, null);
				boolean foundAttrInModel=false;
				int attrc=shapeAttributes.size();
	        	for(int attri=0;attri<attrc;attri++) {
	        		AttributeDescriptor shapeAttribute=shapeAttributes.get(attri);
	        		IomObject subIomObj=null;
	        		// attribute name
	        		String shapeAttrName=shapeAttribute.getLocalName();
	        		String iliAttrName=iliAttributes.get(attri);
	        		// attribute type
	        		AttributeType shapeAttrType=shapeAttribute.getType();
	        		Object shapeAttrValue=shapeObj.getAttribute(shapeAttrName);
	        		if(shapeAttrValue!=null) {
		        		if(shapeAttrValue instanceof MultiLineString) {
	    					// multiLineString
	        				MultiLineString multiLineString=(MultiLineString)shapeAttrValue;
	        				// contains number of linestrings.
	        				Coordinate[] coords=multiLineString.getCoordinates();
	        				PrecisionModel precisionModel=multiLineString.getPrecisionModel();
	        				Integer srid=multiLineString.getSRID();
	        				if(multiLineString.getNumGeometries()==1) {
			        			// lineString
	        					try {
	        						if(coords!=null && precisionModel!=null && srid!=null) {
	        							// create lineString
	        							LineString lineString=new LineString(coords, precisionModel, srid);
	        							if(lineString!=null) {
	        								// convert to iox polyline
	        								subIomObj=Jts2iox.JTS2polyline(lineString);
	        							}
	        						}
	        					} catch (Exception e) {
									throw new IoxException(e);
								}
	    					}else {
	    						// multiLineString
		        				try {
	    							subIomObj=Jts2iox.JTS2multipolyline(multiLineString);
								} catch (Exception e){
									throw new IoxException(e);
								}
	        				}
	        				iomObj.addattrobj(iliAttrName, subIomObj);
	    				}else if(shapeAttrValue instanceof MultiPoint) {
	    					// multiPoint
	        				MultiPoint multiPoint=(MultiPoint)shapeAttrValue;
	        				try {
	        					Coordinate[] coords=multiPoint.getCoordinates();
								subIomObj=Jts2iox.JTS2multicoord(coords);
								iomObj.addattrobj(iliAttrName, subIomObj);
							} catch (Exception e){
								throw new IoxException(e);
							}
	    				}else if(shapeAttrValue instanceof MultiPolygon) {
	        				// multiPolygon
	        				MultiPolygon multiPolygon=(MultiPolygon)shapeAttrValue;
	        				if(multiPolygon.getNumGeometries()==1) {
	        					try {
		        					Polygon polygonObj=(Polygon)multiPolygon.getGeometryN(0);
									subIomObj=Jts2iox.JTS2surface(polygonObj);
			        				iomObj.addattrobj(iliAttrName, subIomObj);
	        					} catch (Exception e){
									throw new IoxException(e);
								}
	        				}else {
		        				try {
	    							subIomObj=Jts2iox.JTS2multisurface(multiPolygon);
	    							iomObj.addattrobj(iliAttrName, subIomObj);
								} catch (Exception e){
									throw new IoxException(e);
								}
	        				}
	        			}else if(shapeAttrValue instanceof Point) {
	        				// point
	        				Point pointObj=(Point)shapeAttrValue;
	        				Coordinate coord=pointObj.getCoordinate();
							subIomObj=Jts2iox.JTS2coord(coord);
							iomObj.addattrobj(iliAttrName, subIomObj);
	        			}else if(shapeAttrValue instanceof Polygon) {
	        				// polygon
	        				Polygon polygonObj=(Polygon)shapeAttrValue;
							subIomObj=Jts2iox.JTS2surface(polygonObj);
	        				iomObj.addattrobj(iliAttrName, subIomObj);
	        			}else {
		        			String shapeGeomTypeName=shapeAttrType.getName().toString();
		        			String shapeTypeName=shapeAttrType.getBinding().getSimpleName();
		        			if(shapeAttrValue instanceof java.util.Date) {
		        				java.util.Date date=(java.util.Date)shapeAttrValue;
		        				if(date.getHours()==0 && date.getMinutes()==0 && date.getSeconds()==0) {
			        				iomObj.setattrvalue(iliAttrName,xtfDate.format(date));
		        				}else {
			        				iomObj.setattrvalue(iliAttrName,xtfDateTime.format(date));
		        				}
		        			}else {
		        				String valueStr=shapeAttrValue.toString();
		        				if(valueStr!=null && valueStr.length()>0)
			        			iomObj.setattrvalue(iliAttrName, valueStr);
		        			}
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

    private String getNameList(List<AttributeDescriptor> attrs) {
		StringBuffer ret=new StringBuffer();
		String sep="";
		for(AttributeDescriptor attr:attrs) {
			ret.append(sep);
			ret.append(attr.getLocalName());
			sep=",";
		}
		return ret.toString();
	}
    
	private Viewable getViewableByShapeAttributes(List<AttributeDescriptor> shapeAttrs,List<String> iliAttrs) throws IoxException{
    	Viewable viewable=null;
    	ArrayList<ArrayList<Viewable>> models=setupNameMapping();
    	// first last model file.
    	for(int modeli=models.size()-1;modeli>=0;modeli--){
    		ArrayList<Viewable> classes=models.get(modeli);
    		for(int classi=classes.size()-1;classi>=0;classi--){
    			Viewable iliViewable=classes.get(classi);
    			Map<String,ch.interlis.ili2c.metamodel.AttributeDef> iliAttrMap=new HashMap<String,ch.interlis.ili2c.metamodel.AttributeDef>();
    			Iterator attrIter=iliViewable.getAttributes();
    			ArrayList<ch.interlis.ili2c.metamodel.AttributeDef> geomAttrs=new ArrayList<ch.interlis.ili2c.metamodel.AttributeDef>();
    			while(attrIter.hasNext()){
    				ch.interlis.ili2c.metamodel.AttributeDef attribute=(ch.interlis.ili2c.metamodel.AttributeDef) attrIter.next();
    				String attrName=attribute.getName();
    				ch.interlis.ili2c.metamodel.Type type=attribute.getDomainResolvingAliases();
    				if(type instanceof ch.interlis.ili2c.metamodel.CoordType || type instanceof ch.interlis.ili2c.metamodel.LineType) {
    					geomAttrs.add(attribute);
    				}else {
        				iliAttrMap.put(attrName.toLowerCase(),attribute);
    				}
    			}
    			// check if ili model attributes are the same as the attributes in the shape file
				if(equalAttrs(iliAttrMap, geomAttrs,shapeAttrs)){
					viewable=iliViewable;
		    		iliAttrs.clear();
			    	theGeomAttr=geomAttrs.get(0).getName();
			    	for(AttributeDescriptor shapeAttr:shapeAttrs) {
			    		if(shapeAttr.getLocalName().equals(GEOTOOLS_THE_GEOM)) {
			    			iliAttrs.add(theGeomAttr);
			    		}else {
				    		iliAttrs.add(iliAttrMap.get(shapeAttr.getLocalName().toLowerCase()).getName());
			    		}
			    	}
		    		return viewable;
				}
    		}
    	}
    	return null;
    }
	
    private boolean equalAttrs(Map<String, ch.interlis.ili2c.metamodel.AttributeDef> iliAttrs, List<ch.interlis.ili2c.metamodel.AttributeDef> geomAttrs,List<AttributeDescriptor> shapeAttrs) {
    	if(iliAttrs.size()+1!=shapeAttrs.size() || geomAttrs.size()!=1) {
    		return false;
    	}
    	for(AttributeDescriptor shapeAttr:shapeAttrs) {
    		if(shapeAttr.getLocalName().equals(GEOTOOLS_THE_GEOM)) {
    			// ignore it
    		}else if(!iliAttrs.containsKey(shapeAttr.getLocalName().toLowerCase())) {
    			return false;
    		}
    	}
		return true;
	}

    private ArrayList<ArrayList<Viewable>> setupNameMapping(){
    	ArrayList<ArrayList<Viewable>> models=new ArrayList<ArrayList<Viewable>>();
		Iterator tdIterator = td.iterator();
		while(tdIterator.hasNext()){
			Object modelObj = tdIterator.next();
			if(!(modelObj instanceof Model)){
				continue;
			}
			if(modelObj instanceof PredefinedModel) {
				continue;
			}
			// iliModel
			Model model = (Model) modelObj;
			ArrayList<Viewable> classes=new ArrayList<Viewable>();
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
		    		if(!(classObj instanceof Table)){
    					continue;
    				}
		    		Table viewable = (Table) classObj;
		    		if(viewable.isAbstract() || !viewable.isIdentifiable()) {
		    			continue;
		    		}
		    		classes.add(viewable);
		    	}
			}
			models.add(classes);
		}
		return models;
    }
    
    /** increase count is used to increase oid and bid.
     * @return String of increasing number.
     */
    private String getNextId(){
    	int count=nextId;
    	nextId+=1;
    	return String.valueOf(count);
    }
    
   /** create a new IomObject.
    * IomObject iomObj=createIomObject("{@code type}", "{@code oid}");<br>
	* Define param:<br>
	* <br>
	* if model is set:
	* <li>type=="modelname.topicname.classname"</li>
	* <li>oid==Start counting by 1, go on adding 1 to existing count. model, basket and class id's are unique.</li>
	* <p>
	* if no model set:
	* <li>The default modelname is: 'The name of the shapefile'</li>
	* <li>The default topicname is: 'Topic(1,2,3,...)'</li>
	* <li>The default classname is: 'Class(1,2,3,...)'</li>
	* <li>type=="modelname.topicname.classname"</li>
    * <li>oid==Start counting by 1, go on adding 1 to existing count. model, basket and class id's are unique.</li>
    * <p>
    * @param type
    * @param oid
    * @return IomObject
    * @exception IoxException
    */
    @Override
	public IomObject createIomObject(String type, String oid)throws IoxException{
    	if(oid==null){
			oid="o"+getNextId();
    	}
		return factory.createIomObject(type, oid);
	}
    
    /** close feature collection iterator and data store.
     * @exception IoxException
     */
	@Override
	public void close() throws IoxException {
		if(featureCollectionIter!=null) {
			featureCollectionIter.close();
			featureCollectionIter=null;
		}
		if(dataStore!=null) {
			dataStore.dispose();
			dataStore=null;
		}
	}

	/** get factory.
	 * @return IoxFactoryCollection
	 * @exception IoxException
	 */
	@Override
	public IoxFactoryCollection getFactory() throws IoxException{
		return factory;
	}
	
	/** set factory.
	 * @param factory to set.
	 * @exception IoxException
	 */
	@Override
	public void setFactory(IoxFactoryCollection factory) throws IoxException{
		this.factory=factory;
	}
	
	/** gets the list of attributes in the read/returned IomObjects.
	 * @return list of attribute names.
	 */
	public String[] getAttributes() {
		return iliAttributes.toArray(new String[iliAttributes.size()]);
	}
	
	/** gets the name of the geometry attribute in the read/returned IomObjects.
	 * @return name of geometry attribute.
	 */
	public String getGeomAttr() {
		return theGeomAttr;
	}
}