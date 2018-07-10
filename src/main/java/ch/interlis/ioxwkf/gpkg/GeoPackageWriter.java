package ch.interlis.ioxwkf.gpkg;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.generator.XSDGenerator;
import ch.interlis.ili2c.metamodel.LocalAttribute;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.EndBasketEvent;
import ch.interlis.iox.EndTransferEvent;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxWriter;
import ch.interlis.iox.StartBasketEvent;
import ch.interlis.iox.StartTransferEvent;
import ch.interlis.iox_j.ObjectEvent;

/** Write data to a GeoPackage.
 * If the table in the file already exists, the existing table will be replaced.
 * If the file exists, the table will be added.
 * <p>
 * <b>Interlis model</b><p>
 * <ul>
 * <li>If a model is set, a class as defined by the object tag is required. The feature type is created based on this class. If the class can not be found, an exception will be thrown.</li>
 * <li>If no model is set, but attribute descriptors defined, these attributes descriptors are used to create the feature type (see {@link #setAttributeDescriptors(AttributeDescriptor[])}.</li>
 * <li>If no model is set, the first object will be used to create the feature type.</li>
 * </ul>
 * <p>
 * <b>Data type mapping</b><br>
 * Any attribute type that is not Numeric, COORD, POLYLINE, SURFACE, AREA or XMLDate is mapped to a text attribute in the shape file
 * and its value is encoded according to Interlis 2.3 encoding rules.<p>
 * <b>Not supported INTERLIS data types</b><p>
 * <ul>
 * <li>StructureType</li>
 * <li>ReferenceType</li>
 * </ul>
 * <p>
 * <b>Curved geometries are not supported.</b><p>
 * <p>
 * 
 * <b>Attachement</b><p>
 * <li><a href="http://www.geopackage.org/spec/">GeoPackage specification</a></li>
 * <li><a href="https://www.ech.ch/vechweb/page?p=dossier&documentNumber=eCH-0031&documentVersion=2.0">Interlis specification</a></li>
 */
public class GeoPackageWriter implements IoxWriter {
    private static final String CRS_CODESPACE_EPSG = "EPSG";

    private List<AttributeDescriptor> attrDescs=null; // --> attribute type data

    // gpkg geometry types
    private static final String POINT="POINT";
    private static final String LINESTRING="LINESTRING";
    private static final String POLYGON="POLYGON";
    private static final String MULTIPOINT="MULTIPOINT";
    private static final String MULTILINESTRING="MULTILINESTRING";
    private static final String MULTIPOLYGON="MULTIPOLYGON";
    private static final String GEOMETRYCOLLECTION="GEOMETRYCOLLECTION";
    private static final String CIRCULARSTRING="CIRCULARSTRING";
    private static final String COMPOUNDCURVE="COMPOUNDCURVE";
    private static final String CURVEPOLYGON="CURVEPOLYGON";
    private static final String MULTICURVE="MULTICURVE";
//    private static final String MULTISURFACE="MULTISURFACE";
    
    // ili types
    private static final String COORD="COORD";
    private static final String MULTICOORD="MULTICOORD";
    private static final String POLYLINE="POLYLINE";
    private static final String MULTIPOLYLINE="MULTIPOLYLINE";
    private static final String MULTISURFACE="MULTISURFACE";

    // geopackage writer
    private Connection conn = null;
    private ResultSet featureSet = null;
    private Statement featureStatement = null;

    private Integer srsId=null;
    private Integer defaultSrsId;

    // model
    private TransferDescription td=null;
    private String iliGeomAttrName=null;
    
    private String tableName = null;
    private boolean isNewFile = false;

    public GeoPackageWriter(File file, String tableName) throws IoxException {
        this(file, tableName, null);
    }
    
    public GeoPackageWriter(File file, String tableName, Settings settings) throws IoxException {
        init(file, tableName, settings);
    }
    
    private void init(File file, String tableName, Settings settings) throws IoxException {
        this.tableName = tableName;
        
        if (file.exists()) {
            isNewFile = false;
        } else {
            isNewFile = true;
        }
        
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
            conn.setAutoCommit(false);
            if (isNewFile) {
                initGeoPackageFile();
            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                    conn = null;
                } catch (SQLException f) {
                    throw new IoxException("not able to connect or create geopackage file");
                }
            }
            throw new IoxException(e);
        } 
    }
    
    private void initGeoPackageFile() {
        if (isNewFile) {
            // exec init script
            LineNumberReader reader=null;
            try {
                String filename = "init.sql";
                InputStream initsqlStream = getClass().getResourceAsStream(filename);
                if (initsqlStream == null) {
                    throw new IllegalStateException("Resource "+filename+" not found");
                }
                reader = new LineNumberReader(new InputStreamReader(initsqlStream, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
            try{
                String line=reader.readLine();
                while (line != null) {
                    // exec sql
                    line = line.trim();
                    if (line.length() > 0) {
                        Statement dbstmt = null;
                        try {
                            try {
                                dbstmt = conn.createStatement();
                                dbstmt.execute(line);
                            } finally {
                                dbstmt.close();
                            }
                        } catch(SQLException ex) {
                            throw new IllegalStateException(ex);
                        }
                    }
                    // read next line
                    line = reader.readLine();
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } finally {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
    
    //TODO: Notizen:
    // BOOLEAN = ch.interlis.ili2c.metamodel.EnumerationType
    // XMLDate = ch.interlis.ili2c.metamodel.FormattedType
    
    @Override
    public void write(IoxEvent event) throws IoxException {
        if (event instanceof StartTransferEvent){
            // ignore
        } else if (event instanceof StartBasketEvent) {
            // ignore
        } else if (event instanceof ObjectEvent) {
            ObjectEvent obj=(ObjectEvent) event;
            IomObject iomObj=(IomObject)obj.getIomObject();
            System.out.println(iomObj.toString());
            String tag = iomObj.getobjecttag();
            
            if (attrDescs == null) {
                if (td != null) {
                	
                	StringBuffer createTable = new StringBuffer();
                	List<String> attrList = new ArrayList<String>();
                	
                	
                    Viewable aclass=(Viewable) XSDGenerator.getTagMap(td).get(tag);
                    if (aclass == null){
                        throw new IoxException("class "+iomObj.getobjecttag()+" not found in model");
                    }
                    Iterator viewableIter = aclass.getAttributes();
                    while (viewableIter.hasNext()) {
                        Object attrObj=viewableIter.next();
                        System.out.println(attrObj);
                        if(attrObj instanceof LocalAttribute) {
                            LocalAttribute localAttr= (LocalAttribute)attrObj;
                            String attrName=localAttr.getName();
                            System.out.println(attrName);
                            
                                                        
                            ch.interlis.ili2c.metamodel.Type iliType=localAttr.getDomainResolvingAliases();
                            System.out.println(iliType);
                            System.out.println(iliType);
                            if(iliType instanceof ch.interlis.ili2c.metamodel.CoordType) {
                                System.out.println("CoordType");
                                attrList.add(attrName + " " + POINT);
//                                createTable.append("POINT,");
                                
                                if (defaultSrsId != null) {
                                    // fubar
                                }
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.PolylineType) {
                                System.out.println("PolylineType");
                                
                                ch.interlis.ili2c.metamodel.PolylineType polylineType = (ch.interlis.ili2c.metamodel.PolylineType) iliType;
                                
                                if (defaultSrsId != null) {
                                    // fubar
                                }
                            } else if(iliType instanceof ch.interlis.ili2c.metamodel.SurfaceOrAreaType) {
                                System.out.println("SurfaceOrArea");
                                
                                ch.interlis.ili2c.metamodel.SurfaceOrAreaType surfaceOrAreaType = (ch.interlis.ili2c.metamodel.SurfaceOrAreaType) iliType;
                                System.out.println(surfaceOrAreaType.getDefinedMaxOverlap());
                                System.out.println(surfaceOrAreaType.getLineForms()[0].getName());
                                System.out.println(surfaceOrAreaType.getLineForms()[1].getName());
                                
                                if (defaultSrsId != null) {
                                    // fubar
                                }
                            } 
                            
                            else {
                                System.out.println("String");
                                                            }
                       
                        }
                    }
                    System.out.println(String.join(",", attrList));
                }
            }
            

        } else if(event instanceof EndBasketEvent){
            // ignore
        } else if (event instanceof EndTransferEvent) {
            if (conn != null) {
                try {
                    conn.commit();
                } catch (SQLException e) {
                    try {
                        conn.rollback();  
                    } catch (SQLException ex) {
                        throw new IoxException(ex.getMessage());
                    }                  
                    throw new IoxException(e.getMessage());
                } finally {
                    try {
                        conn.setAutoCommit(true);
                        conn.close();
                    } catch (SQLException e) {
                        throw new IoxException(e.getMessage());
                    }
                }
            }
        }
        
        
        // TODO Auto-generated method stub
        // if/else if event instance of ...
        // im ObjectEvent prüfen ob tabelle existiert, falls nicht -> erstellen
    }
    
    /** Defines the Interlis model/class of the file, to be written.
     * @param td Interlis model
     */
    public void setModel(TransferDescription td) {
        this.td = td;
    }
    
    @Override
    public void close() throws IoxException {
        // TODO Auto-generated method stub

    }

    @Override
    public IomObject createIomObject(String arg0, String arg1) throws IoxException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void flush() throws IoxException {
        // TODO Auto-generated method stub

    }

    @Override
    public IoxFactoryCollection getFactory() throws IoxException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setFactory(IoxFactoryCollection arg0) throws IoxException {
        // TODO Auto-generated method stub

    }



}
