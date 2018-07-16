package ch.interlis.ioxwkf.gpkg;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.converter.ConverterException;
import ch.ehi.ili2gpkg.GpkgColumnConverter;
import ch.ehi.ili2gpkg.Iox2gpkg;
import ch.interlis.ili2c.generator.XSDGenerator;
import ch.interlis.ili2c.metamodel.CoordType;
import ch.interlis.ili2c.metamodel.Domain;
import ch.interlis.ili2c.metamodel.LocalAttribute;
import ch.interlis.ili2c.metamodel.MultiSurfaceOrAreaType;
import ch.interlis.ili2c.metamodel.NumericType;
import ch.interlis.ili2c.metamodel.NumericalType;
import ch.interlis.ili2c.metamodel.SurfaceOrAreaType;
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
import ch.interlis.iox_j.wkb.Iox2wkbException;

/** Write data to a GeoPackage.
 * If the table in the file already exists, the existing table will be replaced.
 * If the file exists, the table will be added.
 * Only one geometry attribute per table is allowed.
 * Curved geometries are not supported.
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

    // gpkg geometry types
    private static final String POINT="POINT";
    private static final String LINESTRING="LINESTRING";
    private static final String POLYGON="POLYGON";
    private static final String MULTIPOINT="MULTIPOINT";
    private static final String MULTILINESTRING="MULTILINESTRING";
    private static final String MULTIPOLYGON="MULTIPOLYGON";
    private static final String GEOMETRYCOLLECTION="GEOMETRYCOLLECTION";
//    private static final String CIRCULARSTRING="CIRCULARSTRING";
//    private static final String COMPOUNDCURVE="COMPOUNDCURVE";
//    private static final String CURVEPOLYGON="CURVEPOLYGON";
//    private static final String MULTICURVE="MULTICURVE";
//    private static final String MULTISURFACE="MULTISURFACE";
    
    // gpkg types
    private static final String TEXT="TEXT";
    private static final String INTEGER="INTEGER";
    private static final String REAL="REAL";
    
    
    // ili types
    private static final String COORD="COORD";
    private static final String MULTICOORD="MULTICOORD";
    private static final String POLYLINE="POLYLINE";
    private static final String MULTIPOLYLINE="MULTIPOLYLINE";
    private static final String MULTISURFACE="MULTISURFACE";

    // geopackage writer
    private Connection conn = null;
    private ResultSet featureSet = null;
    private PreparedStatement featureStatement = null;
    private boolean tableExists = false;
    private boolean appendFeatures = false; // TODO
    private List<AttributeDescriptor> attrDescs = null;
    
    private Integer srsId=null;
    private Integer defaultSrsId = -1;

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
                	attrDescs = new ArrayList<AttributeDescriptor>();
                	Viewable aclass=(Viewable) XSDGenerator.getTagMap(td).get(tag);
                    if (aclass == null){
                        throw new IoxException("class "+iomObj.getobjecttag()+" not found in model");
                    }
                    Iterator viewableIter = aclass.getAttributes();
                    while (viewableIter.hasNext()) {
                        AttributeDescriptor attrDesc = new AttributeDescriptor();
                        Object attrObj=viewableIter.next();
                        if(attrObj instanceof LocalAttribute) {
                            LocalAttribute localAttr= (LocalAttribute)attrObj;
                            String attrName=localAttr.getName();
                            
                            System.out.println("attr name: "+ attrName);
                            
                            ch.interlis.ili2c.metamodel.Type iliType=localAttr.getDomainResolvingAliases();
                            if(iliType instanceof ch.interlis.ili2c.metamodel.CoordType) {
                                System.out.println("CoordType");
                                attrDesc.setGeometry(true);
                                attrDesc.setSrsId(defaultSrsId);

                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;
                                
                                if (!tableExists) {
                                    attrDesc.setDbColumnName(attrName.toLowerCase());
                                    attrDesc.setDbColumnTypeName(POINT);

                                    CoordType coordType = (CoordType)iliType;
                                    if (coordType.getDimensions().length == 3) {
                                    	attrDesc.set3D(true);
                                    }
                
                                    BoundingBox bbox = getBoundingBoxFromIliGeomTyp(coordType);
                                    attrDesc.setBbox(bbox);
                                    
                                    attrDescs.add(attrDesc);
                                }
                                
                                
                                
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.PolylineType) {
                                System.out.println("PolylineType");
                                
                                if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;

                                ch.interlis.ili2c.metamodel.PolylineType polylineType = (ch.interlis.ili2c.metamodel.PolylineType) iliType;
                                

                            } else if(iliType instanceof ch.interlis.ili2c.metamodel.SurfaceOrAreaType) {
                                System.out.println("SurfaceOrArea");        
                                attrDesc.setGeometry(true);
                                attrDesc.setSrsId(defaultSrsId);
                                System.out.println(attrDesc.getSrsId());

                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;

                                if (!tableExists) {
                                    attrDesc.setDbColumnName(attrName.toLowerCase());
                                    attrDesc.setDbColumnTypeName(POLYGON);

                                    Domain domain = ((SurfaceOrAreaType) iliType).getControlPointDomain();
                                    CoordType coordType = (CoordType) domain.getType();

                                    if (coordType.getDimensions().length == 3) {
                                    	attrDesc.set3D(true);
                                    }
                                    
                                    BoundingBox bbox = getBoundingBoxFromIliGeomTyp(coordType);
                                    attrDesc.setBbox(bbox);
                                    
                                    SurfaceOrAreaType surfaceOrAreaType = (SurfaceOrAreaType) iliType;
                                    double maxOverlap = surfaceOrAreaType.getMaxOverlap().doubleValue();       
                                    attrDesc.setMaxOverlap(maxOverlap);
                                    
                                    attrDescs.add(attrDesc);
                                }                                
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.MultiSurfaceOrAreaType) {
                                System.out.println("MultiSurfaceOrArea");        
                                attrDesc.setGeometry(true);
                                attrDesc.setSrsId(defaultSrsId);
                                System.out.println(attrDesc.getSrsId());

                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;
                                
                                if (!tableExists) {
                                    attrDesc.setDbColumnName(attrName.toLowerCase());
                                    attrDesc.setDbColumnTypeName(MULTIPOLYGON);

                                    Domain domain = ((MultiSurfaceOrAreaType) iliType).getControlPointDomain();
                                    CoordType coordType = (CoordType) domain.getType();

                                    if (coordType.getDimensions().length == 3) {
                                    	attrDesc.set3D(true);
                                    }
                                    
                                    BoundingBox bbox = getBoundingBoxFromIliGeomTyp(coordType);
                                    attrDesc.setBbox(bbox);
                                    
                                    MultiSurfaceOrAreaType multiSurfaceOrAreaType = (MultiSurfaceOrAreaType) iliType;
                                    double maxOverlap = multiSurfaceOrAreaType.getMaxOverlap().doubleValue();       
                                    attrDesc.setMaxOverlap(maxOverlap);
                                    
                                    attrDescs.add(attrDesc);
                                }                                
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.NumericalType) {
                            	System.out.println("NumericalType");
                            	
                                if (!tableExists) {
                                	attrDesc.setDbColumnName(attrName.toLowerCase());
                                	
                                	NumericalType numericalType = (NumericalType)iliType;
                                	NumericType numericType = (NumericType)numericalType;
                                	int precision = numericType.getMinimum().getAccuracy(); 
                                	if (precision > 0) {
                                		attrDesc.setDbColumnTypeName(REAL);
                                	} else {
                                		attrDesc.setDbColumnTypeName(INTEGER);
                                	}
                                	
                                    attrDescs.add(attrDesc);
                                }
                            } else {
                            	System.out.println("String, Boolean, ");
                                if (localAttr.isDomainBoolean()) {
	                                if (!tableExists) {
	                                    attrDesc.setDbColumnName(attrName.toLowerCase());
	                                    attrDesc.setDbColumnTypeName(INTEGER);
	                                }
                                } else {
                                    if (!tableExists) {
                                        attrDesc.setDbColumnName(attrName.toLowerCase());
                                        attrDesc.setDbColumnTypeName(TEXT);
                                        attrDescs.add(attrDesc);
                                    }
                                }
                            }         
                        }
                    }
                   
                } else {
                	System.out.println("no td set");
                	System.out.println(iomObj.getattrcount());
                	for (int u=0;u<iomObj.getattrcount();u++) {
                		String attrName=iomObj.getattrname(u);
                		System.out.println(attrName);

                		
                		IomObject iomGeom=iomObj.getattrobj(attrName,0);
                		System.out.println(iomGeom.toString());
                	}
                }
            }
            if (tableExists) {
            	throw new IoxException("Table '" + tableName + "' already exists.");
            } else {
            	if (attrDescs != null) {
                	try {
                		// create empty table
                		List<String> attrList = new ArrayList<String>();
                		for (AttributeDescriptor attrDesc : attrDescs) {
                			attrList.add(attrDesc.getDbColumnName() + " " + attrDesc.getDbColumnTypeName());
                		}
                		StringBuffer createTableSql = new StringBuffer();
                		createTableSql.append("CREATE TABLE " + tableName + " (");
                		createTableSql.append(String.join(",", attrList));
                		createTableSql.append(")");
                		
                		System.out.println(createTableSql);
                		
                		PreparedStatement createTableStmt = conn.prepareStatement(createTableSql.toString());
                		createTableStmt.executeUpdate();

                		// Insert table information into gpkg_contents meta table.
                		String gpkgContentsSql = "INSERT INTO gpkg_contents (table_name, data_type, identifier, last_change, min_x, min_y, max_x, max_y, srs_id) "
                				+ "VALUES (?,?,?,?,?,?,?,?,?)";

                		PreparedStatement gpkgContentsStmt = conn.prepareStatement(gpkgContentsSql);
                		gpkgContentsStmt.setString(1, tableName);
                		gpkgContentsStmt.setString(2, "features");
                		gpkgContentsStmt.setString(3, tableName);

                		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
                		gpkgContentsStmt.setString(4, dt.format(new Date()));

                		for (AttributeDescriptor attrDesc : attrDescs) {
                			if (attrDesc.isGeometry()) {
                				BoundingBox bbox = attrDesc.getBbox();
                				gpkgContentsStmt.setDouble(5, bbox.getxMin());
                				gpkgContentsStmt.setDouble(6, bbox.getyMin());
                				gpkgContentsStmt.setDouble(7, bbox.getxMax());
                				gpkgContentsStmt.setDouble(8, bbox.getyMax());
                				gpkgContentsStmt.setInt(9, attrDesc.getSrsId());
                			}
                		}
                		gpkgContentsStmt.executeUpdate();
                		
                		// Insert into gpkg_geometry_columns meta table.
                		String gpkgGeoColSql = "INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) "
                				+ "VALUES (?,?,?,?,?,?)";
                		
                		PreparedStatement gpkgGeoColStmt = conn.prepareStatement(gpkgGeoColSql);
                		gpkgGeoColStmt.setString(1, tableName);
                		
                		for (AttributeDescriptor attrDesc : attrDescs) {
                			if (attrDesc.isGeometry()) {
                				gpkgGeoColStmt.setString(2, attrDesc.getDbColumnName());
                				gpkgGeoColStmt.setString(3, attrDesc.getDbColumnTypeName());
                				gpkgGeoColStmt.setInt(4, attrDesc.getSrsId());
                				
                				if (attrDesc.is3D()) {
                					gpkgGeoColStmt.setInt(5, 1);
                				} else {
                					gpkgGeoColStmt.setInt(5, 0);
                				}
                				
                				gpkgGeoColStmt.setInt(6, 0);
                			}
                		}
                		gpkgGeoColStmt.executeUpdate();


                	} catch (SQLException e) {
                		// TODO: rollback hier? Es gibt bereits eine journal-Datei.
                		throw new IoxException(e.getMessage());
                	}
                    tableExists = true;	
            	}
            }
           
            if (attrDescs != null) {
        		// insert statement
        		List<String> attrList = new ArrayList<String>();
        		for (AttributeDescriptor attrDesc : attrDescs) {
        			attrList.add(attrDesc.getDbColumnName());
        		}
        		
        		StringBuffer insertIntoTableSql = new StringBuffer();
        		insertIntoTableSql.append("INSERT INTO " + tableName + " (");
        		insertIntoTableSql.append(String.join(",", attrList));
        		insertIntoTableSql.append(") VALUES (");
        		
        		for (int i=0; i<attrDescs.size(); i++) {
        			if (i==0) {
        				insertIntoTableSql.append("?");
        			} else {
        				 insertIntoTableSql.append(",?");
        			}
        		}
        		
        		insertIntoTableSql.append(")");
                System.out.println(insertIntoTableSql);
                
                try {
                	PreparedStatement pstmt = conn.prepareStatement(insertIntoTableSql.toString());
                	System.out.println("******************");
                	convertObject(iomObj, pstmt);
                	pstmt.executeUpdate();
				} catch (SQLException e) {
					// TODO: How to deal with sql exception from preparedstatements?
					// A .gpkg-journal file will be around. Rollback/Close does not help.
                	throw new IoxException(e.getMessage());
				} catch (Iox2wkbException e) {
                	throw new IoxException(e.getMessage()); // TODO: Nicht mehr benötigt, da ConverterException?
                } catch (ConverterException e) {
                	System.out.println("fehler");
                	e.printStackTrace();
                	throw new IoxException(e.getMessage());
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
    }
    
    private void convertObject(IomObject obj, PreparedStatement pstmt) throws Iox2wkbException, ConverterException, SQLException {
    	
    	for (int i = 0; i < attrDescs.size(); i++) {
    		AttributeDescriptor attrDesc = attrDescs.get(i);
    		String attrName = attrDesc.getDbColumnName();
    		System.out.println(attrName);
    		
    		if (attrDesc.isGeometry()) {
    			System.out.println("isGeometry");
//    			int outputDimension = (attrDesc.is3D()) ? 3 : 2;
//    	    	Iox2gpkg iox2gpkg = new Iox2gpkg(outputDimension);
    	    	GpkgColumnConverter conv = new GpkgColumnConverter();

    	    	if (attrDesc.getDbColumnTypeName().equalsIgnoreCase(POINT)) {
    	    		System.out.println("isPoint");
    	    		System.out.println(obj.toString());
    				IomObject iomGeom = obj.getattrobj(iliGeomAttrName,0);
    	    		Object geom = conv.fromIomCoord(iomGeom, attrDesc.getSrsId(), attrDesc.is3D());
    	    		pstmt.setObject(i+1, geom);
    	    		System.out.println(geom);
    	    	} else if (attrDesc.getDbColumnTypeName().equalsIgnoreCase(POLYGON)) {
    	    		
    	    	} else if (attrDesc.getDbColumnTypeName().equalsIgnoreCase(MULTIPOLYGON)) {
    				IomObject iomGeom = obj.getattrobj(iliGeomAttrName,0);
    	    		Object geom = conv.fromIomMultiSurface(iomGeom, 2056, false, attrDesc.is3D(), attrDesc.getMaxOverlap()); 
    	    		pstmt.setObject(i+1, geom);
    	    		System.out.println(geom);
    	    	}
    	    	
    			
    		} else {
    			String val=obj.getattrprim(attrName,0);
    			pstmt.setString(i+1, obj.getattrprim(attrName, 0));
        		System.out.println(val);

    		}
    	
    		
    		
    		
    	}
    }
    
    
    
    // calculate x/y min/max from ili model
    private BoundingBox getBoundingBoxFromIliGeomTyp(ch.interlis.ili2c.metamodel.CoordType coordType) throws IoxException {
    	BoundingBox bbox = new BoundingBox();
    	
        NumericalType dimv[] = coordType.getDimensions();
        if(!(dimv[0] instanceof NumericType) || !(dimv[1] instanceof NumericType)){
			throw new IoxException("COORD type not supported ("+dimv[0].getClass().getName()+")");
		}
        
        if(((NumericType)dimv[0]).getMinimum() != null) {
        	bbox.setxMin(((NumericType)dimv[0]).getMinimum().doubleValue());
        	bbox.setxMax(((NumericType)dimv[0]).getMaximum().doubleValue());
        	bbox.setyMin(((NumericType)dimv[1]).getMinimum().doubleValue());
        	bbox.setyMax(((NumericType)dimv[1]).getMaximum().doubleValue());
        }
        
    	return bbox;
    }
    
    
	/** The default srid code, used if not given by the model or the attribute descriptors.
	 * @param sridCode EPSG code e.g. "2056"
	 */
	public void setDefaultSridCode(String sridCode) {
		defaultSrsId = Integer.parseInt(sridCode);
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
