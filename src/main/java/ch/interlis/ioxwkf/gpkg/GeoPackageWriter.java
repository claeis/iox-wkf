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
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2gpkg.Iox2gpkg;
import ch.interlis.ioxwkf.dbtools.AttributeDescriptor;
import ch.interlis.ili2c.generator.XSDGenerator;
import ch.interlis.ili2c.metamodel.BlackboxType;
import ch.interlis.ili2c.metamodel.CoordType;
import ch.interlis.ili2c.metamodel.Domain;
import ch.interlis.ili2c.metamodel.LineForm;
import ch.interlis.ili2c.metamodel.LocalAttribute;
import ch.interlis.ili2c.metamodel.MultiSurfaceOrAreaType;
import ch.interlis.ili2c.metamodel.NumericType;
import ch.interlis.ili2c.metamodel.NumericalType;
import ch.interlis.ili2c.metamodel.SurfaceOrAreaType;
import ch.interlis.ili2c.metamodel.PolylineType;
import ch.interlis.ili2c.metamodel.MultiPolylineType;
import ch.interlis.ili2c.metamodel.MultiCoordType;
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
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;
import ch.interlis.iox_j.wkb.Iox2wkbException;

/** Write data to a GeoPackage.
 * If the table in the file already exists, an exception will be thrown.
 * If the gpkg-file exists, the table will be added.
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
 * Any attribute type that is not Numeric, BOOLEAN, BLOB, COORD, POLYLINE, SURFACE, AREA or XMLDate is mapped to a text attribute in the shape file
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
    private static final String CIRCULARSTRING="CIRCULARSTRING";
    private static final String COMPOUNDCURVE="COMPOUNDCURVE";
    private static final String CURVEPOLYGON="CURVEPOLYGON";
    private static final String MULTICURVE="MULTICURVE";
//    private static final String MULTISURFACE="MULTISURFACE";
    
    // gpkg types
    private static final String TEXT="TEXT";
    private static final String INTEGER="INTEGER";
    private static final String REAL="REAL";
    private static final String DATE="DATE";
    private static final String DATETIME="DATETIME";
    private static final String BLOB="BLOB";
    private static final String BOOLEAN="BOOLEAN";

    // ili types
    private static final String COORD="COORD";
    private static final String MULTICOORD="MULTICOORD";
    private static final String POLYLINE="POLYLINE";
    private static final String MULTIPOLYLINE="MULTIPOLYLINE";
    private static final String SURFACE="SURFACE";
    private static final String MULTISURFACE="MULTISURFACE";
    private static final String TRUE="true";
    private static final String FALSE="false";

    // geopackage writer
    private Connection conn = null;
    private boolean tableExists = false;
    private boolean appendFeatures = false; // TODO
    private List<AttributeDescriptor> attrDescs = null;
    private Double xMin = null;
    private Double yMin = null;
    private Double xMax = null;
    private Double yMax = null;
    
    private Integer srsId=null;
    private Integer defaultSrsId = -1;

	private SimpleDateFormat xtfDate=new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat xtfDateTime=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	// model
    private TransferDescription td=null;
    private String iliGeomAttrName=null;
    
    private String tableName = null;
    private boolean isNewFile = false;
    private boolean emptyDataSet = true;

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
                        Statement stmt = null;
                        try {
                            try {
                                stmt = conn.createStatement();
                                stmt.execute(line);
                            } finally {
                                stmt.close();
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
        
    @Override
    public void write(IoxEvent event) throws IoxException {
        if (event instanceof StartTransferEvent){
            // ignore
        } else if (event instanceof StartBasketEvent) {
            // ignore
        } else if (event instanceof ObjectEvent) {
        	ObjectEvent obj=(ObjectEvent) event;
            IomObject iomObj=(IomObject)obj.getIomObject();
            String tag = iomObj.getobjecttag();
            
            // Falls der Writer via Db2gpkg verwendet wird: In diesem Fall
            // wird attrDescs aus den DB-Tabellen resp. -Spalten direkt abgefüllt und
            // mittels public Methode 'setAttributeDescriptors' gesetzt.
            // attrDescs wird benötigt, um die Tabelle in der GeoPackage-DB anzulegen.
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
                            attrDesc.setIomAttributeName(attrName);
                            
                            ch.interlis.ili2c.metamodel.Type iliType=localAttr.getDomainResolvingAliases();
                            if(iliType instanceof ch.interlis.ili2c.metamodel.CoordType) {
                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;
                                
                                CoordType coordType = (CoordType)iliType;
                                attrDesc.setCoordDimension(coordType.getDimensions().length);
                            	
                                attrDesc.setDbColumnGeomTypeName(POINT);
                                attrDesc.setSrId(defaultSrsId);
                                attrDesc.setDbColumnName(attrName.toLowerCase());
                                attrDescs.add(attrDesc);
                                
                                setExtentFromIliDimensions(coordType.getDimensions());                          
                            } else if(iliType instanceof ch.interlis.ili2c.metamodel.MultiCoordType) {
                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;
                                
                                MultiCoordType multiCoordType = (MultiCoordType)iliType;
                                attrDesc.setCoordDimension(multiCoordType.getDimensions().length);

                                attrDesc.setDbColumnGeomTypeName(MULTIPOINT);
                                attrDesc.setSrId(defaultSrsId);
                                attrDesc.setDbColumnName(attrName.toLowerCase());
                                attrDescs.add(attrDesc);
                                
                                setExtentFromIliDimensions(multiCoordType.getDimensions());                          
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.PolylineType) {
                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;

                                Domain domain = ((PolylineType) iliType).getControlPointDomain();
                                CoordType coordType = (CoordType) domain.getType();
                                attrDesc.setCoordDimension(coordType.getDimensions().length);
                            
                                attrDesc.setDbColumnGeomTypeName(LINESTRING);
                                attrDesc.setSrId(defaultSrsId);
                                attrDesc.setDbColumnName(attrName.toLowerCase());
                                attrDescs.add(attrDesc);
                                
                                setExtentFromIliDimensions(coordType.getDimensions());
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.MultiPolylineType) {
                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;

                                Domain domain = ((MultiPolylineType) iliType).getControlPointDomain();
                                CoordType coordType = (CoordType) domain.getType();
                                attrDesc.setCoordDimension(coordType.getDimensions().length);
                              
                                attrDesc.setDbColumnGeomTypeName(MULTILINESTRING);
                                attrDesc.setSrId(defaultSrsId);
                                attrDesc.setDbColumnName(attrName.toLowerCase());
                                attrDescs.add(attrDesc);
                                
                                setExtentFromIliDimensions(coordType.getDimensions());
                            } else if(iliType instanceof ch.interlis.ili2c.metamodel.SurfaceOrAreaType) {
                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;

                                Domain domain = ((SurfaceOrAreaType) iliType).getControlPointDomain();
                                CoordType coordType = (CoordType) domain.getType();
                                attrDesc.setCoordDimension(coordType.getDimensions().length);

                                attrDesc.setDbColumnGeomTypeName(POLYGON);
                                attrDesc.setSrId(defaultSrsId);
                                attrDesc.setDbColumnName(attrName.toLowerCase());
                                attrDescs.add(attrDesc);
                                
                                setExtentFromIliDimensions(coordType.getDimensions());
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.MultiSurfaceOrAreaType) {
                            	if (iliGeomAttrName != null) {
                                	throw new IoxException("only one geometry attribute allowed");
                                }
                                iliGeomAttrName = attrName;

                                Domain domain = ((MultiSurfaceOrAreaType) iliType).getControlPointDomain();
                                CoordType coordType = (CoordType) domain.getType();
                                attrDesc.setCoordDimension(coordType.getDimensions().length);
                                      
                                attrDesc.setDbColumnGeomTypeName(MULTIPOLYGON);
                                
                                // TODO
//                                LineForm[] lineForms = ((MultiSurfaceOrAreaType) iliType).getLineForms();
//                                for (LineForm lineForm : lineForms) {
//                                	if (lineForm.getName().equalsIgnoreCase("ARCS")) {
//                                        attrDesc.setDbColumnGeomTypeName(CURVEPOLYGON);
//                                	} 
//                                }
                                
                                attrDesc.setSrId(defaultSrsId);
                                attrDesc.setDbColumnName(attrName.toLowerCase());
                                attrDescs.add(attrDesc);
                                                                
                                setExtentFromIliDimensions(coordType.getDimensions());
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.NumericalType) {
                            	NumericalType numericalType = (NumericalType)iliType;
                            	NumericType numericType = (NumericType)numericalType;
                            	int precision = numericType.getMinimum().getAccuracy(); 
                            	if (precision > 0) {
                            		attrDesc.setDbColumnTypeName(REAL);
                            	} else {
                            		attrDesc.setDbColumnTypeName(INTEGER);
                            	}
                            	attrDesc.setDbColumnName(attrName.toLowerCase());
                            	attrDescs.add(attrDesc);
                            } else if (iliType instanceof ch.interlis.ili2c.metamodel.BlackboxType) {
                        		attrDesc.setDbColumnName(attrName.toLowerCase());
                            	if(((BlackboxType)iliType).getKind()==BlackboxType.eXML) {
                            		attrDesc.setDbColumnTypeName(TEXT);
                            	} else {
                            		attrDesc.setDbColumnTypeName(BLOB);
                            	}
                            	attrDescs.add(attrDesc);
                            } else {
                            	if (localAttr.isDomainBoolean()) {
                            		attrDesc.setDbColumnName(attrName.toLowerCase());
                            		attrDesc.setDbColumnTypeName(BOOLEAN);
                                	attrDescs.add(attrDesc);
                            	} else if (localAttr.isDomainIli2Date()) {
                            		attrDesc.setDbColumnName(attrName.toLowerCase());
                            		attrDesc.setDbColumnTypeName(DATE);
                                	attrDescs.add(attrDesc);
                            	} else if (localAttr.isDomainIli2DateTime()) {
                            		attrDesc.setDbColumnName(attrName.toLowerCase());
                            		attrDesc.setDbColumnTypeName(DATETIME);
                                	attrDescs.add(attrDesc);
                            	} else {
                            		attrDesc.setDbColumnName(attrName.toLowerCase());
                            		attrDesc.setDbColumnTypeName(TEXT);
                            		attrDescs.add(attrDesc);
                            	}
                            }   
                        }
                    } 
                } else {
                	attrDescs = new ArrayList<AttributeDescriptor>();
                	for (int u=0;u<iomObj.getattrcount();u++) {
                		AttributeDescriptor attrDesc = new AttributeDescriptor();
                		String attrName=iomObj.getattrname(u);
                		
                		if (iliGeomAttrName==null && iomObj.getattrvaluecount(attrName)>0 && iomObj.getattrobj(attrName,0)!=null) {
    						iliGeomAttrName=attrName;
    						IomObject iomGeom=iomObj.getattrobj(attrName,0);
                    		if (iomGeom != null) {
                    			if (iomGeom.getobjecttag().equals(COORD)) {
                    				attrDesc.setDbColumnGeomTypeName(POINT);
                    				
                    			} else if (iomGeom.getobjecttag().equals(MULTICOORD)) {
                    				attrDesc.setDbColumnGeomTypeName(MULTIPOINT);
                    			} else if (iomGeom.getobjecttag().equals(POLYLINE)) {
                    				attrDesc.setDbColumnGeomTypeName(LINESTRING);
                    			} else if (iomGeom.getobjecttag().equals(MULTIPOLYLINE)) {
                    				attrDesc.setDbColumnGeomTypeName(MULTILINESTRING);
                    			} else if (iomGeom.getobjecttag().equals(MULTISURFACE)) {
    								int surfaceCount=iomGeom.getattrvaluecount("surface");
    								if (surfaceCount <= 1) {
                        				attrDesc.setDbColumnGeomTypeName(POLYGON);
    								} else {
                        				attrDesc.setDbColumnGeomTypeName(MULTIPOLYGON);
    								}
                    			}
                				attrDesc.setSrId(defaultSrsId);
                				attrDesc.setCoordDimension(2); // TODO: figure it out from input object
                    		}
                		} else {
                 			attrDesc.setDbColumnTypeName(TEXT);
                 		}
        				attrDesc.setIomAttributeName(attrName);
                		attrDesc.setDbColumnName(attrName.toLowerCase());
                		attrDescs.add(attrDesc);
                	}
                }
            }
            if (tableExists) {
            	throw new IoxException("Table '" + tableName + "' already exists.");
            } else {
            	if (attrDescs != null && attrDescs.size() > 0) {
                	try {
                		// Create empty table.
                		List<String> attrList = new ArrayList<String>();
                		for (AttributeDescriptor attrDesc : attrDescs) {
                			if (attrDesc.getDbColumnGeomTypeName() != null) {
                    			attrList.add(attrDesc.getDbColumnName() + " " + attrDesc.getDbColumnGeomTypeName());
                			} else {
                    			attrList.add(attrDesc.getDbColumnName() + " " + attrDesc.getDbColumnTypeName());
                			}
                		}
                		StringBuffer createTableSql = new StringBuffer();
                		createTableSql.append("CREATE TABLE " + tableName + " (");
                		createTableSql.append(String.join(",", attrList));
                		createTableSql.append(")");
                		
                		System.out.println(createTableSql.toString());
                		                		
                		PreparedStatement createTableStmt = conn.prepareStatement(createTableSql.toString());
                		createTableStmt.executeUpdate();
                		
                		// Insert into gpkg_geometry_columns meta table.
                		String gpkgGeoColSql = "INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) "
                				+ "VALUES (?,?,?,?,?,?)";
                		
                		PreparedStatement gpkgGeoColStmt = conn.prepareStatement(gpkgGeoColSql);
                		gpkgGeoColStmt.setString(1, tableName);
                		
                		for (AttributeDescriptor attrDesc : attrDescs) {
                			if (attrDesc.getDbColumnGeomTypeName() != null) {
                				gpkgGeoColStmt.setString(2, attrDesc.getDbColumnName());
                				gpkgGeoColStmt.setString(3, attrDesc.getDbColumnGeomTypeName());
                				gpkgGeoColStmt.setInt(4, attrDesc.getSrId());
                				
                				if (attrDesc.getCoordDimension() == 3) {
                					gpkgGeoColStmt.setInt(5, 1);
                				} else {
                					gpkgGeoColStmt.setInt(5, 0);
                				}
                				
                				// m aka measure. 0=false, 1=true
                				gpkgGeoColStmt.setInt(6, 0);
                			}
                		}
                		gpkgGeoColStmt.executeUpdate();
                	} catch (SQLException e) {
                		throw new IoxException(e.getMessage());
                		// TODO: There is a .gpkg-journal file when an exception is thrown here.
                		// Not sure how to handle this?!
                	}
                    tableExists = true;
                    emptyDataSet = false;
            	}
            }
           
            if (attrDescs != null && attrDescs.size() > 0) {
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
                
                try {
                	PreparedStatement pstmt = conn.prepareStatement(insertIntoTableSql.toString());
                	convertObject(iomObj, pstmt);
                	pstmt.executeUpdate();
				} catch (SQLException e) {
					// TODO: How to deal with sql exception from preparedstatements?
					// A .gpkg-journal file will be around. Rollback/Close does not help.
					e.printStackTrace();
                	throw new IoxException(e.getMessage());
				} catch (Iox2wkbException e) {
                	e.printStackTrace();
                	throw new IoxException(e.getMessage());
                } catch (Iox2jtsException e) {
					e.printStackTrace();
                	throw new IoxException(e.getMessage());
				} 
            }
        } else if(event instanceof EndBasketEvent){
            // ignore
        } else if (event instanceof EndTransferEvent) {
    		if (!emptyDataSet && attrDescs != null && attrDescs.size() > 0) {
            	try {
            		// Insert table information into gpkg_contents meta table.
            		// Since x/y min/max can change, we insert these values at the end of the reading process.
            		String gpkgContentsSql = "INSERT INTO gpkg_contents (table_name, data_type, identifier, last_change, min_x, min_y, max_x, max_y, srs_id) "
            				+ "VALUES (?,?,?,?,?,?,?,?,?)";

            		PreparedStatement gpkgContentsStmt = conn.prepareStatement(gpkgContentsSql);
            		gpkgContentsStmt.setString(1, tableName);
            		gpkgContentsStmt.setString(2, "features");
            		gpkgContentsStmt.setString(3, tableName);

            		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            		gpkgContentsStmt.setString(4, dt.format(new Date()));

            		for (AttributeDescriptor attrDesc : attrDescs) {
            	   		if (attrDesc.getDbColumnGeomTypeName() != null) {
            	   			if (xMin != null) {
                	   			gpkgContentsStmt.setDouble(5, xMin);
                	   			gpkgContentsStmt.setDouble(6, yMin);
                	   			gpkgContentsStmt.setDouble(7, xMax);
                	   			gpkgContentsStmt.setDouble(8, yMax);
            	   			}
            	   			gpkgContentsStmt.setInt(9, attrDesc.getSrId());
                		}
            		}
            		gpkgContentsStmt.executeUpdate();

            	} catch (SQLException e) {
            		e.printStackTrace();
            		throw new IoxException(e.getMessage());
            	}	
    		}	
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
    }
       
    private void convertObject(IomObject obj, PreparedStatement pstmt) throws Iox2jtsException, Iox2wkbException, SQLException {
    	for (int i = 0; i < attrDescs.size(); i++) {
    		AttributeDescriptor attrDesc = attrDescs.get(i);
    		String iliAttrName = attrDesc.getIomAttributeName();
    		
    		if (attrDesc.getDbColumnGeomTypeName() != null) {
    	    	Iox2gpkg iox2gpgk = new Iox2gpkg(attrDesc.getCoordDimension());
				IomObject iomGeom = obj.getattrobj(iliGeomAttrName,0);
				
				Geometry jtsGeom = null;
				
    	    	if (attrDesc.getDbColumnGeomTypeName().equalsIgnoreCase(POINT)) {
					jtsGeom = new GeometryFactory().createPoint(Iox2jts.coord2JTS(iomGeom));
					Object geom = iox2gpgk.coord2wkb(iomGeom, attrDesc.getSrId());
    	    		pstmt.setObject(i+1, geom);
    	    	} else if (attrDesc.getDbColumnGeomTypeName().equalsIgnoreCase(MULTIPOINT)) {
    	    		jtsGeom = Iox2jts.multicoord2JTS(iomGeom);
    	    		Object geom = iox2gpgk.multicoord2wkb(iomGeom, attrDesc.getSrId());
    	    		pstmt.setObject(i+1, geom);
    	    	} else if (attrDesc.getDbColumnGeomTypeName().equalsIgnoreCase(LINESTRING)) {
    	    		jtsGeom = new GeometryFactory().createLineString(Iox2jts.polyline2JTS(iomGeom, false, 0.00).toCoordinateArray());
    	    		Object geom = iox2gpgk.polyline2wkb(iomGeom, false, false, 0.00, attrDesc.getSrId());
    	    		pstmt.setObject(i+1, geom);
    	    	} else if (attrDesc.getDbColumnGeomTypeName().equalsIgnoreCase(MULTILINESTRING)) {
    	    		jtsGeom = Iox2jts.multipolyline2JTS(iomGeom, 0.00);
    	    		Object geom = iox2gpgk.multiline2wkb(iomGeom, false, 0.00, attrDesc.getSrId());
    	    		pstmt.setObject(i+1, geom);
    	    	} else if (attrDesc.getDbColumnGeomTypeName().equalsIgnoreCase(POLYGON)) {
    	    		jtsGeom = Iox2jts.surface2JTS(iomGeom, 0.00);
    	    		Object geom = iox2gpgk.surface2wkb(iomGeom, false, 0.00, attrDesc.getSrId());
    	    		pstmt.setObject(i+1, geom);
    	    	} else if (attrDesc.getDbColumnGeomTypeName().equalsIgnoreCase(MULTIPOLYGON)) {
    	    		jtsGeom = Iox2jts.multisurface2JTS(iomGeom, 0.00, defaultSrsId);
    	    		Object geom = iox2gpgk.multisurface2wkb(iomGeom, false, 0.00, attrDesc.getSrId());
    	    		pstmt.setObject(i+1, geom);
    	    	}
    	    	
    	    	if (td == null) {
    	    		this.updateExtentFromGeometry(jtsGeom);
    	    	}
    	    	
    		} else if (attrDesc.getDbColumnTypeName().equals(BOOLEAN)) {
    			String val = obj.getattrprim(iliAttrName, 0);
    			if (val.equalsIgnoreCase(TRUE)) {
        			pstmt.setObject(i+1, 1);
    			} else {
        			pstmt.setObject(i+1, 0);
    			}
    		} else if (attrDesc.getDbColumnTypeName().equals(BLOB)) {    			
    			String val = obj.getattrprim(iliAttrName, 0);
    			byte[] byteValue = Base64.getDecoder().decode(val);
    			pstmt.setObject(i+1, byteValue);
    		} else if (attrDesc.getDbColumnTypeName().equals(DATETIME)) {
    			// TODO: do we need some timezone conversion?
    			String val=obj.getattrprim(iliAttrName,0);
    			pstmt.setObject(i+1, val + "Z");
    		} else {
    			String val=obj.getattrprim(iliAttrName,0);
    			pstmt.setObject(i+1, val);
    		}
    	}
    }
    
    // calculate x/y min/max from ili model
    private void setExtentFromIliDimensions(ch.interlis.ili2c.metamodel.NumericalType[] dimv) throws IoxException {    	
        if(!(dimv[0] instanceof NumericType) || !(dimv[1] instanceof NumericType)){
			throw new IoxException("COORD type not supported ("+dimv[0].getClass().getName()+")");
		}
        
        if(((NumericType)dimv[0]).getMinimum() != null) {
        	xMin = ((NumericType)dimv[0]).getMinimum().doubleValue();
        	xMax = ((NumericType)dimv[0]).getMaximum().doubleValue();
        	yMin = ((NumericType)dimv[1]).getMinimum().doubleValue();
        	yMax = ((NumericType)dimv[1]).getMaximum().doubleValue();
        }
    }
    
    // update x/y min/max from geometry
    private void updateExtentFromGeometry(Geometry geom) {    	
    	Envelope envelope = geom.getEnvelopeInternal();
    	double xMinObj = envelope.getMinX();
    	double xMaxObj = envelope.getMaxX();
    	double yMinObj = envelope.getMinY();
    	double yMaxObj = envelope.getMaxY();
    	if (this.xMin == null || this.xMin > xMinObj) {
    		this.xMin = xMinObj;
    	}
    	if (this.xMax == null || this.xMax < xMaxObj) {
    		this.xMax = xMaxObj;
    	}
    	if (this.yMin == null || this.yMin > yMinObj) {
    		this.yMin = yMinObj;
    	}
    	if (this.yMax == null || this.yMax < yMaxObj) {
    		this.yMax = yMaxObj;
    	}
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
    
	/** Sets the attribute descriptors.
	 * Alternative to setting a model.
	 * @param attrDescs[]
	 */
	public void setAttributeDescriptors(AttributeDescriptor attrDescs[]) {
		this.attrDescs = new ArrayList<AttributeDescriptor>();
		for(AttributeDescriptor attrDesc : attrDescs) {
			if(attrDesc.getDbColumnGeomTypeName() != null) {
				iliGeomAttrName = attrDesc.getIomAttributeName();
			}
			this.attrDescs.add(attrDesc);
		}
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
