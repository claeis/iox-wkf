package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Types;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;
import ch.interlis.ioxwkf.shp.ShapeWriter;

/**<b>Db2Shp</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * write data of data base with any IoxWriter, converted from PostGis to Interlis dataTypes and export converted data to SHP file.<br>
 * <p>
 * 
 * <b>Create a new Db2Shp</b><br>
 * <li>Create an Db2Shp object. Db2Csv extends AbstractExport2db class.</li>
 * <p>
 * 
 * <b>AttributeDescriptor possibilities</b><br>
 * {@link ch.interlis.ioxwkf.dbtools.AttributeDescriptor}<br>
 * <p>
 * 
 * <b>Setting possibilities</b><br>
 * {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}<br>
 * <p>
 * 
 * <b>Attachement</b><br>
 * <li><a href="https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf">Shapespecification</a></li>
 * <li><a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
 */
public class Db2Shp extends AbstractExportFromdb {
	/** Create the ShpWriter and return created IoxWriter.<br>
	 * <p>
	 * Set File (Mandatory)<br>
	 * The file to write to.
	 * <p>
	 * 
	 * Path has to exist.<br>
	 * File file=new File("C:\file.shp");<br>
	 * Db2Shp shpExport= new Db2Shp();<br>
	 * shpExport.exportData(file,"Connection", config);
	 * <p>
	 * 
	 * Setting possibilities:<br>
	 * <li>Setting possibilities<br>
	 *	   {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
	 * </li>
	 * @param file
	 * @param config
	 * @param dbColumns[]
	 * @exception IoxException
	 * @return IoxWriter
	 */
	@Override
	protected IoxWriter createWriter(File file, Settings config, AttributeDescriptor dbColumns[]) throws IoxException {
		// mandatory: file to reader has not to be null.
		if(file!=null) {
			EhiLogger.logState("file to write to: <"+file.getName()+">");
		}else {
			throw new IoxException("file==null.");
		}
		
		// create and return a shape writer.
		ShapeWriter writer=new ShapeWriter(file,config);
		org.opengis.feature.type.AttributeDescriptor attrDescs[]=new org.opengis.feature.type.AttributeDescriptor[dbColumns.length];
		for(int i=0;i<dbColumns.length;i++) {
			org.geotools.feature.AttributeTypeBuilder attributeBuilder=new org.geotools.feature.AttributeTypeBuilder();
			String attrName=dbColumns[i].getIomAttributeName();
			attributeBuilder.setName(attrName);
			int dbColType=dbColumns[i].getDbColumnType();
			if(dbColumns[i].isGeometry()) {
				String geoColumnTypeName=dbColumns[i].getDbColumnGeomTypeName();
				if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
					attributeBuilder.setBinding(Point.class);
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOINT)) {
					attributeBuilder.setBinding(MultiPoint.class);
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
					attributeBuilder.setBinding(LineString.class);
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTILINESTRING)) {
					attributeBuilder.setBinding(MultiLineString.class);
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
					attributeBuilder.setBinding(Polygon.class);
				}else if(geoColumnTypeName.equals(AttributeDescriptor.GEOMETRYTYPE_MULTIPOLYGON)) {
					attributeBuilder.setBinding(MultiPolygon.class);
				}else {
					throw new IllegalStateException("unexpected geometry type "+geoColumnTypeName);
				}
				CoordinateReferenceSystem crs = null;
				
				int srsId=dbColumns[i].getSrId();
				CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
				try {
					crs = factory.createCoordinateReferenceSystem("EPSG:"+srsId);
				} catch (NoSuchAuthorityCodeException e) {
					throw new IoxException("coordinate reference: EPSG:"+srsId+" not found",e);
				} catch (FactoryException e) {
					throw new IoxException(e);
				}
				attributeBuilder.setCRS(crs);
			}else if(dbColType==Types.SMALLINT) {
				attributeBuilder.setBinding(Integer.class);
			}else if(dbColType==Types.TINYINT) {
				attributeBuilder.setBinding(Integer.class);
			}else if(dbColType==Types.INTEGER) {
				attributeBuilder.setBinding(Integer.class);
			}else if(dbColType==Types.NUMERIC) {
				attributeBuilder.setBinding(Double.class);
			}else if(dbColType==Types.BIGINT) {
				attributeBuilder.setBinding(Double.class);
			}else if(dbColType==Types.FLOAT) {
				attributeBuilder.setBinding(Double.class);
			}else if(dbColType==Types.DOUBLE) {
				attributeBuilder.setBinding(Double.class);
			}else if(dbColType==Types.DATE) {
				attributeBuilder.setBinding(java.util.Date.class);
			}else {
				attributeBuilder.setBinding(String.class);
			}
			attributeBuilder.setMinOccurs(0);
			attributeBuilder.setMaxOccurs(1);
			attributeBuilder.setNillable(true);
			attrDescs[i]=attributeBuilder.buildDescriptor(attrName);
		}
		writer.setAttributeDescriptors(attrDescs);
		return writer;
	}
}