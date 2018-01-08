package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.sql.Types;
import java.util.List;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;
import ch.interlis.ioxwkf.shp.ShapeWriter;

/** create a ShapeWriter.
 */
public class Db2Shp extends AbstractExportFromdb {
	/** create the ShpWriter and return created IoxWriter.
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