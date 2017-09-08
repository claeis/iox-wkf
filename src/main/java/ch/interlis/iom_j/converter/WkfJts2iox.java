package ch.interlis.iom_j.converter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import ch.interlis.iom.IomObject;
import ch.interlis.iox_j.jts.Jts2iox;

public class WkfJts2iox {
	
	public static final String MULTISURFACE = "MULTISURFACE";
	public static final String MULTISURFACE_SURFACE = "surface";
	public static final String MULTIPOLYLINE = "MULTIPOLYLINE";
	public static final String MULTIPOLYLINE_POLYLINE = "polyline";
	public static final String MULTICOORD = "MULTICOORD";
	public static final String MULTICOORD_COORD = "coord";
	/** Converts from a MultiPoint to a INTERLIS MULTICOORD.
	 * @param value JTS MultiPoint.
	 * @return INTERLIS MULTICOORD structure
	 */
	static public  IomObject JTS2multicoord(com.vividsolutions.jts.geom.MultiPoint value) 
	{
		IomObject ret=new ch.interlis.iom_j.Iom_jObject(MULTICOORD,null);
		int numberOfCoords=value.getNumGeometries();
		for(int multicoordi=0;multicoordi<numberOfCoords;multicoordi++) {
			Geometry geoObj=value.getGeometryN(multicoordi);
			Coordinate coordObj = (Coordinate)geoObj.getCoordinate();
			ret.addattrobj(MULTICOORD_COORD, Jts2iox.JTS2coord(coordObj));
		}
		return ret;
	}
	/** Converts from a MultiLineString to a INTERLIS MULTIPOLYLINE.
	 * @param value JTS MultiLineString
	 * @return INTERLIS MULTIPOLYLINE structure
	 */
	static public  IomObject JTS2multipolyline(com.vividsolutions.jts.geom.MultiLineString value) 
	{
		IomObject ret=new ch.interlis.iom_j.Iom_jObject(MULTIPOLYLINE,null);
		int numberOfLines=value.getNumGeometries();
		for(int multilinei=0;multilinei<numberOfLines;multilinei++) {
			Geometry geoObj=value.getGeometryN(multilinei);
			LineString lineStringObj = (LineString)geoObj;
			ret.addattrobj(MULTIPOLYLINE_POLYLINE, Jts2iox.JTS2polyline(lineStringObj));
		}
		return ret;
	}
	/** Converts from a MultiPolygon to a INTERLIS MULTISURFACE.
	 * @param value JTS Polygon
	 * @return INTERLIS MULTISURFACE structure
	 */
	static public  IomObject JTS2multisurface(com.vividsolutions.jts.geom.MultiPolygon value) 
	{
		IomObject ret=new ch.interlis.iom_j.Iom_jObject(MULTISURFACE,null);
		int numberOfSurfaces=value.getNumGeometries();
		for(int multiSurface=0;multiSurface<numberOfSurfaces;multiSurface++) {
			Geometry geoObj=value.getGeometryN(multiSurface);
			Polygon polygonObj = (Polygon)geoObj;
			IomObject ioxMultiSurface=Jts2iox.JTS2surface(polygonObj);
			IomObject surface=ioxMultiSurface.getattrobj(MULTISURFACE_SURFACE, 0);
			ret.addattrobj(MULTISURFACE_SURFACE, surface);
		}
		return ret;
	}
}