package ch.interlis.ioxwkf.json;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import ch.ehi.basics.types.OutParam;
import ch.ehi.ili2db.json.Iox2jsonUtility;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.CoordType;
import ch.interlis.ili2c.metamodel.LineType;
import ch.interlis.ili2c.metamodel.NumericType;
import ch.interlis.ili2c.metamodel.PolylineType;
import ch.interlis.ili2c.metamodel.SurfaceOrAreaType;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Type;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.iom.IomConstants;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;
import ch.interlis.iox_j.jts.Jts2iox;

class GeoJson2iox {
    public static final int LV95 = 1;
    public static final int LV03 = 2;
    protected JsonParser jg=null;
    protected TransferDescription td=null;
    private WKTReader wktReader=new WKTReader();

    public GeoJson2iox(JsonParser jg,TransferDescription td) throws IOException
    {
        this.jg=jg;
        this.td=td;
    }
    public ch.interlis.iom.IomObject read() throws IOException
    {
        return read(null,null);
    }
    public IomObject read(OutParam<String> bid, OutParam<String> topic) throws IOException 
    {
        JsonToken current = jg.currentToken();
        // before any tokens have been read?
        if(current==null) {
            current = jg.nextToken();
            // end of input?
            if(current==null) {
                return null;
            }
        }
        if(current!=JsonToken.START_OBJECT) {
            throw new IOException("unexpected json token "+current.toString()+"; '{' expected");
        }
        current = jg.nextToken();
        // "type":"Feature","id":"o1","geometry":null,"properties":{
        if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.TYPE.equals(jg.getCurrentName())) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.TYPE+"' expected");
        }
        current = jg.nextToken();
        if(current!=JsonToken.VALUE_STRING) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.FEATURE+"' expected");
        }
        String type=jg.getValueAsString();
        current = jg.nextToken();
        //if(isGeomType(type)) {
        //    IomObject coord=parseCoordinates(type);
        //    return coord;
        //}
        if(!Iox2geoJson.FEATURE.equals(type)) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.FEATURE+"' expected");
        }
        current = jg.currentToken();
        if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.ID.equals(jg.getCurrentName())) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.ID+"' expected");
        }
        current = jg.nextToken();
        if(current!=JsonToken.VALUE_STRING && current!=JsonToken.VALUE_NUMBER_FLOAT && current!=JsonToken.VALUE_NUMBER_INT) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+JsonToken.VALUE_STRING+"' expected");
        }
        String id=jg.getValueAsString();
        current = jg.nextToken();
        if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.GEOMETRY.equals(jg.getCurrentName())) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.GEOMETRY+"' expected");
        }
        current = jg.nextToken();
        IomObject geom=readGeometryObject();
        current = jg.currentToken();
        
        IomObject ret=new Iom_jObject("TAG",id);
        
        if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.PROPERTIES.equals(jg.getCurrentName())) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.PROPERTIES+"' expected");
        }
        current = jg.nextToken();
        if(current==JsonToken.START_OBJECT) {
            current = jg.nextToken();
            readProperties(ret, bid, topic, current);
            String className=ret.getobjecttag();
            Viewable aclass=(Viewable) td.getElement(className);
            if(aclass!=null) {
                AttributeDef geomAttr = Iox2geoJson.getGeometryAttr(aclass);
                if(geomAttr!=null && geom!=null) {
                    convertIomGeometry(geom,geomAttr);
                    ret.addattrobj(geomAttr.getName(), geom);
                }
            }
            current = jg.nextToken();
        }else if(current==JsonToken.VALUE_NULL) {
            current = jg.nextToken();
        }else {
            throw new IOException("unexpected json token "+current.toString()+"; '{' expected");
        }
        if(current!=JsonToken.END_OBJECT) {
            throw new IOException("unexpected json token "+current.toString()+"; '}' expected");
        }
        return ret;
    }
    private void convertIomGeometry(IomObject geomStruct, AttributeDef geomAttr) throws IOException {
         Type type = geomAttr.getDomainResolvingAll();
         CoordType coordType=null;
        if(type instanceof CoordType) {
            coordType=(CoordType)type;
        }else if(type instanceof LineType) {
            coordType=(CoordType)((LineType)type).getControlPointDomain().getType();
        }
        int destCrs=0;
        if(coordType!=null) {
            double x=((NumericType)coordType.getDimensions()[0]).getMinimum().doubleValue();
            double y=((NumericType)coordType.getDimensions()[1]).getMinimum().doubleValue();
            if(x>=460000.000 && x<=870000.000
                    && y>=45000.000 && y<=310000.000){
                // LV03
                destCrs=LV03;
            }else if( x>=2460000.000 && x<=2870000.000
            && y>=1045000.000 &&  y<=1310000.000) {
                // LV95 
                destCrs=LV95;
            }
        }
        if(destCrs!=0) {
            String geomTag=geomStruct.getobjecttag();
            if(geomTag.equals("MULTISURFACE")) {
                convertSurface(geomStruct,destCrs);
            }else if(geomTag.equals("POLYLINE")) {
                convertPolyline(geomStruct,destCrs);
            }else if(geomTag.equals("COORD")) {
                convertCoord(geomStruct,destCrs);
            }else {
                throw new IOException("unexpected geom obj "+geomTag);
            }
            
        }
    }
    private void convertSurface(IomObject obj, int destCrs) throws IOException {
        for(int surfacei=0;surfacei<obj.getattrvaluecount("surface");surfacei++){
            IomObject surface=obj.getattrobj("surface",surfacei);
            int boundaryc=surface.getattrvaluecount("boundary");
            for(int boundaryi=0;boundaryi<boundaryc;boundaryi++){
                IomObject boundary=surface.getattrobj("boundary",boundaryi);
                for(int polylinei=0;polylinei<boundary.getattrvaluecount("polyline");polylinei++){
                    IomObject polyline=boundary.getattrobj("polyline",polylinei);
                    convertPolyline(polyline,destCrs);
                }
            }            
        }
    }
    private void convertPolyline(IomObject polylineObj, int destCrs) throws IOException {
        for(int sequencei=0;sequencei<polylineObj.getattrvaluecount("sequence");sequencei++){
            IomObject sequence=polylineObj.getattrobj("sequence",sequencei);
            for(int segmenti=0;segmenti<sequence.getattrvaluecount("segment");segmenti++){
                IomObject segment=sequence.getattrobj("segment",segmenti);
                if(segment.getobjecttag().equals("COORD")){
                    convertCoord(segment,destCrs);
                }else if(segment.getobjecttag().equals("ARC")){
                    // ARC
                    convertCoord(segment,destCrs);
                    Coordinate ap=new Coordinate();
                    ap.x=Double.parseDouble(segment.getattrvalue("A1"));
                    ap.y=Double.parseDouble(segment.getattrvalue("A2"));
                    convertCoordinate(ap, destCrs);
                    segment.setattrvalue("A1", Double.toString(ap.x));
                    segment.setattrvalue("A2", Double.toString(ap.y));
                }else{
                    // custum line form
                    throw new IllegalArgumentException("custom line form not supported");
                }
            }
        }
    }
    private void convertCoord(IomObject segment, int destCrs) throws IOException {
        Coordinate coord;
        try {
            coord = Iox2jts.coord2JTS(segment);
        } catch (Iox2jtsException e) {
            throw new IOException(e);
        }
        convertCoordinate(coord, destCrs);
        IomObject ret=Jts2iox.JTS2coord(coord);
        segment.setattrvalue("C1", ret.getattrvalue("C1"));
        segment.setattrvalue("C2", ret.getattrvalue("C2"));
        if(ret.getattrvaluecount("C3")>0) {
            segment.setattrvalue("C3", ret.getattrvalue("C3"));
        }
    }
    private boolean isGeomType(String type) {
        if(Iox2geoJson.POINT.equals(type)) {
            return true;
        }else if(Iox2geoJson.LINESTRING.equals(type)) {
            return true;
        }else if(Iox2geoJson.POLYGON.equals(type)) {
            return true;
        }
        return false;
    }
    private IomObject parseCoordinates(String type, IomObject ret) throws IOException {
        if(Iox2geoJson.POINT.equals(type)) {
            ret=parsePointCoordinates(ret);
        }else if(Iox2geoJson.LINESTRING.equals(type)) {
            ret=parseLineStringCoordinates(ret);
        }else if(Iox2geoJson.POLYGON.equals(type)) {
            ret=parsePolygonCoordinates(ret);
        }else {
            throw new IllegalArgumentException("unexpected type "+type);
        }
        return ret;
    }
    private IomObject parsePolygonCoordinates(IomObject ret) throws IOException {
        JsonToken current = jg.currentToken();
        if(!current.equals(JsonToken.START_ARRAY)) {
            throw new IOException("unexpected json token "+current.toString()+"; '[' expected");
        }
        current = jg.nextToken();
        ret.setobjecttag("MULTISURFACE");
        IomObject surface=new ch.interlis.iom_j.Iom_jObject("SURFACE",null);
        ret.addattrobj("surface",surface);
        while(current.equals(JsonToken.START_ARRAY)) {
            IomObject polyline=parseLineStringCoordinates(new Iom_jObject(null,null));
            current = jg.currentToken();
            IomObject boundary=new ch.interlis.iom_j.Iom_jObject("BOUNDARY",null);
            surface.addattrobj("boundary",boundary);
            boundary.addattrobj("polyline", polyline);
        }
        if(!current.equals(JsonToken.END_ARRAY)) {
            throw new IOException("unexpected json token "+current.toString()+"; ']' expected");
        }
        current = jg.nextToken();
        return ret;
    }
    private IomObject parseLineStringCoordinates(IomObject ret) throws IOException {
        JsonToken current = jg.currentToken();
        ret.setobjecttag("POLYLINE");
        if(!current.equals(JsonToken.START_ARRAY)) {
            throw new IOException("unexpected json token "+current.toString()+"; '[' expected");
        }
        current = jg.nextToken();
        IomObject sequence=new ch.interlis.iom_j.Iom_jObject("SEGMENTS",null);
        ret.addattrobj("sequence",sequence);
        while(current.equals(JsonToken.START_ARRAY)) {
            IomObject coord=parsePointCoordinates(new Iom_jObject(null,null));
            current = jg.currentToken();
            sequence.addattrobj("segment", coord);
        }
        if(!current.equals(JsonToken.END_ARRAY)) {
            throw new IOException("unexpected json token "+current.toString()+"; ']' expected");
        }
        current = jg.nextToken();
        return ret;
    }
    // current is on [ after "coordinates"
    // function 
    // current is after ]
    private IomObject parsePointCoordinates(IomObject ret) throws IOException {
        JsonToken current = jg.currentToken();
        ret.setobjecttag("COORD");
        if(!current.equals(JsonToken.START_ARRAY)) {
            throw new IOException("unexpected json token "+current.toString()+"; '[' expected");
        }
        current = jg.nextToken();
        if(current!=JsonToken.VALUE_NUMBER_FLOAT && current!=JsonToken.VALUE_NUMBER_INT) {
            throw new IOException("unexpected json token "+current.toString()+"; number expected");
        }
        String c1=jg.getValueAsString();
        ret.setattrvalue("C1", c1);
        current = jg.nextToken();
        if(current!=JsonToken.VALUE_NUMBER_FLOAT && current!=JsonToken.VALUE_NUMBER_INT) {
            throw new IOException("unexpected json token "+current.toString()+"; number expected");
        }
        String c2=jg.getValueAsString();
        ret.setattrvalue("C2", c2);
        current = jg.nextToken();
        if(current==JsonToken.VALUE_NUMBER_FLOAT || current==JsonToken.VALUE_NUMBER_INT) {
            String c3=jg.getValueAsString();
            ret.setattrvalue("C3", c3);
            current = jg.nextToken();
        }
        if(!current.equals(JsonToken.END_ARRAY)) {
            throw new IOException("unexpected json token "+current.toString()+"; ']' expected");
        }
        current = jg.nextToken();
        return ret;
    }
    
    public static double DecToSexAngle(double dec) {
        int deg = (int) Math.floor(dec);
        int min = (int) Math.floor((dec - deg) * 60);
        double sec = (((dec - deg) * 60) - min) * 60;

        return sec + min*60.0 + deg*3600.0;
    }
    public static void convertCoordinate(Coordinate coord,int destCrs) throws IOException {
        if(destCrs!=0) {
            double lng=DecToSexAngle(coord.x);
            double lat=DecToSexAngle(coord.y);
            
            // Auxiliary values (% Bern)
            double lat_aux = (lat - 169028.66) / 10000.0;
            double lng_aux = (lng - 26782.5) / 10000.0;
            
            double x = ((200147.07 + (308807.95 * lat_aux)
                    + (3745.25 * Math.pow(lng_aux, 2)) + (76.63 * Math.pow(lat_aux,
                    2))) - (194.56 * Math.pow(lng_aux, 2) * lat_aux))
                    + (119.79 * Math.pow(lat_aux, 3));
            
            double y = (600072.37 + (211455.93 * lng_aux))
                    - (10938.51 * lng_aux * lat_aux)
                    - (0.36 * lng_aux * Math.pow(lat_aux, 2))
                    - (44.54 * Math.pow(lng_aux, 3));
            if(destCrs==LV95) {
                coord.x=y+2000000.0;
                coord.y=x+1000000.0;
            }else {
                coord.x=y;
                coord.y=x;
            }
        }
    }
    private IomObject readGeometryObject() throws IOException {
/* "type": "Point",
           "Point", "MultiPoint", "LineString",
           "MultiLineString", "Polygon", "MultiPolygon", and
           "GeometryCollection"
      
    "coordinates": [102.0, 0.5]
    "coordinates": [
                   [102.0, 0.0],
                   [103.0, 1.0],
                   [104.0, 0.0],
                   [105.0, 1.0]
               ]        
*/        
        JsonToken current = jg.currentToken();
        if(current==JsonToken.VALUE_NULL) {
            current = jg.nextToken();
            return null;
        }
        if(current!=JsonToken.START_OBJECT) {
            throw new IOException("unexpected json token "+current.toString()+"; '{' expected");
        }
        current = jg.nextToken();
        if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.TYPE.equals(jg.getCurrentName())) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.TYPE+"' expected");
        }
        current = jg.nextToken();
        if(current!=JsonToken.VALUE_STRING) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.FEATURE+"' expected");
        }
        String type=jg.getValueAsString();
        current = jg.nextToken();
        if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.COORDINATES.equals(jg.getCurrentName())) {
            throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.COORDINATES+"' expected");
        }
        current = jg.nextToken();
        IomObject geom=parseCoordinates(type,new Iom_jObject(null,null));
        current = jg.currentToken();
        
        if(current!=JsonToken.END_OBJECT) {
            throw new IOException("unexpected json token "+current.toString()+"; '}' expected");
        }
        current = jg.nextToken();
        return geom;
    }
    // current is after {
    // function
    // current is on }
    private void readProperties(IomObject ret, OutParam<String> bid, OutParam<String> topic, JsonToken current)
            throws IOException {
        while(current==JsonToken.FIELD_NAME) {
            String propName=jg.getCurrentName();
            current = jg.nextToken();
            String propValue=null;
            if(current==JsonToken.VALUE_TRUE) {
                propValue= "true";
                current = jg.nextToken();
            }else if(current==JsonToken.VALUE_FALSE) {
                propValue= "false";
                current = jg.nextToken();
            }else if(current==JsonToken.VALUE_NULL) {
                current = jg.nextToken();
            }else if(current==JsonToken.VALUE_NUMBER_FLOAT || current==JsonToken.VALUE_NUMBER_INT || current==JsonToken.VALUE_STRING) {
                propValue=jg.getValueAsString();
                current = jg.nextToken();
            }else if(current==JsonToken.START_OBJECT) {
                current = jg.nextToken();
                IomObject structEle=new Iom_jObject(null,null);
                readProperties(structEle,null,null,current);
                ret.addattrobj(propName, structEle);
                current = jg.nextToken();
            }else if(current==JsonToken.START_ARRAY) {
                current = jg.nextToken();
                while(current==JsonToken.START_OBJECT) {
                    current = jg.nextToken();
                    IomObject structEle=new Iom_jObject(null,null);
                    readProperties(structEle,null,null,current);
                    ret.addattrobj(propName, structEle);
                    current = jg.nextToken();
                }
                if(current!=JsonToken.END_ARRAY) {
                    throw new IOException("unexpected json token "+jg.currentToken().toString()+"; ']' expected");
                }
                current = jg.nextToken();
            }else {
                throw new IOException("unexpected json token "+jg.currentToken().toString());
            }
            if(propValue!=null) {
                
                if (propName.equals(Iox2jsonUtility.CONSISTENCY)) {
                    if(propValue.equals(Iox2jsonUtility.CONSISTENCY_ADAPTED)) {
                        ret.setobjectconsistency(IomConstants.IOM_ADAPTED);
                    }else if(propValue.equals(Iox2jsonUtility.CONSISTENCY_INCOMPLETE)) {
                        ret.setobjectconsistency(IomConstants.IOM_INCOMPLETE);
                    }else if(propValue.equals(Iox2jsonUtility.CONSISTENCY_INCONSISTENT)) {
                        ret.setobjectconsistency(IomConstants.IOM_INCONSISTENT);
                    }else {
                        throw new IOException("unexpected consistency value "+propValue);
                    }
                } else if (propName.equals(Iox2jsonUtility.OPERATION)) {
                    if(propValue.equals(Iox2jsonUtility.OPERATION_DELETE)) {
                        ret.setobjectoperation(IomConstants.IOM_OP_DELETE);
                    }else if(propValue.equals(Iox2jsonUtility.OPERATION_UPDATE)) {
                        ret.setobjectoperation(IomConstants.IOM_OP_UPDATE);
                    }else {
                        throw new IOException("unexpected operation value "+propValue);
                    }
                } else if (propName.equals(Iox2jsonUtility.ORDERPOS)) {
                    ret.setobjectreforderpos(Long.parseLong(propValue));
                } else if (propName.equals(Iox2jsonUtility.REF)) {
                    ret.setobjectrefoid(propValue);
                } else if (propName.equals(Iox2jsonUtility.REFBID)) {
                    ret.setobjectrefbid(propValue);
                } else if (propName.equals(Iox2jsonUtility.TYPE)) {
                    ret.setobjecttag(propValue);
                } else if (propName.equals(Iox2jsonUtility.BID)) {
                    if(bid!=null) {
                        bid.value=propValue;
                    }
                } else if (propName.equals(Iox2jsonUtility.TOPIC)) {
                    if(topic!=null) {
                        topic.value=propValue;
                    }
                } else if (propName.equals(Iox2geoJson.TYPE)) {
                    String type=propValue;
                    if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.COORDINATES.equals(jg.getCurrentName())) {
                        throw new IOException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.COORDINATES+"' expected");
                    }
                    current = jg.nextToken();
                    parseCoordinates(type,ret);
                    current = jg.currentToken();
                    
                }else {
                    ret.setattrvalue(propName, propValue);
                }
            }
        }
    }
    
}
