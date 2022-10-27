package ch.interlis.ioxwkf.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTWriter;

import ch.ehi.ili2db.json.Iox2json;
import ch.ehi.ili2db.json.Iox2jsonUtility;
import ch.interlis.ili2c.metamodel.AbstractSurfaceOrAreaType;
import ch.interlis.ili2c.metamodel.AssociationDef;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.CompositionType;
import ch.interlis.ili2c.metamodel.CoordType;
import ch.interlis.ili2c.metamodel.EnumerationType;
import ch.interlis.ili2c.metamodel.LineType;
import ch.interlis.ili2c.metamodel.LocalAttribute;
import ch.interlis.ili2c.metamodel.MultiPolylineType;
import ch.interlis.ili2c.metamodel.NumericType;
import ch.interlis.ili2c.metamodel.ObjectType;
import ch.interlis.ili2c.metamodel.PolylineType;
import ch.interlis.ili2c.metamodel.RoleDef;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Type;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.ili2c.metamodel.ViewableTransferElement;
import ch.interlis.iom.IomConstants;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;

class Iox2geoJson extends Iox2json {
    public static final String PROPERTIES = "properties";
    public static final String GEOMETRY = "geometry";
    public static final String ID = "id";
    public static final String FEATURE = "Feature";
    public static final String POLYGON = "Polygon";
    public static final String TYPE = "type";
    public static final String LINESTRING = "LineString";
    public static final String POINT = "Point";
    public static final String COORDINATES = "coordinates";
    public static final String FEATURE_COLLECTION = "FeatureCollection";
    public static final String FEATURES = "features";
    public Iox2geoJson(JsonGenerator jg,TransferDescription td) throws IOException
    {
        super(jg,td);
    }
    @Override
    public void write(ch.interlis.iom.IomObject obj) throws IOException
    {
        super.write(obj,null,null);
    }
    @Override
    public void write(ch.interlis.iom.IomObject obj,String bid,String topic) throws IOException
    {
        jg.writeStartObject();
        jg.writeStringField(TYPE,FEATURE);
        String oid=obj.getobjectoid();
        if(oid!=null){
            jg.writeStringField(ID,oid);
        }
        
        String className=obj.getobjecttag();
        Viewable aclass=null;
        if(td!=null) {
            aclass=(Viewable) td.getElement(className);
        }
        
        jg.writeFieldName(GEOMETRY);
        String geomAttrName=null;
        AttributeDef geomAttr=null;
        IomObject geomStruct=null;
        if(aclass!=null) {
            geomAttr=getGeometryAttr(aclass);
        }
        if(geomAttr!=null){
            geomAttrName=geomAttr.getName();
            geomStruct=obj.getattrobj(geomAttrName,0);
        }
        if(geomStruct==null) {
            jg.writeNull();
        }else {
            Type type=geomAttr.getDomainResolvingAll();
            if(type instanceof CoordType) {
                Coordinate geom;
                try {
                    geom = Iox2jts.coord2JTS(geomStruct);
                } catch (Iox2jtsException e) {
                    throw new IOException(e);
                }
                writeGeoJsonPoint(jg,geom);
            }else if(type instanceof AbstractSurfaceOrAreaType) {
                Polygon geom;
                try {
                    geom = Iox2jts.surface2JTS(geomStruct, ((AbstractSurfaceOrAreaType)type).getP());
                } catch (Iox2jtsException e) {
                    throw new IOException(e);
                }
                writeGeoJsonPolygon(jg,geom);
            }else if(type instanceof PolylineType || type instanceof MultiPolylineType) {
                LineString geom;
                try {
                    geom = Iox2jts.polyline2JTSlineString(geomStruct,false,((LineType)type).getP());
                } catch (Iox2jtsException e) {
                    throw new IOException(e);
                }
                writeGeoJsonLineString(jg,geom);
            }
        }
        jg.writeFieldName(PROPERTIES);
        jg.writeStartObject();
        
        
        jg.writeStringField(Iox2jsonUtility.TYPE,className);
        if(bid!=null && bid.length()>0){
            jg.writeStringField(Iox2jsonUtility.BID,bid);
        }
        if(topic!=null && topic.length()>0){
            jg.writeStringField(Iox2jsonUtility.TOPIC,topic);
        }
        int op = obj.getobjectoperation();
        if(op==IomConstants.IOM_OP_DELETE) {
            jg.writeStringField(Iox2jsonUtility.OPERATION,Iox2jsonUtility.OPERATION_DELETE);
        }else if(op==IomConstants.IOM_OP_UPDATE) {
            jg.writeStringField(Iox2jsonUtility.OPERATION,Iox2jsonUtility.OPERATION_UPDATE);
        }
        int consistency = obj.getobjectconsistency();
        if(consistency==IomConstants.IOM_INCONSISTENT) {
            jg.writeStringField(Iox2jsonUtility.CONSISTENCY,Iox2jsonUtility.CONSISTENCY_INCONSISTENT);
        }else if(consistency==IomConstants.IOM_INCOMPLETE) {
            jg.writeStringField(Iox2jsonUtility.CONSISTENCY,Iox2jsonUtility.CONSISTENCY_INCOMPLETE);
        }else if(consistency==IomConstants.IOM_ADAPTED) {
            jg.writeStringField(Iox2jsonUtility.CONSISTENCY,Iox2jsonUtility.CONSISTENCY_ADAPTED);
        }
        long orderpos = obj.getobjectreforderpos();
        if(orderpos!=0) {
            jg.writeNumberField(Iox2jsonUtility.ORDERPOS,orderpos);
        }
        String refbid = obj.getobjectrefbid();
        if(refbid!=null){
            jg.writeStringField(Iox2jsonUtility.REFBID,refbid);
        }
        String refoid = obj.getobjectrefoid();
        if(refoid!=null){
            jg.writeStringField(Iox2jsonUtility.REF,refoid);
        }
        
        if(aclass!=null) {
            Iterator attri=aclass.getAttributesAndRoles2();
            while(attri.hasNext()) {
                ViewableTransferElement propDef = (ViewableTransferElement) attri.next();
                if (propDef.obj instanceof AttributeDef) {
                    AttributeDef attr = (AttributeDef) propDef.obj;
                    if(!attr.isTransient()){
                        Type proxyType=attr.getDomain();
                        if(proxyType!=null && (proxyType instanceof ObjectType)){
                            // skip implicit particles (base-viewables) of views
                        }else{
                            String propName=attr.getName();
                            if(attr.isDomainBoolean()) {
                                writeBooleanAttr(obj, propName);
                            }else{
                                Type type=attr.getDomainResolvingAll();
                                if(type instanceof CompositionType) {
                                    writeStructAttr( obj,  propName);
                                }else if(type instanceof NumericType) {
                                    writeNumericAttr( obj, propName);
                                }else if(type instanceof CoordType) {
                                    if(geomAttrName==null || !geomAttrName.equals(propName)) {
                                        writeCoordAttr( obj, propName);
                                    }
                                }else if(type instanceof AbstractSurfaceOrAreaType) {
                                    if(geomAttrName==null || !geomAttrName.equals(propName)) {
                                        writeSurfaceAttr( obj, propName,(AbstractSurfaceOrAreaType) type);
                                    }
                                }else if(type instanceof PolylineType || type instanceof MultiPolylineType) {
                                    if(geomAttrName==null || !geomAttrName.equals(propName)) {
                                        writeLineAttr(obj, propName,(LineType)type);
                                    }
                                }else {
                                    writeStringAttr( obj, propName);
                                }
                            }
                        }
                    }
                }else if(propDef.obj instanceof RoleDef){
                    RoleDef role = (RoleDef) propDef.obj;
                    AssociationDef roleOwner = (AssociationDef) role.getContainer();
                    if (roleOwner.getDerivedFrom() == null) {
                        String propName=role.getName();
                        writeStructAttr(obj, propName);
                    }
                }else {
                    throw new IllegalStateException("unexpected property "+propDef.obj);
                }
            }
        }else {
            boolean isNumeric=false;
            if(className.equals("COORD") || className.equals("ARC")) {
                isNumeric=true;
            }
            int attrc = obj.getattrcount();
            String propNames[]=new String[attrc];
            for(int i=0;i<attrc;i++){
                   propNames[i]=obj.getattrname(i);
            }
            java.util.Arrays.sort(propNames);
            for(int i=0;i<attrc;i++){
               String propName=propNames[i];
                int propc=obj.getattrvaluecount(propName);
                if(propc>0){
                    jg.writeFieldName(propName);
                    if(propc>1){
                        jg.writeStartArray();
                    }
                    for(int propi=0;propi<propc;propi++){
                        String value=obj.getattrprim(propName,propi);
                        if(value!=null){
                            if(isNumeric) {
                                jg.writeNumber(value);
                            }else {
                                jg.writeString(value);
                            }
                        }else{
                            IomObject structvalue=obj.getattrobj(propName,propi);
                            write(structvalue);
                        }
                    }
                    if(propc>1){
                        jg.writeEndArray();
                    }
                }
            }
            
        }
        jg.writeEndObject(); // properties
        jg.writeEndObject(); // feature
    }
    private static void writeGeoJsonPoint(JsonGenerator jg,Coordinate geom) throws IOException {
        jg.writeStartObject();
        jg.writeStringField(TYPE,POINT);
        jg.writeFieldName(COORDINATES);
        writeCoordinates(jg, geom);
        jg.writeEndObject();
    }
    private static void writeGeoJsonLineString(JsonGenerator jg,LineString geom) throws IOException {
        jg.writeStartObject();
        jg.writeStringField(TYPE,LINESTRING);
        jg.writeFieldName(COORDINATES);
        witeLineStringCoordinates(jg, geom);
        jg.writeEndObject();
    }
    private static void writeGeoJsonPolygon(JsonGenerator jg,Polygon geom) throws IOException {
        jg.writeStartObject();
        jg.writeStringField(TYPE,POLYGON);
        jg.writeFieldName(COORDINATES);
        jg.writeStartArray();
        {
            LineString shell=geom.getExteriorRing();
            witeLineStringCoordinates(jg, shell);
        }
        for(int i=0;i<geom.getNumInteriorRing();i++) {
            LineString hole=geom.getInteriorRingN(i);
            witeLineStringCoordinates(jg, hole);
        }
        jg.writeEndArray();
        jg.writeEndObject();
    }
    private static void witeLineStringCoordinates(JsonGenerator jg, LineString shell) throws IOException {
        jg.writeStartArray();
        for(Coordinate coord:shell.getCoordinates()) {
            writeCoordinates(jg, coord);
        }
        jg.writeEndArray();
    }
    private static void writeCoordinates(JsonGenerator jg, Coordinate coord) throws IOException {
        convertCoordinate(coord);
        double x=coord.x;
        double y=coord.y;
            
        jg.writeStartArray();
        jg.writeNumber(x);
        jg.writeNumber(y);
        jg.writeEndArray();
    }
    public static void convertCoordinate(Coordinate coord) throws IOException {
        double x=coord.x;
        double y=coord.y;
        if(x>=460000.000 && x<=870000.000
            && y>=45000.000 && y<=310000.000){
            // LV03
            // Bern 600000.000/200000.000
            double y_aux = (x - 600000.0) / 1000000.0;
            double x_aux = (y - 200000.0) / 1000000.0;

            // Process lat
            double lat = (16.9023892 + (3.238272 * x_aux))
                    - (0.270978 * Math.pow(y_aux, 2))
                    - (0.002528 * Math.pow(x_aux, 2))
                    - (0.0447 * Math.pow(y_aux, 2) * x_aux)
                    - (0.0140 * Math.pow(x_aux, 3));

            // Unit 10000" to 1 " and converts seconds to degrees (dec)
            lat = (lat * 100.0) / 36.0;
            double lng = (2.6779094 + (4.728982 * y_aux)
                    + (0.791484 * y_aux * x_aux) + (0.1306 * y_aux * Math.pow(
                    x_aux, 2))) - (0.0436 * Math.pow(y_aux, 3));

            // Unit 10000" to 1 " and converts seconds to degrees (dec)
            lng = (lng * 100.0) / 36.0;
            
            coord.x=lng;
            coord.y=lat;
        }else if( x>=2460000.000 && x<=2870000.000
            && y>=1045000.000 &&  y<=1310000.000) {
            // LV95 
            // Bern 2600000.000/1200000.000
            double y_aux = (x - 2600000.0) / 10000000.0;
            double x_aux = (y - 1200000.0) / 10000000.0;

            // Process lat
            double lat = (16.9023892 + (3.238272 * x_aux))
                    - (0.270978 * Math.pow(y_aux, 2))
                    - (0.002528 * Math.pow(x_aux, 2))
                    - (0.0447 * Math.pow(y_aux, 2) * x_aux)
                    - (0.0140 * Math.pow(x_aux, 3));

            // Unit 10000" to 1 " and converts seconds to degrees (dec)
            lat = (lat * 100) / 36;
            double lng = (2.6779094 + (4.728982 * y_aux)
                    + (0.791484 * y_aux * x_aux) + (0.1306 * y_aux * Math.pow(
                    x_aux, 2))) - (0.0436 * Math.pow(y_aux, 3));

            // Unit 10000" to 1 " and converts seconds to degrees (dec)
            lng = (lng * 100) / 36;
            coord.x=lng;
            coord.y=lat;
        }
    }
    static public AttributeDef getGeometryAttr(Viewable aclass) {
        Iterator viewableIter=aclass.getAttributes();
        while(viewableIter.hasNext()) {
            Object attrObj=viewableIter.next();
            if(attrObj instanceof LocalAttribute) {
                LocalAttribute attr= (LocalAttribute)attrObj;
                Type type=attr.getDomainResolvingAll();
                if(type instanceof CoordType) {
                    return attr;
                }else if(type instanceof AbstractSurfaceOrAreaType) {
                    return attr;
                }else if(type instanceof PolylineType || type instanceof MultiPolylineType) {
                    return attr;
                }
            }
        }
        return null;
    }
    @Override
    protected void writeCoordAttr(ch.interlis.iom.IomObject obj, 
            String propName) throws IOException {
        IomObject geomStruct=obj.getattrobj(propName,0);
        String geomTxt=null;
        if(geomStruct!=null) {
            Coordinate geom;
            try {
                geom = Iox2jts.coord2JTS(geomStruct);
            } catch (Iox2jtsException e) {
                throw new IOException(e);
            }
            jg.writeFieldName(propName);
            writeGeoJsonPoint(jg,geom);
        }
    }
    @Override
    protected void writeLineAttr(ch.interlis.iom.IomObject obj,
            String propName,LineType type) throws IOException {
        IomObject geomStruct=obj.getattrobj(propName,0);
        String geomTxt=null;
        if(geomStruct!=null) {
            LineString geom;
            try {
                geom = Iox2jts.polyline2JTSlineString(geomStruct,false,type.getP());
            } catch (Iox2jtsException e) {
                throw new IOException(e);
            }
            jg.writeFieldName(propName);
            writeGeoJsonLineString(jg,geom);
        }
    }
    @Override
    protected void writeSurfaceAttr(ch.interlis.iom.IomObject obj, 
            String propName,AbstractSurfaceOrAreaType type) throws IOException {
        IomObject geomStruct=obj.getattrobj(propName,0);
        String geomTxt=null;
        if(geomStruct!=null) {
            Polygon geom;
            try {
                geom = Iox2jts.surface2JTS(geomStruct, type.getP());
            } catch (Iox2jtsException e) {
                throw new IOException(e);
            }
            jg.writeFieldName(propName);
            writeGeoJsonPolygon(jg,geom);
        }
    }
}
