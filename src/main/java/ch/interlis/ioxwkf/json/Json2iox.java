package ch.interlis.ioxwkf.json;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import ch.ehi.basics.types.OutParam;
import ch.ehi.ili2db.json.Iox2jsonUtility;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomConstants;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox_j.jts.Jts2iox;

class Json2iox {
    protected JsonParser jg=null;
    protected TransferDescription td=null;
    private WKTReader wktReader=new WKTReader();

    public Json2iox(JsonParser jg,TransferDescription td) throws IOException
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
        IomObject ret=new Iom_jObject("TAG",null);
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
                IomObject structEle=read();
                ret.addattrobj(propName, structEle);
                current = jg.nextToken();
            }else if(current==JsonToken.START_ARRAY) {
                current = jg.nextToken();
                while(current==JsonToken.START_OBJECT) {
                    IomObject structEle=read();
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
                } else if (propName.equals(Iox2jsonUtility.TID)) {
                    ret.setobjectoid(propValue);
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
                }else {
                    if(propValue.startsWith("POINT")) {
                        Geometry geom;
                        try {
                            geom = wktReader.read(propValue);
                        } catch (ParseException e) {
                            throw new IOException(e);
                        }
                        IomObject coord=Jts2iox.JTS2coord(geom.getCoordinate());
                        ret.addattrobj(propName, coord);
                    }else if(propValue.startsWith("LINESTRING")) {
                        Geometry geom;
                        try {
                            geom = wktReader.read(propValue);
                        } catch (ParseException e) {
                            throw new IOException(e);
                        }
                        IomObject polyline=Jts2iox.JTS2polyline((LineString)geom);
                        ret.addattrobj(propName, polyline);
                    }else if(propValue.startsWith("POLYGON")) {
                        Geometry geom;
                        try {
                            geom = wktReader.read(propValue);
                        } catch (ParseException e) {
                            throw new IOException(e);
                        }
                        IomObject polyline=Jts2iox.JTS2surface((Polygon)geom);
                        ret.addattrobj(propName, polyline);
                    }else {
                        ret.setattrvalue(propName, propValue);
                    }
                }
            }
        }
        if(current!=JsonToken.END_OBJECT) {
            throw new IOException("unexpected json token "+jg.currentToken().toString()+"; '}' expected");
        }
        return ret;
    }
    
}
