package ch.interlis.ioxwkf.json;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import ch.ehi.basics.settings.Settings;
import ch.ehi.basics.types.OutParam;
import ch.interlis.ili2c.metamodel.DataModel;
import ch.interlis.ili2c.metamodel.LocalAttribute;
import ch.interlis.ili2c.metamodel.Topic;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.EndBasketEvent;
import ch.interlis.iox.EndTransferEvent;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.StartBasketEvent;
import ch.interlis.iox.StartTransferEvent;
import ch.interlis.iox_j.ObjectEvent;

public class GeoJsonReader implements ch.interlis.iox.IoxReader {
	private TransferDescription td=null;
	private java.io.Reader reader=null;
    private JsonParser jg=null;
    private GeoJson2iox json2iox=null;
    private String currentBid=null;
    private String currentTopic=null;
    private IomObject currentObj=null;
    private static final int START=0;
    private static final int INSIDE_TRANSFER=1;
    private static final int INSIDE_BASKET=2;
    private static final int INSIDE_OBJECT=3;
    private static final int END_BASKET=4;
    private static final int END_TRANSFER=5;
    private static final int END=6;
    private int state=START;
    private HashSet<String> readBids=new HashSet<String>();
    
	public GeoJsonReader(File file)throws IoxException{
		this(file,null);
	}
	public GeoJsonReader(File file,Settings settings)throws IoxException{
		if(file!=null) {
			try {
				reader=new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(file),"UTF-8"));
			} catch (IOException e) {
				throw new IoxException("could not create file",e);
			}
			if(reader!=null) {
		        JsonFactory jsonF = new JsonFactory();
		        try {
                    jg = jsonF.createJsonParser(reader);
                } catch (IOException e) {
                    throw new IoxException("failed to create JsonParser",e);
                }
			}
		}
	}
	    
    
    /** writes objectvalues of objects via iox-Events
     */
    @Override
	public IoxEvent read() throws IoxException {
        if(state==END) {
            return  null;
        }
        JsonToken current = jg.currentToken();
        // before any tokens have been read?
        if(current==null) {
            try {
                current = jg.nextToken();
            } catch (IOException e) {
                throw new IoxException(e);
            }
            // end of input?
            if(current==null) {
                return null;
            }
        }
        if(state==START) {
            if(current!=JsonToken.START_OBJECT) {
                throw new IoxException("unexpected json token "+jg.currentToken().toString()+"; '{' expected");
            }
            if(td!=null) {
                try {
                    json2iox=new GeoJson2iox(jg,td);
                } catch (IOException e) {
                    throw new IoxException(e);
                }
            }
            try {
                // "type":"FeatureCollection","features":[]
                current = jg.nextToken();
                if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.TYPE.equals(jg.getCurrentName())) {
                    throw new IoxException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.TYPE+"' expected");
                }
                current = jg.nextToken();
                if(current!=JsonToken.VALUE_STRING || !Iox2geoJson.FEATURE_COLLECTION.equals(jg.getValueAsString())) {
                    throw new IoxException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.FEATURE_COLLECTION+"' expected");
                }
                current = jg.nextToken();
                if(current!=JsonToken.FIELD_NAME || !Iox2geoJson.FEATURES.equals(jg.getCurrentName())) {
                    throw new IoxException("unexpected json token "+current.toString()+"; '"+Iox2geoJson.FEATURES+"' expected");
                }
                current = jg.nextToken();
                if(current!=JsonToken.START_ARRAY) {
                    throw new IoxException("unexpected json token "+jg.currentToken().toString()+"; '[' expected");
                }
                current = jg.nextToken();
            } catch (IOException e) {
                throw new IoxException(e);
            }
            state=INSIDE_TRANSFER;
            StartTransferEvent event=new ch.interlis.iox_j.StartTransferEvent();
            return event;
        }else if(state==INSIDE_TRANSFER) {
            if(current==JsonToken.START_OBJECT) {
                OutParam<String> bid=new OutParam<String>();
                OutParam<String> topic=new OutParam<String>();
                try {
                    currentObj=json2iox.read(bid,topic);
                    current = jg.nextToken();
                } catch (IOException e) {
                    throw new IoxException(e);
                }
                currentBid=bid.value;
                currentTopic=topic.value;
                state=INSIDE_BASKET;
                if(readBids.contains(currentBid)) {
                    throw new IoxException("unexpected BID "+currentBid+"; basket already finished");
                }
                StartBasketEvent event=new ch.interlis.iox_j.StartBasketEvent(currentTopic,currentBid);
                return event;
            }else if(current==JsonToken.END_ARRAY) {
                state=END;
                EndTransferEvent event=new ch.interlis.iox_j.EndTransferEvent();
                try {
                    current = jg.nextToken();
                    if(current!=JsonToken.END_OBJECT) {
                        throw new IoxException("unexpected json token "+current.toString()+"; '}' expected");
                    }
                } catch (IOException e) {
                    throw new IoxException(e);
                }
                return event;
            }else {
                throw new IoxException("unexpected json token "+current.toString()+"; ']' or '{' expected");
            }
        }else if(state==INSIDE_BASKET) {
            state=INSIDE_OBJECT;
            ObjectEvent event=new ch.interlis.iox_j.ObjectEvent(currentObj);
            return event;
        }else if(state==INSIDE_OBJECT) {
            if(current==JsonToken.START_OBJECT) {
                OutParam<String> bid=new OutParam<String>();
                OutParam<String> topic=new OutParam<String>();
                try {
                    currentObj=json2iox.read(bid,topic);
                    current = jg.nextToken();
                } catch (IOException e) {
                    throw new IoxException(e);
                }
                if(currentBid.equals(bid.value) && currentTopic.equals(topic.value)) {
                    ObjectEvent event=new ch.interlis.iox_j.ObjectEvent(currentObj);
                    return event;
                }
                if(currentBid!=null) {
                    readBids.add(currentBid);
                }
                currentBid=bid.value;
                currentTopic=topic.value;
                state=END_BASKET;
                EndBasketEvent event=new ch.interlis.iox_j.EndBasketEvent();
                return event;
            }else if(current==JsonToken.END_ARRAY) {
                if(currentBid!=null) {
                    readBids.add(currentBid);
                }
                currentBid=null;
                currentTopic=null;
                state=END_TRANSFER;
                EndBasketEvent event=new ch.interlis.iox_j.EndBasketEvent();
                return event;
            }else {
                throw new IoxException("unexpected json token "+jg.currentToken().toString()+"; ']' or '{' expected");
            }
        }else if(state==END_BASKET) {
            state=INSIDE_BASKET;
            if(readBids.contains(currentBid)) {
                throw new IoxException("unexpected BID "+currentBid+"; basket already finished");
            }
            StartBasketEvent event=new ch.interlis.iox_j.StartBasketEvent(currentTopic,currentBid);
            return event;
        }else if(state==END_TRANSFER) {
            state=END;
            try {
                current = jg.nextToken();
                if(current!=JsonToken.END_OBJECT) {
                    throw new IoxException("unexpected json token "+current.toString()+"; '}' expected");
                }
            } catch (IOException e) {
                throw new IoxException(e);
            }
            EndTransferEvent event=new ch.interlis.iox_j.EndTransferEvent();
            return event;
        }
        throw new IoxException("unexpected state"+state);
        
	}

    /** find appropriate viewable in model/models
     * @param iomObj
     * @return
     * @throws IoxException 
     */
    private Viewable findViewable(IomObject iomObj) throws IoxException {
        List<Viewable>foundIliClasses=null;
        Viewable ret=null;
        String tag=iomObj.getobjecttag();
        String[] elements=tag.split("\\.");
        String viewable=elements[elements.length-1];
        // td is set
        if(td!=null) {
            foundIliClasses=new ArrayList<Viewable>();
            List<HashMap<String,Viewable>> allModels=setupNameMapping();
            for(HashMap<String,Viewable> map : allModels) {
                ret= map.get(viewable);
                if(ret!=null) {
                    foundIliClasses.add(ret);
                }
            }
            if(foundIliClasses.size()>1) {
                throw new IoxException("several possible classes were found: "+foundIliClasses.toString());
            }else if(foundIliClasses.size()==1){
                return foundIliClasses.get(0);
            }
        }
        return null;
    }
    
    /** check attrvalues with attrnames
     * @param viewable
     * @param iomObj
     * @return
     */
    private String[] getAttributeNames(IomObject iomObj){
        // iliAttributes
        String[] attrs=new String[iomObj.getattrcount()];
        int count=0;
        for(int i=0;i<iomObj.getattrcount();i++) {
            String attribute=iomObj.getattrname(i);
            attrs[count]=attribute;
            count+=1;
        }
        java.util.Arrays.sort(attrs);
        return attrs;
    }
    private String[] getAttributeNames(Viewable viewable){
        // iliAttributes
        ArrayList<String> attrs=new ArrayList<String>();
        Iterator viewableIter=viewable.getAttributes();
        while(viewableIter.hasNext()) {
            Object attrObj=viewableIter.next();
            if(attrObj instanceof LocalAttribute) {
                LocalAttribute localAttr= (LocalAttribute)attrObj;
                String iliAttrName=localAttr.getName();
                attrs.add(iliAttrName);
            }
        }
        return attrs.toArray(new String[attrs.size()]);
    }

    /** Iterate through ili file and set all models with class names and appropriate class object.
     */
    private List<HashMap<String, Viewable>> setupNameMapping(){
        List<HashMap<String, Viewable>> allModels=new ArrayList<HashMap<String, Viewable>>();
        HashMap<String, Viewable> allClassesOfModel=null;
        Iterator tdIterator = td.iterator();
        while(tdIterator.hasNext()){
            allClassesOfModel=new HashMap<String, Viewable>();
            Object modelObj = tdIterator.next();
            if(!(modelObj instanceof DataModel)){
                continue;
            }
            // iliModel
            DataModel model = (DataModel) modelObj;
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
                    if(!(classObj instanceof Viewable)){
                        continue;
                    }
                    Viewable viewable = (Viewable) classObj;
                    allClassesOfModel.put(viewable.getName(), viewable);
                }
            }
            allModels.add(0,allClassesOfModel);
        }
        return allModels;
    }
    
    /** get attribute values which are valid to write to file
     * @param attrNames
     * @param currentIomObject
     * @return
     */
    private String[] getAttributeValues(String[] attrNames, IomObject currentIomObject) {
    	String[] attrValues=new String[attrNames.length];
    	for (int i=0;i<attrNames.length;i++) {
    		String attrValue=currentIomObject.getattrvalue(attrNames[i]);
			attrValues[i]=attrValue;
    	}
    	return attrValues;
	}

    

	/** set model/models
	 * @param td
	 */
	public void setModel(TransferDescription td) {
		this.td=td;
	}
	
	/** close writer and delete saved data
	 */
	@Override
	public void close() throws IoxException {
	    if(jg!=null) {
	        try {
                jg.close();
            } catch (IOException e) {
                throw new IoxException(e);
            }finally {
                jg=null;
            }
	    }
		if(reader!=null){
			try {
				reader.close();
			} catch (IOException e) {
				throw new IoxException(e);
			}finally {
	            reader=null;
			}
		}
	}
	
	@Override
	public void setFactory(IoxFactoryCollection factory) throws IoxException {
	}
	
	@Override
	public IoxFactoryCollection getFactory() throws IoxException {
		return null;
	}
	
	@Override
	public IomObject createIomObject(String type, String oid) throws IoxException {
		return null;
	}
}