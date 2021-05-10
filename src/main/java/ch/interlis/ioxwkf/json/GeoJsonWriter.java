package ch.interlis.ioxwkf.json;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.json.Iox2json;
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

public class GeoJsonWriter implements ch.interlis.iox.IoxWriter {
	 // td
	private TransferDescription td=null;
	// writer
	private java.io.Writer writer=null;
    private JsonGenerator jg=null;
    private Iox2json iox2json=null;
	// defined by user
	private boolean doHeader=true;
	// first line of file
	private boolean firstObj=true;
	private String[] headerAttrNames=null;
    private String currentBid=null;
    private String currentTopic=null;

	/** create new CsvWriter
	 * @param file
	 * @throws IoxException
	 */
	public GeoJsonWriter(File file)throws IoxException{
		this(file,null);
	}
	public GeoJsonWriter(File file,Settings settings)throws IoxException{
		if(file!=null) {
			try {
				writer=new BufferedWriter(new java.io.OutputStreamWriter(new java.io.FileOutputStream(file),"UTF-8"));
			} catch (IOException e) {
				throw new IoxException("could not create file",e);
			}
			if(writer!=null) {
		        JsonFactory jsonF = new JsonFactory();
		        try {
                    jg = jsonF.createJsonGenerator(writer);
                } catch (IOException e) {
                    throw new IoxException("failed to create JsonGenerator",e);
                }
			}
		}
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
    
    /** writes objectvalues of objects via iox-Events
     */
    @Override
	public void write(IoxEvent event) throws IoxException {
		if(event instanceof StartTransferEvent){
		    try {
                jg.writeStartObject();
                jg.writeStringField(Iox2geoJson.TYPE,Iox2geoJson.FEATURE_COLLECTION);
                jg.writeFieldName(Iox2geoJson.FEATURES);
                jg.writeStartArray();
            } catch (IOException e) {
                throw new IoxException(e);
            }
		}else if(event instanceof StartBasketEvent){
		    StartBasketEvent startBasket=(StartBasketEvent)event;
		    currentBid=startBasket.getBid();
		    currentTopic=startBasket.getType();
            try {
                iox2json=new Iox2geoJson(jg,td);
            } catch (IOException e) {
                throw new IoxException(e);
            }
		}else if(event instanceof ObjectEvent){
			ObjectEvent obj=(ObjectEvent) event;
			IomObject iomObj=(IomObject)obj.getIomObject();
			// first obj?
			if(firstObj) {
				// get list of attr names
				if(td!=null) {
					Viewable resultViewableHeader=findViewable(iomObj);
					if(resultViewableHeader==null) {
						throw new IoxException("class "+iomObj.getobjecttag()+" in model not found");
					}
		    		headerAttrNames=getAttributeNames(resultViewableHeader);
				}else {
					if(headerAttrNames==null) {
			    		headerAttrNames=getAttributeNames(iomObj);
					}
				}
	    		if(doHeader) {
					try {
						writeHeader(headerAttrNames);
					} catch (IOException e) {
						throw new IoxException(e);
					}
	    		}
	    		firstObj=false;
			}
        	String[] validAttrValues=getAttributeValues(headerAttrNames, iomObj);
        	try {
        		writeFeature(iomObj);
        	} catch (IOException e) {
				throw new IoxException(e);
        	}
        	
        	
		}else if(event instanceof EndBasketEvent){
            currentBid=null;
            currentTopic=null;
		}else if(event instanceof EndTransferEvent){
            try {
                jg.writeEndArray();
                jg.writeEndObject();
            } catch (IOException e) {
                throw new IoxException(e);
            }
			close();
		}else{
			throw new IoxException("unknown event type "+event.getClass().getName());
		}
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

    /** write header attribute names to file
     * @param attrNames
     * @throws IOException
     */
	private void writeHeader(String[] attrNames) throws IOException {
	}

	/** write each line in file
	 * @param attrValues
	 * @throws IOException
	 * @throws IoxException
	 */
    private void writeFeature(IomObject iomObject) throws IOException {
        iox2json.write(iomObject,currentBid,currentTopic);

    }
    

	/** set model/models
	 * @param td
	 */
	public void setModel(TransferDescription td) {
		if(headerAttrNames!=null) {
			throw new IllegalStateException("attributes must not be set");
		}
		this.td=td;
	}
	public void setAttributes(String [] attr)
	{
		if(td!=null) {
			throw new IllegalStateException("ili-model must not be set");
		}
		headerAttrNames=attr.clone();
	}
	
	/** close writer and delete saved data
	 */
	@Override
	public void close() throws IoxException {
	    if(jg!=null) {
	        try {
                jg.flush();
                jg.close();
            } catch (IOException e) {
                throw new IoxException(e);
            }finally {
                jg=null;
            }
	    }
		if(writer!=null){
			try {
				writer.close();
			} catch (IOException e) {
				throw new IoxException(e);
			}finally {
	            writer=null;
			}
		}
	}
	
	@Override
	public void flush() throws IoxException {
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
	
	/**
	 * set header is present or header is absent.
	 * @param headerState
	 */
	public void setWriteHeader(boolean headerState){
		doHeader = headerState;
	}
}