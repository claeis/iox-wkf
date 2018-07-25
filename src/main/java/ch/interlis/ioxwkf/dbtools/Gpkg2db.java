package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;
import ch.interlis.ioxwkf.gpkg.GeoPackageReader;

/**<b>Gpkg2db</b>
 * <p>
 * 
 * <b>The main task</b><br>
 * read data of files with any IoxReader, converted from INTERLIS to PostGIS dataTypes and import converted data to database.<br>
 * <p>
 * 
 * <b>Create a new Gpkg2db</b><br>
 * <li>Create an Gpkg2db object. Gpkg2db extends AbstractImport2db class.</li>
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
 * <li><a href="http://www.geopackage.org/spec/">GeoPackage specification</a></li>
 * <li><a href="https://docs.oracle.com/javase/6/docs/api/java/sql/Types.html">java.sql.Types</a></li>
 */
public class Gpkg2db extends AbstractImport2db {
    /** Create the IoxReader.<br>
     * There are 2 parameter to define:<br>
     * <li>the file to read from.</li>
     * <li>the config.</li>
     * <p>
     * 
     * Set File (Mandatory)<br>
     * The file to read from.
     * <p>
     * 
     * File has to exist and has to be readable.<br>
     * Gpkg2db gpkgImport = new Gpkg2db();<br>
     * gpkgImport.importData("/path/to/file/;tablename", "Connection", config);
     * <p>
     * 
     * Setting possibilities:<br>
     * <li>Setting possibilities<br>
     *     {@link ch.interlis.ioxwkf.dbtools.IoxWkfConfig}
     * </li>
     * @param file to read from
     * @param config defined settings
     * @return IoxReader
     */
    @Override
    protected IoxReader createReader(Object obj, Settings config) throws IoxException {
        File file = null;
        String tableName = null;
        if (obj != null) {
            String[] splits = ((String)obj).split(";");
            file = new File(splits[0]);
            tableName = splits[1];
            if (file.exists()) {
                EhiLogger.logState("file to read from: <"+file.getName()+">");
            } else {
                throw new IoxException("file "+file.getAbsolutePath()+" not found.");
            }
        }  else {
            throw new IoxException("obj==null.");
        }
        return new GeoPackageReader(file, tableName, config);
    }

    @Override
    protected List<AttributeDescriptor> assignIomAttr2DbColumn(IoxReader ioxReader, List<AttributeDescriptor> attrDescriptors, List<String> missingAttributes) {
        List<AttributeDescriptor> ret=new ArrayList<AttributeDescriptor>();
        GeoPackageReader reader = (GeoPackageReader)ioxReader;
        HashMap<String,AttributeDescriptor> attrs=new HashMap<String,AttributeDescriptor>();
        Map<String,AttributeDescriptor> geomAttrs=new HashMap<String,AttributeDescriptor>();
        for(AttributeDescriptor attrDesc:attrDescriptors) {
            if(attrDesc.getDbColumnGeomTypeName()!=null) {
                geomAttrs.put(attrDesc.getDbColumnName().toLowerCase(), attrDesc);
            }
            attrs.put(attrDesc.getDbColumnName().toLowerCase(), attrDesc);
        }
        String[] gpkgAttrs = reader.getAttributes();
        List<String> gpkgGeomAttrs = reader.getGeometryAttributes();
        for (String gpkgAttr : gpkgAttrs) {
            AttributeDescriptor attrDesc=null;
            // geometry columns get some additional information hence the special treatment
            if (gpkgGeomAttrs.contains(gpkgAttr)) {
                attrDesc = geomAttrs.get(gpkgAttr.toLowerCase());
            } else {
                attrDesc = attrs.get(gpkgAttr.toLowerCase());
            }
            if (attrDesc != null) {
                attrDesc.setIomAttributeName(gpkgAttr);
                ret.add(attrDesc);
            } else {
                missingAttributes.add(gpkgAttr);
            }
        }
        return ret;
    }
}
