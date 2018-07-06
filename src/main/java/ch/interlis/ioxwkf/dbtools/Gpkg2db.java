package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.util.List;

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
     * File file=new File("C:\file.shp");<br>
     * Gpkg2db gpkgImport = new Gpkg2db();<br>
     * gpkgImport.importData(file, "Connection", config);
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
    protected IoxReader createReader(File file, Settings config) throws IoxException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected List<AttributeDescriptor> assignIomAttr2DbColumn(IoxReader reader, List<AttributeDescriptor> dbColumns,
            List<String> missingDbColumns) {
        // TODO Auto-generated method stub
        return null;
    }

}



/**
 * AbstractImport2db shp2db=new Shp2db();
 * shp2db.importData(data, jdbcConnection, config);
 * importData:
 *     IoxReader reader=createReader(file, config); 
*/