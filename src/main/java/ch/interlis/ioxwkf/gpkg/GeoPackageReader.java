package ch.interlis.ioxwkf.gpkg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxReader;

/** Read data from a GeoPackage.
 * If the file to read from can not be found, an exception will be thrown.
 */
public class GeoPackageReader implements IoxReader {

    // state
    private int state;
    private static final int START = 0;
    private static final int INSIDE_TRANSFER = 1;
    private static final int INSIDE_BASKET = 2;
    private static final int INSIDE_OBJECT = 3;
    private static final int END_BASKET = 4;
    private static final int END_TRANSFER = 5;
    private static final int END = 6;

    // geopackage reader
    private Connection conn = null;
    
    // iox
    private TransferDescription td;
    private IoxFactoryCollection factory = new ch.interlis.iox_j.DefaultIoxFactoryCollection();
    private java.io.File inputFile = null;
    private String tableName = null;
    private int nextId = 1;

    // model, topic, class
    private String topicIliQName = "Topic";
    private String classIliQName = null;

    /** Creates a new geopackage reader.
     * @param gpkgFile to read from
     * @throws IoxException
     */
    public GeoPackageReader(java.io.File gpkgFile, String tableName) throws IoxException {
        this(gpkgFile, tableName, null);
    }
    
    /** Creates a new geopackage reader.
     * @param gpkgFile to read from
     * @throws IoxException
     */
    public GeoPackageReader(java.io.File gpkgFile, String tableName, Settings settings) throws IoxException{
        state = START;
        td = null;
        inputFile = gpkgFile;
        init(inputFile, settings);
    }
    
    /** Initialize file content.
     * @param gpkgFile
     * @param settings
     * @throws IoxException
     */
    private void init(java.io.File gpkgFile, Settings settings) throws IoxException {
        factory = new ch.interlis.iox_j.DefaultIoxFactoryCollection();
        
    
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + gpkgFile);
//            Statement statement = conn.createStatement();
//            ResultSet rs = statement.executeQuery("SELECT * FROM point;");
//            while(rs.next())
//            {
//                // read the result set
//                System.out.println(rs.getString("foo").toString());
//            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                    conn = null;
                } catch (SQLException f) {
                    throw new IoxException(f);
                }
            }
            throw new IoxException(e);
        } 
    }

    /** The optional Interlis model.
     * @param td
     */
    public void setModel(TransferDescription td){
        this.td = td;
    }

    @Override
    public IoxEvent read() throws IoxException {
        IomObject iomObj = null;        
        if(state == START){
            state = INSIDE_TRANSFER;
            topicIliQName = null;
            classIliQName = null;
            return new ch.interlis.iox_j.StartTransferEvent();
        }
        if(state==INSIDE_TRANSFER){
            state=INSIDE_BASKET;
        }
        
        if(state == INSIDE_BASKET) {
            System.out.println("********* INSIDE_BASKET");
        }

        return null;
    }

    
    
    
    @Override
    public void close() throws IoxException {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
            } catch (SQLException e) {
                throw new IoxException(e);
            }
        }        
    }

    @Override
    public IomObject createIomObject(String arg0, String arg1) throws IoxException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IoxFactoryCollection getFactory() throws IoxException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setFactory(IoxFactoryCollection arg0) throws IoxException {
        // TODO Auto-generated method stub
        
    }

}
