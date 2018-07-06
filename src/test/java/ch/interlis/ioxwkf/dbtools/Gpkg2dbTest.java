package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;

import ch.ehi.basics.settings.Settings;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Gpkg2dbTest {
    private String dburl=System.getProperty("dburl");
    private String dbuser=System.getProperty("dbusr");
    private String dbpwd=System.getProperty("dbpwd");
    private Statement stmt=null;
    private static final String TEST_IN="src/test/data/Gpkg2DB/";

    // Testet ob der Import eines Points in die Datenbank funktioniert,
    // wenn die Test-Konfiguration wie folgt gesetzt wird:
    // - set: database-schema
    // - set: database-table
    // --
    // Erwartung: SUCCESS.
    @Test
    public void import_Point_Ok() throws Exception {
        Settings config=new Settings();
        Connection jdbcConnection=null;
        
        System.out.println("Stefan");
        System.out.println("Ziegler");
        System.out.println(dburl);
        
        
        
        assertTrue(true);

    }
}
