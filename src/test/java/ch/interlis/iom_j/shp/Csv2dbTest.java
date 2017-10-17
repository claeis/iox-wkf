package ch.interlis.iom_j.shp;

import static org.junit.Assert.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.db2shp.Config;
import ch.interlis.db2shp.Csv2db;
import ch.interlis.iox.IoxException;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Csv2dbTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private Statement stmt=null;
	private static final String ROW="row";
	private Map<String, List<String>> rows=null;
	
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table set
	@Test
	public void import_SetAll_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "model2");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportwithheadernopk");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				String id=null;
				String abbreviation=null;
				String state=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportwithheadernopk.id, csvtodbschema.csvimportwithheadernopk.abbreviation, csvtodbschema.csvimportwithheadernopk.state FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("20")) {
						assertTrue(rowValue.get(1).equals("AU"));
						assertTrue(rowValue.get(2).equals("Deutschland"));
						break;
				  	}
				}
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk WHERE state='Deutschland'");
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header not set
	// - model set
	// - model path set
	// - db schema not set
	// - db table set
	@Test
	public void import_SchemaNotSet_HeaderNotSet_Ok() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				// HEADER: HEADERPRESENT, HEADERABSENT not set
				config.setValue(Config.SETTING_MODELNAMES, "modelImport");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
				// DBSCHEMA: "csvtodbschema" not set
				config.setValue(Config.TABLE, "csvimportnopkcolumn");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				String attr1=null;
				String attr2=null;
				String attr3=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT public.csvimportnopkcolumn.attr1, public.csvimportnopkcolumn.attr2, public.csvimportnopkcolumn.attr3 FROM public.csvimportnopkcolumn");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					attr1 = rs.getString(1);
					attr2 = rs.getString(2);
					attr3= rs.getString(3);
					row.add(attr1);
					row.add(attr2);
					row.add(attr3);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean dataFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("25") && rowValue.get(1).equals("AU") && rowValue.get(2).equals("Holland")) {
				  		dataFound=true;
				  	}
				}
				if(!dataFound) {
					dataFound=false;
					fail();
				}
				dataFound=false;
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM public.csvimportnopkcolumn WHERE attr3='Holland'");
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
			
		}
	}
	
	// import test
	// - header not set
	// - model not set
	// - model path not set
	// - db schema set
	// - db table set
	@Test
	public void import_HeaderAndPathNotSet_ModelNotSet_Ok() throws Exception
	{
		String attr1=null;
		String attr2=null;
		String attr3=null;
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				config.setValue(Config.HEADER, Config.HEADERABSENT);
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportnopk");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportnopk.attr1, csvtodbschema.csvimportnopk.attr2, csvtodbschema.csvimportnopk.attr3 FROM "+config.getValue(Config.DBSCHEMA)+".csvimportnopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					attr1 = rs.getString(1);
					attr2 = rs.getString(2);
					attr3= rs.getString(3);
					row.add(attr1);
					row.add(attr2);
					row.add(attr3);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean attrValuesFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("25")&&rowValue.get(1).equals("AU")&&rowValue.get(2).equals("Holland")){
				  		attrValuesFound=true;
				  	}
				}
				if(!attrValuesFound) {
					attrValuesFound=false;
					fail();
				}
				attrValuesFound=false;
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM csvtodbschema.csvimportnopk WHERE attr3='Holland'");
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema not set
	// - db table set
	@Test
	public void import_SchemaNotSet_Ok() throws Exception
	{
		String id=null;
		String abbreviation=null;
		String state=null;
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "model2");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
				// DBSCHEMA: not set "csvtodbschema"
				config.setValue(Config.TABLE, "csvimportwithheader");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvimportwithheader.id, csvimportwithheader.abbreviation, csvimportwithheader.state FROM csvimportwithheader");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean attrValuesFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("20")&&rowValue.get(1).equals("AU")&&rowValue.get(2).equals("Deutschland")){
				  		attrValuesFound=true;
				  	}
				}
				if(!attrValuesFound) {
					attrValuesFound=false;
					fail();
				}
				attrValuesFound=false;
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM csvimportwithheader WHERE state='Deutschland'");
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// import test
	// - header present
	// - model set
	// - model path not set
	// - db schema set
	// - db table set
	@Test
	public void import_PathNotSet_Ok() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				config.setValue(Config.SETTING_MODELNAMES, "model2");
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set.
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportwithheadernopk");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				String id=null;
				String abbreviation=null;
				String state=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvimportwithheadernopk.id, csvimportwithheadernopk.abbreviation, csvimportwithheadernopk.state FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("20")) {
						assertTrue(rowValue.get(1).equals("AU"));
						assertTrue(rowValue.get(2).equals("Deutschland"));
						break;
				  	}
				}
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk WHERE state='Deutschland'");
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header present
	// - model not set
	// - model not path set
	// - db schema set
	// - db table set
	@Test
	public void import_ModelAndPathNotSet_Ok() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
				config.setValue(Config.HEADER, Config.HEADERPRESENT);
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportwithheadernopk");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				String id=null;
				String abbreviation=null;
				String state=null;
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportwithheadernopk.id, csvtodbschema.csvimportwithheadernopk.abbreviation, csvtodbschema.csvimportwithheadernopk.state FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("20")) {
						assertTrue(rowValue.get(1).equals("AU"));
						assertTrue(rowValue.get(2).equals("Deutschland"));
						break;
				  	}
				}
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM "+config.getValue(Config.DBSCHEMA)+".csvimportwithheadernopk WHERE state='Deutschland'");
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header absent
	// - model not set
	// - model not path set
	// - db schema set
	// - db table set
	@Test
	public void import_ModelAndPathNotSet_HeaderSetAbsent_Ok() throws Exception
	{
		String id=null;
		String abbreviation=null;
		String state=null;
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				config.setValue(Config.HEADER, Config.HEADERABSENT);
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportnopk");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportnopk.attr1, csvtodbschema.csvimportnopk.attr2, csvtodbschema.csvimportnopk.attr3 FROM csvtodbschema.csvimportnopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean attrValuesFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("25")&&rowValue.get(1).equals("AU")&&rowValue.get(2).equals("Holland")){
				  		attrValuesFound=true;
				  	}
				}
				if(!attrValuesFound) {
					attrValuesFound=false;
					fail();
				}
				attrValuesFound=false;
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM csvtodbschema.csvimportnopk WHERE attr3='Holland'");
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header not set
	// - model not set
	// - model not path set
	// - db schema set
	// - db table set
	@Test
	public void import_ModelAndPathNotSet_HeaderNotSet_Ok() throws Exception
	{
		String id=null;
		String abbreviation=null;
		String state=null;
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				// HEADER: Config.HEADERABSENT not set
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				config.setValue(Config.DBSCHEMA, "csvtodbschema");
				config.setValue(Config.TABLE, "csvimportnopk");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT csvtodbschema.csvimportnopk.attr1, csvtodbschema.csvimportnopk.attr2, csvtodbschema.csvimportnopk.attr3 FROM csvtodbschema.csvimportnopk");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	assertTrue(rowValue.get(0).equals("25"));
					assertTrue(rowValue.get(1).equals("AU"));
					assertTrue(rowValue.get(2).equals("Holland"));
				}
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM csvtodbschema.csvimportnopk WHERE attr3='Holland'");
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header not set
	// - model not set
	// - model not path set
	// - db schema not set
	// - db table set
	@Test
	public void import_ModelAndPathNotSet_HeaderNotSet_SchemaNotSet_Ok() throws Exception
	{
		String id=null;
		String abbreviation=null;
		String state=null;
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent2.csv");
				// HEADER: Config.HEADERABSENT not set
				// SETTING_MODELNAMES: "model2" not set
				// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
				// DBSCHEMA: "csvtodbschema" not set
				config.setValue(Config.TABLE, "csvimportnopkcolumn");
				Csv2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT public.csvimportnopkcolumn.attr1, public.csvimportnopkcolumn.attr2, public.csvimportnopkcolumn.attr3 FROM public.csvimportnopkcolumn");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=1;
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					id = rs.getString(1);
					abbreviation = rs.getString(2);
					state= rs.getString(3);
					row.add(id);
					row.add(abbreviation);
					row.add(state);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				// test on attribute-names: id, abbreviation, state.
				boolean attrValuesFound=false;
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("25")&&rowValue.get(1).equals("EU")&&rowValue.get(2).equals("Europa")){
				  		attrValuesFound=true;
				  	}
				}
				if(!attrValuesFound) {
					attrValuesFound=false;
					fail();
				}
				attrValuesFound=false;
				// delete after tested
				int result=stmt.executeUpdate("DELETE FROM public.csvimportnopkcolumn WHERE attr3='Europa'");
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// attributes of db schema not found in csv file and ili model
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table set
	@Test
	public void import_AttrNamesOfDbNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
			// csv
			File data=new File("src/test/data/Csv2DB/AttributesHeader3.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model3");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			Csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("data base attribute names: [id, abbreviation, state] not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// connection to database failed
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table set
	@Test
	public void import_ConnectionFailed_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, "12345");
			// csv
			File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			Csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("FATAL: Passwort-Authentifizierung"));
			assertTrue(e.getMessage().contains("fehlgeschlagen"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header not set
	// - model not set
	// - model not path set
	// - db schema not set
	// - db table not set
	@Test
	public void import_AllNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
			// csv
			File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
			// HEADER: HEADERPRESENT, HEADERABSENT not set
			// SETTING_MODELNAMES: "model2" not set
			// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
			// DBSCHEMA: "csvtodbschema" not set
			// TABLE: "csvimportwithheader" not set
			Csv2db.importData(data, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("expected tablename"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table not set
	@Test
	public void import_TableNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
			// csv
			File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			// TABLE: "csvimportwithheader" not set
			Csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("expected tablename"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// attributes of ili model not found in csv file
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table set
	@Test
	public void import_AttrNamesNotFoundInModel_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
			// csv
			File data=new File("src/test/data/Csv2DB/AttributesHeader3.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			Csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(IoxException e) {
			assertTrue(e.getMessage().contains("attributes of headerrecord: [name, lastname, planet] not found in iliModel: model2"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// csv file not found
	// import test
	// - header present
	// - model set
	// - model path set
	// - db schema set
	// - db table set
	// - csv file not found
	@Test
	public void import_CsvFileNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
			// csv
			File data=new File("src/test/data/NotExist/AttributesHeader.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "model2");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/NotExist");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			Csv2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// import test
	// - header present
	// - model not found
	// - model path set
	// - db schema set
	// - db table set
	@Test
	public void import_ModelNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
			// csv
			File data=new File("src/test/data/Csv2DB/AttributesHeader.csv");
			config.setValue(Config.HEADER, Config.HEADERPRESENT);
			config.setValue(Config.SETTING_MODELNAMES, "modelNotFound");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
			config.setValue(Config.DBSCHEMA, "csvtodbschema");
			config.setValue(Config.TABLE, "csvimportwithheader");
			Csv2db.importData(data, jdbcConnection, config);
	    	fail();
			}catch(IoxException e) {
				assertTrue(e.getMessage().contains("models [modelNotFound] not found"));
			}finally{
				if(jdbcConnection!=null){
					jdbcConnection.close();
				}
			}
	}
	
	// import test
	// - header not set
	// - model set
	// - model path set
	// - db schema not set
	// - db table set
	@Test
	public void import_UniqueConstraint_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
				// csv
				File data=new File("src/test/data/Csv2DB/AttributesHeaderAbsent.csv");
				// HEADER: HEADERPRESENT, HEADERABSENT not set
				config.setValue(Config.SETTING_MODELNAMES, "model2");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Csv2DB");
				// DBSCHEMA: "csvtodbschema" not set
				config.setValue(Config.TABLE, "csvimport");
				Csv2db.importData(data, jdbcConnection, config);
				Csv2db.importData(data, jdbcConnection, config);
				fail();
			}
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("import of model2.Topic12.Class1 oid o2 {attr2 AU, attr1 25, attr3 Holland} to csvtodbschema.csvimport failed"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}