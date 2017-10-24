package ch.interlis.ioxwkf.dbtools;

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
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.ioxwkf.dbtools.AbstractImport2db;
import ch.interlis.ioxwkf.dbtools.Config;
import ch.interlis.ioxwkf.dbtools.Shp2db;
import ch.interlis.ioxwkf.shp.ShapeReader;

//-Ddburl=jdbc:postgresql:dbname -Ddbusr=usrname -Ddbpwd=1234
public class Shp2dbTest {
	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private Statement stmt=null;
	private static final String ROW="row";
	private Map<String, List<String>> rows=null;
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SetAll_Point_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text)WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrs.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrs");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/Attributes");
				config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shptodbschema.shpimporttable.idname, shptodbschema.shpimporttable.textname, shptodbschema.shpimporttable.doublename, shptodbschema.shpimporttable.the_geom FROM shptodbschema.shpimporttable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String textValue = rs.getString(2);
					String doubleValue= rs.getString(3);
					String theGeomValue= rs.getString(4);
					row.add(idValue);
					row.add(textValue);
					row.add(doubleValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("1")) {
						assertTrue(rowValue.get(1).equals("text1"));
						assertTrue(rowValue.get(2).equals("53434"));
						assertTrue(rowValue.get(3).equals("COORD {C1 -0.4025974025974026, C2 1.3974025974025972}"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SetAll_MultiPoint_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text)WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/MultiPoint/MultiPoint2.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/MultiPoint");
				config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shptodbschema.shpimporttable.idname, shptodbschema.shpimporttable.the_geom FROM shptodbschema.shpimporttable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("111")) {
						assertTrue(
							rowValue.get(1).contains("COORD {C1 -0.5852801213385155, C2 0.7454355863114989}") ||
							rowValue.get(1).contains("COORD {C1 -0.45787468006446114, C2 0.6607109678642527}") ||
							rowValue.get(1).contains("COORD {C1 -0.4196530476822448, C2 0.7562650488197935}") ||
							rowValue.get(1).contains("COORD {C1 -0.39098682339558255, C2 0.5587866148450091}") ||
							rowValue.get(1).contains("COORD {C1 -0.6171314816570291, C2 0.5301203905583469}") ||
							rowValue.get(1).contains("COORD {C1 -0.4196530476822448, C2 0.7562650488197935}")
						);
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SetAll_LineString_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text)WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/LineString/LineString.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/LineString");
				config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shptodbschema.shpimporttable.idname, shptodbschema.shpimporttable.the_geom FROM shptodbschema.shpimporttable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("111")) {
						assertTrue(rowValue.get(1).equals("MULTIPOLYLINE {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -1.0462287104622872, C2 0.47688564476885653}, COORD {C1 0.5547445255474452, C2 0.15328467153284664}]}}}"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SetAll_MultiLineString_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text)WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/MultiLineString/MultiLineString.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/MultiLineString");
				config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shptodbschema.shpimporttable.idname, shptodbschema.shpimporttable.the_geom FROM shptodbschema.shpimporttable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("5555")) {
						assertTrue(rowValue.get(1).equals("MULTIPOLYLINE {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -1.0, C2 0.8321167883211679}, COORD {C1 0.5547445255474452, C2 0.8272506082725061}, COORD {C1 -1.02676399026764, C2 0.4720194647201946}, COORD {C1 0.4866180048661801, C2 0.3211678832116788}, COORD {C1 -1.0608272506082725, C2 0.11678832116788318}, COORD {C1 0.49148418491484214, C2 -0.05109489051094884}]}}}"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SetAll_Polygon_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text)WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/Polygon/Polygon.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/Polygon");
				config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shptodbschema.shpimporttable.idname, shptodbschema.shpimporttable.the_geom FROM shptodbschema.shpimporttable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("888")) {
					  	assertTrue(
							rowValue.get(1).contains("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -0.6374695863746959, C2 0.6618004866180048}, COORD {C1 0.6520681265206811, C2 0.6788321167883211}, COORD {C1 0.6618004866180052, C2 -0.36253041362530425}, COORD {C1 -0.6472019464720195, C2 -0.34793187347931886}, COORD {C1 -0.6374695863746959, C2 0.6618004866180048}]}}}}}")
						);
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SetAll_MultiPolygon_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text)WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/MultiPolygon/MultiPolygon.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/MultiPolygon");
				config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shptodbschema.shpimporttable.idname, shptodbschema.shpimporttable.the_geom FROM shptodbschema.shpimporttable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).equals("456")) {
					  	assertTrue(rowValue.get(1).contains("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -3.3585766423357675, C2 1.780413625304136}, COORD {C1 -3.2357055961070573, C2 1.780413625304136}, COORD {C1 -3.2925790754257918, C2 1.7156326034063258}, COORD {C1 -3.3859489051094904, C2 1.73661800486618}, COORD {C1 -3.3585766423357675, C2 1.780413625304136}]}}}}}"));
				  	}else if(rowValue.get(0).equals("34")) {
					  	assertTrue(rowValue.get(1).contains("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -3.402372262773724, C2 1.6846107055961068}, COORD {C1 -3.2810218978102204, C2 1.675486618004866}, COORD {C1 -3.3156934306569354, C2 1.6031021897810218}, COORD {C1 -3.4045012165450133, C2 1.648722627737226}, COORD {C1 -3.402372262773724, C2 1.6846107055961068}]}}}}}"));
				  	}else if(rowValue.get(0).equals("356")) {
					  	assertTrue(rowValue.get(1).contains("MULTISURFACE {surface SURFACE {boundary BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -3.2332725060827263, C2 1.729318734793187}, COORD {C1 -3.1739659367396604, C2 1.7247566909975667}, COORD {C1 -3.1757907542579087, C2 1.66970802919708}, COORD {C1 -3.2509124087591252, C2 1.7101581508515813}, COORD {C1 -3.2332725060827263, C2 1.729318734793187}]}}}}}"));
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - set: model-path
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_SchemaNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop table
	        	preStmt.execute("DROP TABLE IF EXISTS shpimportnoschematable CASCADE");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shpimportnoschematable(idname text NOT NULL,textname text,doublename text,the_geom text,CONSTRAINT shpimporttable_pkey PRIMARY KEY (idname)) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/LineString/LineString.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/LineString");
				//config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimportnoschematable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shpimportnoschematable.idname, shpimportnoschematable.the_geom FROM shpimportnoschematable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("111")) {
						assertTrue(rowValue.get(1).equals("MULTIPOLYLINE {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -1.0462287104622872, C2 0.47688564476885653}, COORD {C1 0.5547445255474452, C2 0.15328467153284664}]}}}"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}

	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_PathNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text)WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/LineString/LineString.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				//config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/LineString");
				config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shptodbschema.shpimporttable.idname, shptodbschema.shpimporttable.the_geom FROM shptodbschema.shpimporttable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("111")) {
						assertTrue(rowValue.get(1).equals("MULTIPOLYLINE {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -1.0462287104622872, C2 0.47688564476885653}, COORD {C1 0.5547445255474452, C2 0.15328467153284664}]}}}"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - NOT SET: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_ModelAndPathNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text)WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/LineString/LineString.shp");
				//config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				//config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/LineString");
				config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimporttable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shptodbschema.shpimporttable.idname, shptodbschema.shpimporttable.the_geom FROM shptodbschema.shpimporttable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("111")) {
						assertTrue(rowValue.get(1).equals("MULTIPOLYLINE {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -1.0462287104622872, C2 0.47688564476885653}, COORD {C1 0.5547445255474452, C2 0.15328467153284664}]}}}"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - NOT SET: model
	// - NOT SET: model-path
	// - NOT SET: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_ModelAndPathNotSet_SchemaNotSet_Ok() throws Exception
	{
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop table
	        	preStmt.execute("DROP TABLE IF EXISTS shpimportnoschematable CASCADE");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shpimportnoschematable(idname text NOT NULL,textname text,doublename text,the_geom text,CONSTRAINT shpimporttable_pkey PRIMARY KEY (idname)) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
				// shp
				File data=new File("src/test/data/Shp2DB/LineString/LineString.shp");
				//config.setValue(Config.SETTING_MODELNAMES, "ShapeModel");
				//config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/LineString");
				//config.setValue(Config.DBSCHEMA, "shptodbschema");
				config.setValue(Config.TABLE, "shpimportnoschematable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
			}
			{
				rows = new HashMap<String, List<String>>();
				stmt=jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT shpimportnoschematable.idname, shpimportnoschematable.the_geom FROM shpimportnoschematable");
				ResultSetMetaData rsmd = rs.getMetaData();
				int rowCount=rs.getRow();
				while(rs.next()){
					List<String> row = new ArrayList<String>();
					String idValue = rs.getString(1);
					String theGeomValue= rs.getString(2);
					row.add(idValue);
					row.add(theGeomValue);
					rows.put(ROW+String.valueOf(rowCount), row);
					rowCount+=1;
				}
				for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
				  	List<String> rowValue = entry.getValue();
				  	if(rowValue.get(0).contains("111")) {
						assertTrue(rowValue.get(1).equals("MULTIPOLYLINE {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 -1.0462287104622872, C2 0.47688564476885653}, COORD {C1 0.5547445255474452, C2 0.15328467153284664}]}}}"));
						break;
				  	}
				}
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird eine Shape-Datei gelesen, welche die folgenden Attribute beinhaltet:
	// - idname
	// - textname
	// - doublename
	// - the_geom
	// --
	// Nun werden die Attribute-Werte, nach den Attribute-Namen welche im Model definiert sind,
	// aus der Shapedatei herausgelesen:
	// - idname
	// - textname
	// - doublename
	// - the_geom
	// --
	// Von diesen Attributen werden nun Einschraenkungen durch das Datenbank-Schema gemacht
	// und die folgenden Daten in die Datenbank-Tabelle importiert:
	// - textname
	// - the_geom
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: SUCCESS.
	@Test
	public void import_LimitedSelection_Ok() throws Exception{
		{
			// reader test
			ShapeReader reader=null;
			TransferDescription tdM=null;
			Configuration ili2cConfig=new Configuration();
			FileEntry fileEntryConditionClass=new FileEntry("src/test/data/Shp2DB/Attributes/ShapeModelAttrsLimited.ili", FileEntryKind.ILIMODELFILE);
			ili2cConfig.addFileEntry(fileEntryConditionClass);
			tdM=ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
			assertNotNull(tdM);
			try {
				reader=new ShapeReader(new File("src/test/data/Shp2DB/Attributes/testPointAttrsLimited.shp"));
				reader.setModel(tdM);
				assertTrue(reader.read() instanceof StartTransferEvent);
				assertTrue(reader.read() instanceof StartBasketEvent);
				IoxEvent event=reader.read();
				if(event instanceof ObjectEvent){
		        	IomObject iomObj=((ObjectEvent)event).getIomObject();
		        	assertTrue(iomObj.getattrcount()==4);
		        	assertTrue(iomObj.getattrvalue("doublename").equals("54321"));
		        	assertTrue(iomObj.getattrvalue("idname").equals("1"));
		        	assertTrue(iomObj.getattrvalue("textname").equals("text1"));
		        	assertTrue(iomObj.getattrobj("the_geom", 0).toString().equals("COORD {C1 -0.5332351148239034, C2 0.7382312503416462}"));
				}
				assertTrue(reader.read() instanceof EndBasketEvent);
				assertTrue(reader.read() instanceof EndTransferEvent);
			}finally {
				if(reader!=null) {
			    	reader.close();
					reader=null;
		    	}
			}
		}
		{
			// import test
			Settings config=new Settings();
			Connection jdbcConnection=null;
			try{
		        Class driverClass = Class.forName("org.postgresql.Driver");
		        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
		        {
		        	Statement preStmt=jdbcConnection.createStatement();
		        	// drop schema
		        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
		        	// create schema
		        	preStmt.execute("CREATE SCHEMA shptodbschema");
		        	// create table in schema
		        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttablelimited(textname text,the_geom text)WITH (OIDS=FALSE)");
		        	preStmt.close();
		        }
		        {
					// shp
					File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrsLimited.shp");
					config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrsLimited");
					config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/Attributes");
					config.setValue(Config.DBSCHEMA, "shptodbschema");
					config.setValue(Config.TABLE, "shpimporttablelimited");
					AbstractImport2db shp2db=new Shp2db();
					shp2db.importData(data, jdbcConnection, config);
				}
				{
					rows = new HashMap<String, List<String>>();
					stmt=jdbcConnection.createStatement();
					ResultSet rs = stmt.executeQuery("SELECT * FROM shptodbschema.shpimporttablelimited");
					ResultSetMetaData rsmd = rs.getMetaData();
					int rowCount=rs.getRow();
					while(rs.next()){
						List<String> row = new ArrayList<String>();
						String textValue = rs.getString(1);
						String theGeomValue= rs.getString(2);
						row.add(textValue);
						row.add(theGeomValue);
						rows.put(ROW+String.valueOf(rowCount), row);
						rowCount+=1;
					}
					for (Map.Entry<String,List<String>> entry : rows.entrySet()) {
					  	List<String> rowValue = entry.getValue();
					  	if(rowValue.get(0).equals("text1")) {
							assertTrue(rowValue.get(1).equals("COORD {C1 -0.5332351148239034, C2 0.7382312503416462}"));
							break;
					  	}
					}
				}
			}finally{
				if(jdbcConnection!=null){
					jdbcConnection.close();
				}
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Spalten-Namen (Attribute-Namen) der gesuchten Datenbank-Tabelle,
	// welche sich innerhalb des gesuchten Datenbank-Schemas befindet,
	// weder in den gesetzten Modellen, noch in der Shape-Datei gefunden werden koennen.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: data base attribute names ... not found.
	@Test
	public void import_AttrNamesOfDbNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
			{
				Statement preStmt=jdbcConnection.createStatement();
				// drop schema
				preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
				// create schema
				preStmt.execute("CREATE SCHEMA shptodbschema");
				// create table in schema
				preStmt.execute("CREATE TABLE shptodbschema.shpimportnoattrsfound(id text,name text,state text) WITH (OIDS=FALSE)");
				preStmt.close();
			}
	        // shp
			File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrs.shp");
			config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrsNotFound");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/Attributes");
			config.setValue(Config.DBSCHEMA, "shptodbschema");
			config.setValue(Config.TABLE, "shpimportnoattrsfound");
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("model attribute names: '[id2, name2, lastname2, phonenumber2]' not found in"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die gesetzte Connection nicht funktioniert.
	// --
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: data base attribute names ... not found.
	@Test
	public void import_ConnectionFailed_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, "12345");
	        // shp
			File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrs.shp");
			config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrs");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/Attributes");
			config.setValue(Config.DBSCHEMA, "shptodbschema");
			config.setValue(Config.TABLE, "shpimporttable");
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
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
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - NOT SET: model
	// - NOT SET: model-path
	// - NOT SET: database-schema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	@Test
	public void import_AllNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        // shp
			File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrs.shp");
			// SETTING_MODELNAMES: "model2" not set
			// SETTING_ILIDIRS: "src/test/data/Csv2DB" not set
			// DBSCHEMA: "csvtodbschema" not set
			// TABLE: "csvimportwithheader" not set
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
			fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("expected tablename"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Test-Konfiguration wie folgt gesetzt wird:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - NOT SET: database-table
	// --
	// Erwartung: FEHLER: expected tablename
	@Test
	public void import_TableNotSet_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        // shp
			File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrs.shp");
			config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrs");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/Attributes");
			config.setValue(Config.DBSCHEMA, "shptodbschema");
			// TABLE: "shpimporttable"
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("expected tablename"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Attribute-Namen innerhalb des Modells nicht gefunden werden koennen.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - NOT SET: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: model attribute names: '[id2, name2, lastname2, phonenumber2]' not found in ... .csv file
	@Test
	public void import_AttrNamesNotFoundInModel_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        // shp
			File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrs.shp");
			config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrsNotFound");
			// SETTING_ILIDIRS: "src/test/data/Shp2DB/Attributes"
			config.setValue(Config.DBSCHEMA, "shptodbschema");
			config.setValue(Config.TABLE, "shpimporttable");
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("model attribute names: '[id2, name2, lastname2, phonenumber2]' not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die gesetzte shp Datei nicht existiert.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: shapefile ... not found
	@Test
	public void import_CsvFileNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        // shp
			File data=new File("src/test/data/Shp2DB/NotExist/testPointAttrs.shp");
			config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrs");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/NotExist");
			config.setValue(Config.DBSCHEMA, "shptodbschema");
			config.setValue(Config.TABLE, "shpimporttable");
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("shapefile"));
			assertTrue(e.getMessage().contains("not found"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn das gesetzte Model/die Modelle nicht existieren.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: model ... not found
	@Test
	public void import_ModelNotFound_Fail() throws Exception
	{
		Settings config=null;
		config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop schema
	        	preStmt.execute("DROP SCHEMA IF EXISTS shptodbschema CASCADE");
	        	// create schema
	        	preStmt.execute("CREATE SCHEMA shptodbschema");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shptodbschema.shpimporttable(idname text,textname text,doublename text,the_geom text) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        // shp
			File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrs.shp");
			config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrsNotFound");
			config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/Attributes");
			config.setValue(Config.DBSCHEMA, "shptodbschema");
			config.setValue(Config.TABLE, "shpimporttable");
			AbstractImport2db shp2db=new Shp2db();
			shp2db.importData(data, jdbcConnection, config);
	    	fail();
			}catch(Exception e) {
				assertTrue(e.getMessage().contains("model attribute names: '[id2, name2, lastname2, phonenumber2]' not found in"));
			}finally{
				if(jdbcConnection!=null){
					jdbcConnection.close();
				}
			}
	}
	
	// Es wird getestet ob eine Fehlermeldung ausgegeben wird,
	// wenn die Daten bereits in der Datenbank vorhanden sind.
	// Die Test-Konfiguration wird wie folgt gesetzt:
	// - set: model
	// - set: model-path
	// - set: database-schema
	// - set: database-table
	// --
	// Erwartung: FEHLER: pkid ... bereits vorhanden in datenbank
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
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop table
	        	preStmt.execute("DROP TABLE IF EXISTS shpimportnoschematable CASCADE");
	        	// create table in schema
	        	preStmt.execute("CREATE TABLE shpimportnoschematable(idname text NOT NULL,textname text,doublename text,the_geom text,CONSTRAINT shpimporttable_pkey PRIMARY KEY (idname)) WITH (OIDS=FALSE)");
	        	preStmt.close();
	        }
	        {
		        // shp
				File data=new File("src/test/data/Shp2DB/Attributes/testPointAttrs.shp");
				config.setValue(Config.SETTING_MODELNAMES, "ShapeModelAttrs");
				config.setValue(Config.SETTING_ILIDIRS, "src/test/data/Shp2DB/Attributes");
				// DBSCHEMA: "shptodbschema"
				config.setValue(Config.TABLE, "shpimportnoschematable");
				AbstractImport2db shp2db=new Shp2db();
				shp2db.importData(data, jdbcConnection, config);
				shp2db.importData(data, jdbcConnection, config);
				fail();
			}
		}catch(Exception e) {
			assertTrue(e.getMessage().contains("import of ShapeModelAttrs.Topic1.testPointAttrs oid o2 {idname 1, doublename 53434, the_geom COORD {C1 -0.4025974025974026, C2 1.3974025974025972}, textname text1} to shpimportnoschematable failed"));
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}