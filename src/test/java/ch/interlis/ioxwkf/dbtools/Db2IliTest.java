package ch.interlis.ioxwkf.dbtools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import org.junit.BeforeClass;
import org.junit.Test;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.metamodel.AbstractClassDef;
import ch.interlis.ili2c.metamodel.AssociationDef;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.BlackboxType;
import ch.interlis.ili2c.metamodel.Cardinality;
import ch.interlis.ili2c.metamodel.Container;
import ch.interlis.ili2c.metamodel.Domain;
import ch.interlis.ili2c.metamodel.Element;
import ch.interlis.ili2c.metamodel.LineForm;
import ch.interlis.ili2c.metamodel.Model;
import ch.interlis.ili2c.metamodel.NumericType;
import ch.interlis.ili2c.metamodel.PolylineType;
import ch.interlis.ili2c.metamodel.PrecisionDecimal;
import ch.interlis.ili2c.metamodel.RoleDef;
import ch.interlis.ili2c.metamodel.SurfaceType;
import ch.interlis.ili2c.metamodel.Table;
import ch.interlis.ili2c.metamodel.TextType;
import ch.interlis.ili2c.metamodel.Topic;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Type;
import ch.interlis.ili2c.metamodel.TypeAlias;
import ch.interlis.iox.IoxException;

public class Db2IliTest {

	private String dburl=System.getProperty("dburl");
	private String dbuser=System.getProperty("dbusr");
	private String dbpwd=System.getProperty("dbpwd");
	private static final String TEST_OUT="src/test/data/Db2Ili/";
	
	@BeforeClass
	public static void setup() throws Ili2cFailure
	{
		new File(TEST_OUT).mkdirs();
	}
	
	// Alle Tabellen innerhalb dem definierten Schema, sollen gefunden werden.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Alle Tabellen innerhalb dem definierten Schema werden gefunden.
	@Test
	public void export_FindAllTablesInDefinedSchema_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		final String TABLE2="table2";
		final String TABLE3="table3";
		final String TABLE4="table4";
		final String TABLE5="table5";
		final File iliFile=new File(TEST_OUT+"export_FindAllTablesInDefinedSchema_Ok.ili");
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE2+"();");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE3+"();");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE4+"();");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE5+"();");
	        	preStmt.close();
	        }
			if(iliFile.exists()) {
				iliFile.delete();
			}
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(iliFile, jdbcConnection, config);
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFilename=TEST_OUT+"export_FindAllTablesInDefinedSchema_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFilename);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			assertNotNull(td);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			Table table2=(Table) topic.getElement(Table.class, TABLE2);
			assertNotNull(table2);
			Table table3=(Table) topic.getElement(Table.class, TABLE3);
			assertNotNull(table3);
			Table table4=(Table) topic.getElement(Table.class, TABLE4);
			assertNotNull(table4);
			Table table5=(Table) topic.getElement(Table.class, TABLE5);
			assertNotNull(table5);
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}
	
	// Es werden 3 Tabellen mit je einem Tabellenkommentar innerhalb des definierten Schemas erstellt.
	// Die Tabellenkommentare werden als ili Kommentare in die ili Datei exportiert.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: oberhalb jeder Tabelle wird eine Beschreibung mit "/** text */" eingefuegt.
	@Test
	public void export_TableDescriptionsInDefinedSchema_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		final String TABLE2="table2";
		final String TABLE3="table3";
		final String TABLE1DESCRIPTION="This is table1.";
		final String TABLE2DESCRIPTION="This is table2.";
		final String TABLE3DESCRIPTION="This is table3.";
		final File iliFile=new File(TEST_OUT+"export_TableDescriptionsInDefinedSchema_Ok.ili");
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
	        	preStmt.execute("COMMENT ON TABLE "+SCHEMANAME+"."+TABLE1+" IS '"+TABLE1DESCRIPTION+"';");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE2+"();");
	        	preStmt.execute("COMMENT ON TABLE "+SCHEMANAME+"."+TABLE2+" IS '"+TABLE2DESCRIPTION+"';");
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE3+"();");
	        	preStmt.execute("COMMENT ON TABLE "+SCHEMANAME+"."+TABLE3+" IS '"+TABLE3DESCRIPTION+"';");
	        	preStmt.close();
	        }
			if(iliFile.exists()) {
				iliFile.delete();
			}
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(iliFile, jdbcConnection, config);
		}catch(Exception e) {
			throw new IoxException(e);
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	    String query="SELECT obj_description('"+SCHEMANAME+"."+TABLE1+"'::regclass);";		
		try{
			// model compile test
			String iliFilename=TEST_OUT+"export_TableDescriptionsInDefinedSchema_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFilename);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			assertNotNull(td);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertEquals(TABLE1DESCRIPTION, table1.getDocumentation());
			assertNotNull(table1);
			Table table2=(Table) topic.getElement(Table.class, TABLE2);
			assertEquals(TABLE2DESCRIPTION, table2.getDocumentation());
			assertNotNull(table2);
			Table table3=(Table) topic.getElement(Table.class, TABLE3);
			assertEquals(TABLE3DESCRIPTION, table3.getDocumentation());
			assertNotNull(table3);
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}
	
	// Es wird 1 Tabelle innerhalb des definierten Schemas erstellt.
	// Innerhalb dieser Tabellen sollen alle AttributeTypen erstellt werden.
	// Die Tabelle wird mit den DatenTypen in die ili Datei exportiert.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Alle AttributeTypen sollen erkannt werden.
	@Test
	public void export_RecognizeAllDataTypesInClass_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		final File iliFile=new File(TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili");
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoilischema
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	// create dbtoilischema
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	// create table in dbtoilischema
	        	try {
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"("
		        			+ "attr1 smallint NOT NULL,"
		        			+ "attr2 integer,"
		        			+ "attr3 bigint,"
		        			+ "attr4 decimal(10),"
		        			+ "attr5 numeric(20),"
		        			+ "attr6 real,"
		        			+ "attr7 float,"
		        			+ "attr8 double precision,"
		        			+ "attr9 character,"
		        			+ "attr10 character(5) NOT NULL,"
		        			+ "attr11 char,"
		        			+ "attr12 char(2),"
		        			+ "attr13 varchar,"
		        			+ "attr14 varchar(5),"
		        			+ "attr15 character varying,"
		        			+ "attr16 character varying(10),"
		        			+ "attr17 date,"
		        			+ "attr18 time,"
		        			+ "attr19 time with time zone,"
		        			+ "attr20 timestamp,"
		        			+ "attr21 timestamp with time zone,"
							+ "attr22 boolean,"
							+ "attr23 bit(3),"
							+ "attr24 bit(5),"
							+ "attr25 uuid,"
							+ "attr26 xml,"
							+ "attr27 geometry(POINT,2056) NOT NULL,"
							+ "attr28 geometry(LINESTRING,2056),"
							+ "attr29 geometry(POLYGON,2056)"
							+ ")WITH (OIDS=FALSE);");
		        	preStmt.close();
	        	}catch(Exception e) {
	        		throw new IoxException(e);
	        	}
	        }
	        {
				// delete file if already exist
				if(iliFile.exists()) {
					iliFile.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(iliFile, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFilename=TEST_OUT+"export_RecognizeAllDataTypesInClass_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFilename);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			assertNotNull(td);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			// attribute1
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr1");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal(-32768)));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal(32767)));
			}
			// attribute2
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr2");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal(-2147483648)));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal(2147483647)));
			}
			// attribute3
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr3");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal("-9223372036854775808")));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal("9223372036854775807")));
			}
			// attribute4
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr4");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal(-10)));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal(10)));
			}
			// attribute5
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr5");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal(-20)));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal(20)));
			}
			// attribute6
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr6");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal("-340282346638528860000000000000000000000.000000")));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal("340282346638528860000000000000000000000.000000")));
			}
			// attribute7
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr7");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal("-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.000000000000000")));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal("179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.000000000000000")));
			}
			// attribute8
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr8");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal("-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.000000000000000")));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal("179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.000000000000000")));
			}
			// attribute9
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr9");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TextType);
				Type domainType=attribute.getDomain();
				TextType textType=(TextType) domainType;
				assertEquals(1, textType.getMaxLength());
			}
			// attribute10
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr10");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TextType);
				Type domainType=attribute.getDomain();
				TextType textType=(TextType) domainType;
				assertEquals(5, textType.getMaxLength());
			}
			// attribute11
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr11");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TextType);
				Type domainType=attribute.getDomain();
				TextType textType=(TextType) domainType;
				assertEquals(1, textType.getMaxLength());
			}
			// attribute12
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr12");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TextType);
				Type domainType=attribute.getDomain();
				TextType textType=(TextType) domainType;
				assertEquals(2, textType.getMaxLength());
			}
			// attribute13
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr13");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TextType);
				Type domainType=attribute.getDomain();
				TextType textType=(TextType) domainType;
				assertEquals(2147483647, textType.getMaxLength());
			}
			// attribute14
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr14");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TextType);
				Type domainType=attribute.getDomain();
				TextType textType=(TextType) domainType;
				assertEquals(5, textType.getMaxLength());
			}
			// attribute15
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr15");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TextType);
				Type domainType=attribute.getDomain();
				TextType textType=(TextType) domainType;
				assertEquals(2147483647, textType.getMaxLength());
			}
			// attribute16
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr16");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TextType);
				Type domainType=attribute.getDomain();
				TextType textType=(TextType) domainType;
				assertEquals(10, textType.getMaxLength());
			}
			// attribute17
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr17");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TypeAlias);
				Type domainType=attribute.getDomain();
				TypeAlias typeAlias=(TypeAlias) domainType;
				assertEquals(td.INTERLIS.XmlDate.getName(), typeAlias.getAliasing().getName());
			}
			// attribute18
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr18");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TypeAlias);
				Type domainType=attribute.getDomain();
				TypeAlias typeAlias=(TypeAlias) domainType;
				assertEquals(td.INTERLIS.XmlTime.getName(), typeAlias.getAliasing().getName());
			}
			// attribute19
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr19");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TypeAlias);
				Type domainType=attribute.getDomain();
				TypeAlias typeAlias=(TypeAlias) domainType;
				assertEquals(td.INTERLIS.XmlTime.getName(), typeAlias.getAliasing().getName());
			}
			// attribute20
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr20");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TypeAlias);
				Type domainType=attribute.getDomain();
				TypeAlias typeAlias=(TypeAlias) domainType;
				assertEquals(td.INTERLIS.XmlDateTime.getName(), typeAlias.getAliasing().getName());
			}
			// attribute21
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr21");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TypeAlias);
				Type domainType=attribute.getDomain();
				TypeAlias typeAlias=(TypeAlias) domainType;
				assertEquals(td.INTERLIS.XmlDateTime.getName(), typeAlias.getAliasing().getName());
			}
			// attribute22
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr22");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TypeAlias);
				Type domainType=attribute.getDomain();
				TypeAlias typeAlias=(TypeAlias) domainType;
				assertEquals(td.INTERLIS.BOOLEAN.getName(), typeAlias.getAliasing().getName());
			}
			// attribute23
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr23");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.BlackboxType);
				Type domainType=attribute.getDomain();
				BlackboxType blackboxType=(BlackboxType) domainType;
				assertEquals(ch.interlis.ili2c.metamodel.BlackboxType.eBINARY, blackboxType.getKind());
			}
			// attribute24
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr24");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.BlackboxType);
				Type domainType=attribute.getDomain();
				BlackboxType blackboxType=(BlackboxType) domainType;
				assertEquals(ch.interlis.ili2c.metamodel.BlackboxType.eBINARY, blackboxType.getKind());
			}
			// attribute25
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr25");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TypeAlias);
				Type domainType=attribute.getDomain();
				TypeAlias typeAlias=(TypeAlias) domainType;
				assertEquals(td.INTERLIS.UUIDOID.getName(), typeAlias.getAliasing().getName());
			}
			// attribute26
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr26");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.BlackboxType);
				Type domainType=attribute.getDomain();
				BlackboxType blackboxType=(BlackboxType) domainType;
				assertEquals(ch.interlis.ili2c.metamodel.BlackboxType.eXML, blackboxType.getKind());
			}
			// attribute27
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr27");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.TypeAlias);
				Type domainType=attribute.getDomain();
				TypeAlias typeAlias=(TypeAlias) domainType;
				assertEquals("lcoord2056", typeAlias.getAliasing().getName());
				
			}
			// attribute28
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr28");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.PolylineType);
				Type domainType=attribute.getDomain();
				PolylineType lineType=(PolylineType) domainType;
				LineForm[] lineForm=lineType.getLineForms();
				assertEquals(td.INTERLIS.STRAIGHTS.getName(),lineForm[0].getName());
				assertEquals(1,lineForm.length);
				Domain controlPointType=lineType.getControlPointDomain();
				assertEquals("lcoord2056",controlPointType.getName());
				assertEquals(0,lineType.getMaxOverlap().compareTo(new PrecisionDecimal(0.01)));
			}
			// attribute29
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr29");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.SurfaceType);
				Type domainType=attribute.getDomain();
				SurfaceType surfaceType=(SurfaceType) domainType;
				LineForm[] lineForm=surfaceType.getLineForms();
				assertEquals(td.INTERLIS.STRAIGHTS.getName(),lineForm[0].getName());
				assertEquals(1,lineForm.length);
				Domain controlPointType=surfaceType.getControlPointDomain();
				assertEquals("lcoord2056",controlPointType.getName());
				assertEquals(0,surfaceType.getMaxOverlap().compareTo(new PrecisionDecimal(0.01)));
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}
	
	// Es wird 1 Tabelle innerhalb des definierten Schemas erstellt.
	// Innerhalb dieser Tabellen werden Koordinaten, Linien und Oberflaechen mit 3 verschiedenen EPSG Code's erstellt.
	// Die Tabelle wird mit den DatenTypen in die ili Datei exportiert.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Alle AttributeTypen sollen erkannt und geschrieben werden.
	@Test
	public void export_export_DifferentEPSGCoords_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		final File iliFile=new File(TEST_OUT+"export_DifferentEPSGCoords_Ok.ili");
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoilischema
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	// create dbtoilischema
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	// create table in dbtoilischema
	        	try {
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"("
							+ "attr1 geometry(POINT,2056) NOT NULL,"
							+ "attr2 geometry(LINESTRING,2056),"
							+ "attr3 geometry(POLYGON,2056),"
							+ "attr4 geometry(POINT,21781) NOT NULL,"
							+ "attr5 geometry(LINESTRING,21781),"
							+ "attr6 geometry(POLYGON,21781),"
							+ "attr7 geometry(POINT,3333) NOT NULL,"
							+ "attr8 geometry(LINESTRING,3333),"
							+ "attr9 geometry(POLYGON,3333)"
							+ ")WITH (OIDS=FALSE);");
		        	preStmt.close();
	        	}catch(Exception e) {
	        		throw new IoxException(e);
	        	}
	        }
	        {
				// delete file if already exist
				if(iliFile.exists()) {
					iliFile.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(iliFile, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFilename=TEST_OUT+"export_DifferentEPSGCoords_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFilename);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			// attribute1
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr2");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.PolylineType);
				Type domainType=attribute.getDomain();
				PolylineType lineType=(PolylineType) domainType;
				LineForm[] lineForm=lineType.getLineForms();
				assertEquals(td.INTERLIS.STRAIGHTS.getName(),lineForm[0].getName());
				assertEquals(1,lineForm.length);
				Domain controlPointType=lineType.getControlPointDomain();
				assertEquals("lcoord2056",controlPointType.getName());
				assertEquals(0,lineType.getMaxOverlap().compareTo(new PrecisionDecimal(0.01)));
			}
			// attribute4
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr5");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.PolylineType);
				Type domainType=attribute.getDomain();
				PolylineType lineType=(PolylineType) domainType;
				LineForm[] lineForm=lineType.getLineForms();
				assertEquals(td.INTERLIS.STRAIGHTS.getName(),lineForm[0].getName());
				assertEquals(1,lineForm.length);
				Domain controlPointType=lineType.getControlPointDomain();
				assertEquals("lcoord21781",controlPointType.getName());
				assertEquals(0,lineType.getMaxOverlap().compareTo(new PrecisionDecimal(0.01)));
			}
			// attribute7
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr8");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.PolylineType);
				Type domainType=attribute.getDomain();
				PolylineType lineType=(PolylineType) domainType;
				LineForm[] lineForm=lineType.getLineForms();
				assertEquals(td.INTERLIS.STRAIGHTS.getName(),lineForm[0].getName());
				assertEquals(1,lineForm.length);
				Domain controlPointType=lineType.getControlPointDomain();
				assertEquals("lcoord3333",controlPointType.getName());
				assertEquals(0,lineType.getMaxOverlap().compareTo(new PrecisionDecimal(0.01)));
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}
	
	// export tests of curve geometries.
	@Test
	public void export_CurveGeometries_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		final String iliFilename=TEST_OUT+"export_CurveGeometries_Ok.ili";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoshpschema
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	// create dbtoshpschema
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	// CREATE TABLE dbtoshpschema.in dbtoshpschema
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"("
	        			+ "attr1 geometry(COMPOUNDCURVE,2056),"
	        			+ "attr2 geometry(CURVEPOLYGON,2056)"
	        			+ ") WITH (OIDS=FALSE);");
	        	preStmt.close();
	        }
	        {
				File iliFile=new File(iliFilename);
				// delete file if already exist
				if(iliFile.exists()) {
					iliFile.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(iliFile, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFilename);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			// attribute1
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr1");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.PolylineType);
				Type domainType=attribute.getDomain();
				PolylineType lineType=(PolylineType) domainType;
				LineForm[] lineForm=lineType.getLineForms();
				assertEquals(td.INTERLIS.STRAIGHTS.getName(),lineForm[0].getName());
				assertEquals(td.INTERLIS.ARCS.getName(),lineForm[1].getName());
				Domain controlPointType=lineType.getControlPointDomain();
				assertEquals("lcoord2056",controlPointType.getName());
				assertEquals(0,lineType.getMaxOverlap().compareTo(new PrecisionDecimal(0.01)));
			}
			// attribute2
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr2");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.SurfaceType);
				Type domainType=attribute.getDomain();
				SurfaceType surfaceType=(SurfaceType) domainType;
				LineForm[] lineForm=surfaceType.getLineForms();
				assertEquals(td.INTERLIS.STRAIGHTS.getName(),lineForm[0].getName());
				assertEquals(td.INTERLIS.ARCS.getName(),lineForm[1].getName());
				Domain controlPointType=surfaceType.getControlPointDomain();
				assertEquals("lcoord2056",controlPointType.getName());
				assertEquals(0,surfaceType.getMaxOverlap().compareTo(new PrecisionDecimal(0.01)));
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}
	
	// Es wird 1 Tabelle mit einem Tabellenkommentar innerhalb des definierten Schemas erstellt.
	// Innerhalb dieser Tabelle sollen alle Attribute einen Kommentar enthalten.
	// Der Tabellenkommentar wird mit den Attributekommentaren als ili Kommentare in die ili Datei exportiert.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Alle Attributekommentare sollen als ili Kommentar erstellt werden.
	@Test
	public void export_AttributeDescriptionIliFile_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		final File iliFile=new File(TEST_OUT+"export_AttributeDescriptionIliFile_Ok.ili");
		final String TABLE1DESCRIPTION="This is table1.";
		final String ATTRIBUTE1DESCRIPTION="This is attribute1.";
		final String ATTRIBUTE2DESCRIPTION="This is attribute2.";
		final String ATTRIBUTE3DESCRIPTION="This is attribute3.";
		final String ATTRIBUTE4DESCRIPTION="This is attribute4.";
		final String ATTRIBUTE5DESCRIPTION="This is attribute5.";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoilischema
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	// create dbtoilischema
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	// create table in dbtoilischema
	        	try {
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"("
		        			+ "attr1 smallint NOT NULL,"
		        			+ "attr2 integer,"
		        			+ "attr3 bigint,"
		        			+ "attr4 decimal(10),"
		        			+ "attr5 numeric(20)"
							+ ")WITH (OIDS=FALSE);");
		        	preStmt.execute("COMMENT ON TABLE "+SCHEMANAME+"."+TABLE1+" IS '"+TABLE1DESCRIPTION+"';");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE1+".attr1 is '"+ATTRIBUTE1DESCRIPTION+"';");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE1+".attr2 is '"+ATTRIBUTE2DESCRIPTION+"';");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE1+".attr3 is '"+ATTRIBUTE3DESCRIPTION+"';");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE1+".attr4 is '"+ATTRIBUTE4DESCRIPTION+"';");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE1+".attr5 is '"+ATTRIBUTE5DESCRIPTION+"';");
		        	preStmt.close();
	        	}catch(Exception e) {
	        		throw new IoxException(e);
	        	}
	        }
	        {
				// delete file if already exist
				if(iliFile.exists()) {
					iliFile.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(iliFile, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFilename=TEST_OUT+"export_AttributeDescriptionIliFile_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFilename);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			assertNotNull(td);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertEquals(TABLE1DESCRIPTION, table1.getDocumentation());
			assertNotNull(table1);
			// attribute1
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr1");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				assertEquals(ATTRIBUTE1DESCRIPTION, attribute.getDocumentation());
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal(-32768)));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal(32767)));
			}
			// attribute2
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr2");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				assertEquals(ATTRIBUTE2DESCRIPTION, attribute.getDocumentation());
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal(-2147483648)));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal(2147483647)));
			}
			// attribute3
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr3");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				assertEquals(ATTRIBUTE3DESCRIPTION, attribute.getDocumentation());
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal("-9223372036854775808")));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal("9223372036854775807")));
			}
			// attribute4
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr4");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				assertEquals(ATTRIBUTE4DESCRIPTION, attribute.getDocumentation());
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal(-10)));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal(10)));
			}
			// attribute5
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, "attr5");
				assertNotNull(attribute);
				assertTrue(attribute.getDomain() instanceof ch.interlis.ili2c.metamodel.NumericType);
				assertEquals(ATTRIBUTE5DESCRIPTION, attribute.getDocumentation());
				Type domainType=attribute.getDomain();
				NumericType numType=(NumericType) domainType;
				assertEquals(0,numType.getMinimum().compareTo(new PrecisionDecimal(-20)));
				assertEquals(0, numType.getMaximum().compareTo(new PrecisionDecimal(20)));
			}
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}
	
	// Es werden 2 Tabellen innerhalb des definierten Schemas erstellt.
	// Innerhalb der 2.ten Tabelle wird 1 Attribute mit einer Reference die 1.te Tabelle erstellt.
	// Beim Export in die ili Datei, wird eine 3.te Tabelle: ASSOCIATION mit der richtigen Kardinalitaet erstellt.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Die Association Klasse und die Referencen sollen nach der ili-Syntax erstellt werden.
	// Erwartung: Die Minimum-Kardinalitaet betraegt: 0..* zu 0..1.
	@Test
	public void export_Association_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema1";
		final String TABLE1="schueler";
		final String TABLE2="lehrer";
		final String ATTRIBUTEPKDESCRIPTION="This attribute is a primary key.";
		final String ATTRPK="id";
		final String ATTRIBUTEFKDESCRIPTION="This attribute is a foreign key.";
		final String ATTRFK="schueler";
		final File iliFile=new File(TEST_OUT+"export_Association_Ok.ili");
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoilischema
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	// create dbtoilischema
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	// create table in dbtoilischema
	        	try {
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"("
		        			+ATTRPK+" int4 NOT NULL,PRIMARY KEY ("+ATTRPK+")) WITH (OIDS=FALSE);");
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE2+"("+ATTRFK+" int4,"
		        			+ "FOREIGN KEY ("+ATTRFK+") REFERENCES "+SCHEMANAME+"."+TABLE1+" ("+ATTRPK+")) WITH (OIDS=FALSE);");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE1+"."+ATTRPK+" is '"+ATTRIBUTEPKDESCRIPTION+"';");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE2+"."+ATTRFK+" is '"+ATTRIBUTEFKDESCRIPTION+"';");
		        	preStmt.close();
	        	}catch(Exception e) {
	        		throw new IoxException(e);
	        	}
	        }
	        {
				// delete file if already exist
				if(iliFile.exists()) {
					iliFile.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(iliFile, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFilename=TEST_OUT+"export_Association_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFilename);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			assertNotNull(td);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			// attribute1
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, ATTRPK);
				assertEquals(ATTRIBUTEPKDESCRIPTION, attribute.getDocumentation());
				assertNotNull(attribute);
			}
			Table table2=(Table) topic.getElement(Table.class, TABLE2);
			assertNotNull(table2);
			AssociationDef modelAssociationDef = (AssociationDef) topic.getElement(AssociationDef.class, TABLE2+ATTRFK);
			RoleDef role1=(RoleDef) modelAssociationDef.getElement(RoleDef.class, "object");
			assertNotNull(role1);
			RoleDef role2=(RoleDef) modelAssociationDef.getElement(RoleDef.class, ATTRFK);
			assertNotNull(role2);
			// cardinality 0..*
			assertEquals(0,role1.getCardinality().getMinimum());
			assertEquals(Cardinality.UNBOUND,role1.getCardinality().getMaximum());
			// cardinality 0..1
			assertEquals(0,role2.getCardinality().getMinimum());
			assertEquals(1,role2.getCardinality().getMaximum());
			assertEquals(TABLE2,role1.getDestination().getName());
			assertEquals(TABLE1,role2.getDestination().getName());
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}
	
	// Es werden 2 Tabellen innerhalb des definierten Schemas erstellt.
	// Innerhalb der 2.ten Tabelle wird 1 Attribute mit einer Reference die 1.te Tabelle erstellt.
	// Beim Export in die ili Datei, wird eine 3.te Tabelle: ASSOCIATION mit der richtigen Kardinalitaet 1..1 zu 0..* erstellt.
	// Dabei darf keine Fehlermeldung ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: Die Kardinalitaet betraegt: 1..1 zu 0..*;.
	@Test
	public void export_Association_fkNotNull_Ok() throws Exception
	{
		final String SCHEMANAME="dbtoilischema1";
		final String TABLE1="schueler";
		final String TABLE2="lehrer";
		final String ATTRIBUTEPKDESCRIPTION="This attribute is a primary key.";
		final String ATTRPK="id";
		final String ATTRIBUTEFKDESCRIPTION="This attribute is a foreign key.";
		final String ATTRFK="schueler";
		final File iliFile=new File(TEST_OUT+"export_Association_fkNotNull_Ok.ili");
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	// drop dbtoilischema
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	// create dbtoilischema
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	// create table in dbtoilischema
	        	try {
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"("
		        			+ATTRPK+" int4 NOT NULL,PRIMARY KEY ("+ATTRPK+")) WITH (OIDS=FALSE);");
		        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE2+"("+ATTRFK+" int4 NOT NULL,"
		        			+ "FOREIGN KEY ("+ATTRFK+") REFERENCES "+SCHEMANAME+"."+TABLE1+" ("+ATTRPK+")) WITH (OIDS=FALSE);");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE1+"."+ATTRPK+" is '"+ATTRIBUTEPKDESCRIPTION+"';");
		        	preStmt.execute("COMMENT ON COLUMN "+SCHEMANAME+"."+TABLE2+"."+ATTRFK+" is '"+ATTRIBUTEFKDESCRIPTION+"';");
		        	preStmt.close();
	        	}catch(Exception e) {
	        		throw new IoxException(e);
	        	}
	        }
	        {
				// delete file if already exist
				if(iliFile.exists()) {
					iliFile.delete();
				}
				config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
				Db2Ili db2Ili=new Db2Ili();
				db2Ili.exportData(iliFile, jdbcConnection, config);
			}
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
		try{
			// model compile test
			String iliFilename=TEST_OUT+"export_Association_fkNotNull_Ok.ili";
			ArrayList ilifiles=new ArrayList();
			ilifiles.add(iliFilename);
			TransferDescription td=ch.interlis.ili2c.Main.compileIliFiles(ilifiles, null, null);
			assertNotNull(td);
			Topic topic = (Topic) ((Container<Element>) td.getElement(Model.class, SCHEMANAME)).getElement(Topic.class, Db2Ili.TOPICNAME);
			Table table1=(Table) topic.getElement(Table.class, TABLE1);
			assertNotNull(table1);
			// attribute1
			{
				AttributeDef attribute=(AttributeDef) table1.getElement(AttributeDef.class, ATTRPK);
				assertEquals(ATTRIBUTEPKDESCRIPTION, attribute.getDocumentation());
				assertNotNull(attribute);
			}
			Table table2=(Table) topic.getElement(Table.class, TABLE2);
			assertNotNull(table2);
			AssociationDef modelAssociationDef = (AssociationDef) topic.getElement(AssociationDef.class, TABLE2+ATTRFK);
			RoleDef role1=(RoleDef) modelAssociationDef.getElement(RoleDef.class, "object");
			assertNotNull(role1);
			RoleDef role2=(RoleDef) modelAssociationDef.getElement(RoleDef.class, ATTRFK);
			assertNotNull(role2);
			// cardinality 1..1
			assertEquals(0,role1.getCardinality().getMinimum());
			assertEquals(Cardinality.UNBOUND,role1.getCardinality().getMaximum());
			// cardinality 0..*
			assertEquals(1,role2.getCardinality().getMinimum());
			assertEquals(1,role2.getCardinality().getMaximum());
			assertEquals(TABLE2,role1.getDestination().getName());
			assertEquals(TABLE1,role2.getDestination().getName());
		}catch(Exception e) {
			throw new IoxException(e);
		}
	}

	// Die ili-Datei wird fuer den Export nicht angegeben.
	// Eine Fehlermeldung muss ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: file==null.
	@Test
	public void export_FileNotDefined_Fail() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
	        	preStmt.close();
	        }
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(null, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertEquals("file==null", e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Die Connection wird dem Export nicht mitgegeben.
	// Eine Fehlermeldung muss ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: connection==null.
	@Test
	public void export_ConnectionFailed_Fail() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
	        	preStmt.close();
	        }
	        File data=new File(TEST_OUT+"export_ConnectionFailed_Fail.ili");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, SCHEMANAME);
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(data, null, config);
			fail();
		}catch(IoxException e) {
			assertEquals("connection==null", e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
	
	// Das DB-Schema wird dem Export nicht mitgegeben.
	// Eine Fehlermeldung muss ausgegeben werden.
	// Test-Konfiguration:
	// - set: dbtoilischema.
	// --
	// Erwartung: db schema name==null.
	@Test
	public void export_DbSchemaNotDefined_Fail() throws Exception
	{
		final String SCHEMANAME="dbtoilischema";
		final String TABLE1="table1";
		Settings config=new Settings();
		Connection jdbcConnection=null;
		try{
	        Class driverClass = Class.forName("org.postgresql.Driver");
	        jdbcConnection = DriverManager.getConnection(dburl, dbuser, dbpwd);
	        {
	        	Statement preStmt=jdbcConnection.createStatement();
	        	preStmt.execute("DROP SCHEMA IF EXISTS "+SCHEMANAME+" CASCADE");
	        	preStmt.execute("CREATE SCHEMA "+SCHEMANAME);
	        	preStmt.execute("CREATE TABLE "+SCHEMANAME+"."+TABLE1+"();");
	        	preStmt.close();
	        }
	        File data=new File(TEST_OUT+"export_DbSchemaNotDefined_Fail.ili");
			// delete file if already exist
			if(data.exists()) {
				data.delete();
			}
			Db2Ili db2Ili=new Db2Ili();
			db2Ili.exportData(data, jdbcConnection, config);
			fail();
		}catch(IoxException e) {
			assertEquals("db schema name==null", e.getMessage());
		}finally{
			if(jdbcConnection!=null){
				jdbcConnection.close();
			}
		}
	}
}