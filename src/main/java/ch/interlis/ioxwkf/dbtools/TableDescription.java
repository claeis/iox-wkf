package ch.interlis.ioxwkf.dbtools;

import java.util.List;

/** contains the information about the table.
 */
public class TableDescription {
	public static final String JDBC_GETCOLUMNS_TABLENAME="TABLE_NAME";
	
	// table descriptions
	private String name;
	private String description;
	private List<AttributeDescriptor> attrDesc=null;
	
	private TableDescription() {}
	
	/** create a new table description.
	 * @param name of table
	 * @param description of table
	 */
	public TableDescription(String name, String description) {
		this.name=name;
		this.description=description;
	}
	/** get the name of table.
	 * @return the table name
	 */
	public String getName() {
		return name;
	}
	/** get the description of table.
	 * @return the table description
	 */
	public String getDescription() {
		return description;
	}

	/** these are the attributes of this table.
	 * @return attrDesc, which contains all attributes of this table.
	 */
	public List<AttributeDescriptor> getAttrDesc() {
		return attrDesc;
	}

	/** these are the attributes of this table.
	 * @param attrDesc, the list of all attributes in the table.
	 */
	public void setAttrDesc(List<AttributeDescriptor> attrDesc) {
		this.attrDesc = attrDesc;
	}
}