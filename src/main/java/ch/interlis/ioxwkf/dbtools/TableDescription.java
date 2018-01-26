package ch.interlis.ioxwkf.dbtools;

/** contains the information about the table.<br>
 * constructor:<br>
 * <li>the name of the table.</li>
 * <li>the description of the table.</li>
 */
public class TableDescription {
	private String name;
	private String description;
	
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
}