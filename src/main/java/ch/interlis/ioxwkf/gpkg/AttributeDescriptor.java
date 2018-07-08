package ch.interlis.ioxwkf.gpkg;

public class AttributeDescriptor {
    private String dbColumnName;
    private int dbColumnType;
    private String dbColumnTypeName;
    private boolean geometryAttribute;
    
    public String getDbColumnName() {
        return this.dbColumnName;
    }
    
    public void setDbColumnName(String dbColumnName) {
        this.dbColumnName = dbColumnName;
    }
    
    public int getDbColumnType() {
        return this.dbColumnType;
    }
    
    public void setDbColumnType(int dbColumnType) {
        this.dbColumnType = dbColumnType;
    }
    
    public String getDbColumnTypeName() {
        return this.dbColumnTypeName;
    }
    
    public void setDbColumnTypeName(String dbColumnTypeName) {
        this.dbColumnTypeName = dbColumnTypeName;
    }
    
    public boolean isGeometry() {
        return this.geometryAttribute;
    }
    
    public void setGeometry(boolean geometryAttribute) {
        this.geometryAttribute = geometryAttribute;
    }
}
