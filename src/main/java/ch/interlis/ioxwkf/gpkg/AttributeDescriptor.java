package ch.interlis.ioxwkf.gpkg;

public class AttributeDescriptor {
    private String dbColumnName;
    private int dbColumnType;
    private String dbColumnTypeName;
    private boolean isGeometryAttribute = false;
    private BoundingBox bbox;
    private int srsId = -1;
    private boolean is3DGeometry = false;
    private double maxOverlap;
    
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
        return this.isGeometryAttribute;
    }
    
    public void setGeometry(boolean isGeometryAttribute) {
        this.isGeometryAttribute = isGeometryAttribute;
    }

	public BoundingBox getBbox() {
		return bbox;
	}

	public void setBbox(BoundingBox bbox) {
		this.bbox = bbox;
	}
    
	public int getSrsId() {
		return srsId;
	}
	
	public void setSrsId(int srsId) {
		this.srsId = srsId;
	}
	
	public boolean is3D() {
		return this.is3DGeometry;
	}
	
	public void set3D(boolean is3DGeometry) {
		this.is3DGeometry = is3DGeometry;
	}
	
	public double getMaxOverlap() {
		return maxOverlap;
	}
	
	public void setMaxOverlap(double maxOverlap) {
		this.maxOverlap = maxOverlap;
	}

}
