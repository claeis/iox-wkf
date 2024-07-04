package ch.interlis.ioxwkf.jts;

import java.io.IOException;

public class JtsPackageConverter {
    public static org.locationtech.jts.geom.Geometry toNewPackage(com.vividsolutions.jts.geom.Geometry oldGeom) throws IOException {
        if (oldGeom == null) {
            return null;
        }
        try {
            org.locationtech.jts.io.WKBReader wkbReader = new org.locationtech.jts.io.WKBReader();
            com.vividsolutions.jts.io.WKBWriter wkbWriter = new com.vividsolutions.jts.io.WKBWriter();
            org.locationtech.jts.geom.Geometry newGeom = wkbReader.read(wkbWriter.write(oldGeom));
            return newGeom;
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new IOException(e);
        } 
    }
    
    public static org.locationtech.jts.geom.Coordinate toNewPackage(com.vividsolutions.jts.geom.Coordinate oldCoord) throws IOException {
        if (oldCoord == null) {
            return null;
        } 
        
        if (Double.valueOf(oldCoord.z) != null) {
            return new org.locationtech.jts.geom.Coordinate(oldCoord.x, oldCoord.y, oldCoord.z);    
        } else {
            return new org.locationtech.jts.geom.Coordinate(oldCoord.x, oldCoord.y);    
        }        
    }

    public static org.locationtech.jts.geom.CoordinateList toNewPackage(com.vividsolutions.jts.geom.CoordinateList oldCoords) throws IOException {
        if (oldCoords == null) {
            return null;
        }
        
        org.locationtech.jts.geom.CoordinateList newCoords = new org.locationtech.jts.geom.CoordinateList();
        for (int i=0; i<oldCoords.size(); i++) {
            org.locationtech.jts.geom.Coordinate newCoord = new org.locationtech.jts.geom.Coordinate(
                    ((com.vividsolutions.jts.geom.Coordinate)oldCoords.get(i)).x, 
                    ((com.vividsolutions.jts.geom.Coordinate)oldCoords.get(i)).y,
                    ((com.vividsolutions.jts.geom.Coordinate)oldCoords.get(i)).z);
            newCoords.add(newCoord);
        }
        return newCoords;
    }
    
    public static org.locationtech.jts.geom.Polygon toNewPackage(com.vividsolutions.jts.geom.Polygon oldGeom) throws IOException {
        return (org.locationtech.jts.geom.Polygon) toNewPackage((com.vividsolutions.jts.geom.Geometry) oldGeom);        
    }

    public static org.locationtech.jts.geom.MultiPolygon toNewPackage(com.vividsolutions.jts.geom.MultiPolygon oldGeom) throws IOException {
        return (org.locationtech.jts.geom.MultiPolygon) toNewPackage((com.vividsolutions.jts.geom.Geometry) oldGeom);        
    }    
    
    public static org.locationtech.jts.geom.MultiPoint toNewPackage(com.vividsolutions.jts.geom.MultiPoint oldGeom) throws IOException {
        return (org.locationtech.jts.geom.MultiPoint) toNewPackage((com.vividsolutions.jts.geom.Geometry) oldGeom);        
    }

    public static org.locationtech.jts.geom.MultiLineString toNewPackage(com.vividsolutions.jts.geom.MultiLineString oldGeom) throws IOException {
        return (org.locationtech.jts.geom.MultiLineString) toNewPackage((com.vividsolutions.jts.geom.Geometry) oldGeom);        
    }
    
    public static com.vividsolutions.jts.geom.Geometry toOldPackage(org.locationtech.jts.geom.Geometry newGeom) throws IOException {
        if (newGeom == null) {
            return null;
        }
        try {
            com.vividsolutions.jts.io.WKBReader wkbReader = new com.vividsolutions.jts.io.WKBReader();
            org.locationtech.jts.io.WKBWriter wkbWriter = new org.locationtech.jts.io.WKBWriter();
            com.vividsolutions.jts.geom.Geometry oldGeom = wkbReader.read(wkbWriter.write(newGeom));            
            return oldGeom;
        } catch (com.vividsolutions.jts.io.ParseException e) {
            throw new IOException(e);
        } 
    }
    
    public static com.vividsolutions.jts.geom.LineString toOldPackage(org.locationtech.jts.geom.LineString newGeom) throws IOException {
        return (com.vividsolutions.jts.geom.LineString) toOldPackage((org.locationtech.jts.geom.Geometry) newGeom);
    }

    public static com.vividsolutions.jts.geom.MultiLineString toOldPackage(org.locationtech.jts.geom.MultiLineString newGeom) throws IOException {
        return (com.vividsolutions.jts.geom.MultiLineString) toOldPackage((org.locationtech.jts.geom.Geometry) newGeom);
    }

    public static com.vividsolutions.jts.geom.Polygon toOldPackage(org.locationtech.jts.geom.Polygon newGeom) throws IOException {
        return (com.vividsolutions.jts.geom.Polygon) toOldPackage((org.locationtech.jts.geom.Geometry) newGeom);
    }

    public static com.vividsolutions.jts.geom.MultiPolygon toOldPackage(org.locationtech.jts.geom.MultiPolygon newGeom) throws IOException {
        return (com.vividsolutions.jts.geom.MultiPolygon) toOldPackage((org.locationtech.jts.geom.Geometry) newGeom);
    }

    public static com.vividsolutions.jts.geom.Coordinate toOldPackage(org.locationtech.jts.geom.Coordinate newCoord) {
        if (newCoord == null) {
            return null;
        }
        com.vividsolutions.jts.geom.Coordinate oldCoord = new com.vividsolutions.jts.geom.Coordinate(newCoord.getX(), newCoord.getY(), newCoord.getZ());
        return oldCoord;
    }

    public static com.vividsolutions.jts.geom.Coordinate[] toOldPackage(org.locationtech.jts.geom.Coordinate[] newCoords) {
        if (newCoords == null) {
            return null;
        }
        com.vividsolutions.jts.geom.CoordinateList oldCoords = new com.vividsolutions.jts.geom.CoordinateList();
        for (int i=0; i<newCoords.length; i++) {
            com.vividsolutions.jts.geom.Coordinate oldCoord = new com.vividsolutions.jts.geom.Coordinate(newCoords[i].getX(), newCoords[i].getY(), newCoords[i].getZ());
            oldCoords.add(oldCoord);
        }
        return oldCoords.toCoordinateArray();
    }
}
