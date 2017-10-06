/* This file is part of the iox-wkf project.
 * For more information, please see <http://www.eisenhutinformatik.ch/iox-wkf/>.
 *
 * Copyright (c) 2006 Eisenhut Informatik AG
 * Permission is hereby granted, free of charge, to any person obtaining a 
 * copy of this software and associated documentation files (the "Software"), 
 * to deal in the Software without restriction, including without limitation 
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included 
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
 * DEALINGS IN THE SOFTWARE.
 */
package ch.interlis.iom_j.converter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;

import ch.interlis.iom.IomConstants;
import ch.interlis.iom.IomObject;
import ch.interlis.iox_j.jts.Iox2jtsException;

/** Utility to convert from INTERLIS to JTS geometry types.
 * @author ceis
 */
public class Iox2multijts {
	// utility, no instances
	private Iox2multijts(){}
	private static double dist(double re1,double ho1,double re2,double ho2)
	{
		double ret;
		ret=Math.hypot(re2-re1,ho2-ho1);
		return ret;
	}
	
	public static com.vividsolutions.jts.geom.CoordinateList multiCoord2JTS(IomObject value) throws Iox2jtsException {
		CoordinateList ret=new CoordinateList();
		for(int segmenti=0;segmenti<value.getattrvaluecount("coord");segmenti++){
			IomObject segment=value.getattrobj("coord",segmenti);
			if(segment.getobjecttag().equals("COORD")){
				// COORD
				ret.add(coord2JTS(segment));
			}else{
				// custum line form
				throw new Iox2jtsException("custom coord form not supported");
			}
		}
		return ret;
	}
	
	public static com.vividsolutions.jts.geom.LineString[] multiLineString2JTS(IomObject value) throws Iox2jtsException {
		LineString[] ret=new LineString[value.getattrvaluecount("polyline")];
		for(int polylinei=0;polylinei<value.getattrvaluecount("polyline");polylinei++){
			IomObject polyline=value.getattrobj("polyline",polylinei);
			if(polyline.getobjecttag().equals("POLYLINE")){
				// POLYLINE
				CoordinateList coordList=polyline2JTS(polyline, true, 0.00);
				LineString lineString=new LineString(coordList.toCoordinateArray(), null, polylinei);
				ret[polylinei]=lineString;
			}else{
				// custum line form
				throw new Iox2jtsException("custom multipolyline form not supported");
			}
		}
		return ret;
	}
	
	/** Converts a COORD to a JTS Coordinate.
	 * @param value INTERLIS COORD structure.
	 * @return JTS Coordinate.
	 * @throws Iox2jtsException
	 */
	public static com.vividsolutions.jts.geom.Coordinate coord2JTS(IomObject value) 
	throws Iox2jtsException 
	{
		if(value==null){
			return null;
		}
		String c1=value.getattrvalue("C1");
		String c2=value.getattrvalue("C2");
		String c3=value.getattrvalue("C3");
		double xCoord;
		try{
			xCoord = Double.parseDouble(c1);
		}catch(Exception ex){
			throw new Iox2jtsException("failed to read C1 <"+c1+">",ex);
		}
		double yCoord;
		try{
			yCoord = Double.parseDouble(c2);
		}catch(Exception ex){
			throw new Iox2jtsException("failed to read C2 <"+c2+">",ex);
		}
		com.vividsolutions.jts.geom.Coordinate coord=null;
		if(c3==null){
			coord=new com.vividsolutions.jts.geom.Coordinate(xCoord, yCoord);
		}else{
			double zCoord;
			try{
				zCoord = Double.parseDouble(c3);
			}catch(Exception ex){
				throw new Iox2jtsException("failed to read C3 <"+c3+">",ex);
			}
			coord=new com.vividsolutions.jts.geom.Coordinate(xCoord, yCoord,zCoord);
		}
		return coord;
	}

	/** Converts a POLYLINE to a JTS CoordinateList.
	 * @param polylineObj INTERLIS POLYLINE structure
	 * @param isSurfaceOrArea true if called as part of a SURFACE conversion.
	 * @param p maximum stroke to use when removing ARCs
	 * @return JTS CoordinateList
	 * @throws Iox2jtsException
	 */
	public static com.vividsolutions.jts.geom.CoordinateList polyline2JTS(IomObject polylineObj,boolean isSurfaceOrArea,double p)
	throws Iox2jtsException
	{
		if(polylineObj==null){
			return null;
		}
		com.vividsolutions.jts.geom.CoordinateList ret=new com.vividsolutions.jts.geom.CoordinateList();
		// is POLYLINE?
		if(isSurfaceOrArea){
			IomObject lineattr=polylineObj.getattrobj("lineattr",0);
			if(lineattr!=null){
				//writeAttrs(out,lineattr);
				throw new Iox2jtsException("Lineattributes not supported");							
			}
		}
		boolean clipped=polylineObj.getobjectconsistency()==IomConstants.IOM_INCOMPLETE;
		if(clipped){
			throw new Iox2jtsException("clipped polyline not supported");
		}
		for(int sequencei=0;sequencei<polylineObj.getattrvaluecount("sequence");sequencei++){
			if(clipped){
				//out.startElement(tags::get_CLIPPED(),0,0);
			}else{
				// an unclipped polyline should have only one sequence element
				if(sequencei>0){
					throw new Iox2jtsException("unclipped polyline with multi 'sequence' elements");
				}
			}
			IomObject sequence=polylineObj.getattrobj("sequence",sequencei);
			for(int segmenti=0;segmenti<sequence.getattrvaluecount("segment");segmenti++){
				IomObject segment=sequence.getattrobj("segment",segmenti);
				//EhiLogger.debug("segmenttag "+segment.getobjecttag());
				if(segment.getobjecttag().equals("COORD")){
					// COORD
					ret.add(coord2JTS(segment));
				}else{
					// custum line form
					throw new Iox2jtsException("custom line form not supported");
					//out.startElement(segment->getTag(),0,0);
					//writeAttrs(out,segment);
					//out.endElement(/*segment*/);
				}

			}
			if(clipped){
				//out.endElement(/*CLIPPED*/);
			}
		}
		return ret;
	}
	/** Converts a SURFACE to a JTS Polygon.
	 * @param obj INTERLIS SURFACE structure
	 * @param strokeP maximum stroke to use when removing ARCs
	 * @return JTS Polygon
	 * @throws Iox2jtsException
	 */
	public static com.vividsolutions.jts.geom.Polygon surface2JTS(IomObject obj,double strokeP) //SurfaceOrAreaType type)
	throws Iox2jtsException
	{
		if(obj==null){
			return null;
		}
		com.vividsolutions.jts.geom.Polygon ret=null;
		//IFMEFeatureVector bndries=session.createFeatureVector();
		boolean clipped=obj.getobjectconsistency()==IomConstants.IOM_INCOMPLETE;
		if(clipped){
			throw new Iox2jtsException("clipped surface not supported");
		}
		for(int surfacei=0;surfacei<obj.getattrvaluecount("surface");surfacei++){
			if(clipped){
				//out.startElement("CLIPPED",0,0);
			}else{
				// an unclipped surface should have only one surface element
				if(surfacei>0){
					throw new Iox2jtsException("unclipped surface with multi 'surface' elements");
				}
			}
			IomObject surface=obj.getattrobj("surface",surfacei);
			com.vividsolutions.jts.geom.LinearRing shell=null;
			com.vividsolutions.jts.geom.LinearRing holes[]=null;
			int boundaryc=surface.getattrvaluecount("boundary");
			if(boundaryc>1){
				holes=new com.vividsolutions.jts.geom.LinearRing[boundaryc-1];				
			}
			for(int boundaryi=0;boundaryi<boundaryc;boundaryi++){
				IomObject boundary=surface.getattrobj("boundary",boundaryi);
				//IFMEFeature fmeLine=session.createFeature();
				com.vividsolutions.jts.geom.CoordinateList jtsLine=new com.vividsolutions.jts.geom.CoordinateList();
				for(int polylinei=0;polylinei<boundary.getattrvaluecount("polyline");polylinei++){
					IomObject polyline=boundary.getattrobj("polyline",polylinei);
					jtsLine.addAll(polyline2JTS(polyline,true,strokeP));
				}
				jtsLine.closeRing();
				if(boundaryi==0){
					shell=new com.vividsolutions.jts.geom.GeometryFactory().createLinearRing(jtsLine.toCoordinateArray());
				}else{
					holes[boundaryi-1]=new com.vividsolutions.jts.geom.GeometryFactory().createLinearRing(jtsLine.toCoordinateArray());
				}
				//bndries.append(fmeLine);
			}
			ret=new com.vividsolutions.jts.geom.GeometryFactory().createPolygon(shell,holes);
			if(clipped){
				//out.endElement(/*CLIPPED*/);
			}
		}
		if(obj.getattrvaluecount("boundary")>0) {
			com.vividsolutions.jts.geom.LinearRing shell=null;
			com.vividsolutions.jts.geom.LinearRing holes[]=null;
			int boundaryc=obj.getattrvaluecount("boundary");
			if(boundaryc>1){
				holes=new com.vividsolutions.jts.geom.LinearRing[boundaryc-1];				
			}
			for(int boundaryi=0;boundaryi<boundaryc;boundaryi++){
				IomObject boundary=obj.getattrobj("boundary",boundaryi);
				//IFMEFeature fmeLine=session.createFeature();
				com.vividsolutions.jts.geom.CoordinateList jtsLine=new com.vividsolutions.jts.geom.CoordinateList();
				for(int polylinei=0;polylinei<boundary.getattrvaluecount("polyline");polylinei++){
					IomObject polyline=boundary.getattrobj("polyline",polylinei);
					jtsLine.addAll(polyline2JTS(polyline,true,strokeP));
				}
				jtsLine.closeRing();
				if(boundaryi==0){
					shell=new com.vividsolutions.jts.geom.GeometryFactory().createLinearRing(jtsLine.toCoordinateArray());
				}else{
					holes[boundaryi-1]=new com.vividsolutions.jts.geom.GeometryFactory().createLinearRing(jtsLine.toCoordinateArray());
				}
				//bndries.append(fmeLine);
			}
			ret=new com.vividsolutions.jts.geom.GeometryFactory().createPolygon(shell,holes);
			if(clipped){
				//out.endElement(/*CLIPPED*/);
			}
		}
		return ret;
	}

}
