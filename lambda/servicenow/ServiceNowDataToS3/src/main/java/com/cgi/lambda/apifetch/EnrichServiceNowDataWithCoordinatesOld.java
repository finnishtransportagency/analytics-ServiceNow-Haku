package com.cgi.lambda.apifetch;

import org.osgeo.proj4j.CRSFactory;
import org.osgeo.proj4j.CoordinateReferenceSystem;
import org.osgeo.proj4j.CoordinateTransform;
import org.osgeo.proj4j.CoordinateTransformFactory;
import org.osgeo.proj4j.ProjCoordinate;

import com.amazonaws.services.lambda.runtime.Context;
//import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Class to add wgs84 coordinates to JSON formed data gotten from Servicenow
 * @author alapeijario
 *
 */

public class EnrichServiceNowDataWithCoordinatesOld {
	CoordinateTransformFactory ctFactory = new CoordinateTransformFactory(); 
	CRSFactory csFactory = new CRSFactory();
	CoordinateReferenceSystem sourceCRS; 
	CoordinateReferenceSystem targestCRS; 
	CoordinateTransform transformer;
	
public 	ArrayList<JSONObject> EnrichedList= new ArrayList<JSONObject>();
	String data;
	//private Context context;
	private int limit;

/**
 * As parameters we JSON data from ServiceNow and  current Coordinate Reference System.
 * @param context Context of the lambda for logging purposes  	
 * @param data Json array from ServceNow
 * @param sourceSCRS which is the current coordinate reference system
 */
	public EnrichServiceNowDataWithCoordinatesOld(Context context, String data, String sourceSCRS, int limit) 
	{
		sourceCRS = csFactory.createFromName(sourceSCRS);
		targestCRS = csFactory.createFromName("EPSG:4326"); //wgs84
		this.data=data;
		//this.context=context;
		transformer=ctFactory.createTransform(sourceCRS, targestCRS);
		this.limit=limit;
	}	
	
	/**
	 * Method to trigger enriching data
	 * @return string with added WGS84-x and WGS84-y for the object
	 */

	public String enrichData() {
		JSONObject records = new JSONObject(data);
		JSONArray rec = new JSONArray();
		JSONArray jsonArray=records.getJSONArray("records");
		//JSONObject records = new JSONObject(data);
		//JSONArray jsonArray = new JSONArray(data);
		ProjCoordinate sourceCP = new ProjCoordinate();
		ProjCoordinate targetCP = new ProjCoordinate();	
		
		int size=jsonArray.length();

		for (int i=0; i<size; i++) 
		{
			//String number ="";
			JSONObject obj = (JSONObject) jsonArray.get(i);
			try {			
			sourceCP.x=obj.getDouble("u_x_coordinate");
			sourceCP.y=obj.getDouble("u_y_coordinate");
			//number =obj.getString("number");
			transformer.transform(sourceCP, targetCP);
			obj.put("WGS84-x",targetCP.x);
			obj.put("WGS84-y", targetCP.y);
			} catch (JSONException e) {
				//context.getLogger().log("Warning: Could not read WGS coordinates to object with number: " + number + "and row number :" +i  );
				obj.put("WGS84-x","");
				obj.put("WGS84-y", "");
			}
			rec.put(obj);
			if (i!=0 && i%limit==0) {
				JSONObject saveobj= new JSONObject();
				saveobj.put("records", rec);
				EnrichedList.add(saveobj);
				JSONArray newrec = new JSONArray();
				rec=newrec;
			}
		}
		if (!rec.isEmpty()) {
		JSONObject saveobj= new JSONObject();
		saveobj.put("records", rec);
		EnrichedList.add(saveobj);
		}
		return EnrichedList.get(0).toString();
	}

}
