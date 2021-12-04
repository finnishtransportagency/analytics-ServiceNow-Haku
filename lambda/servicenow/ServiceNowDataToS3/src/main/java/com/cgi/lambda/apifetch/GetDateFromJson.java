package com.cgi.lambda.apifetch;

import org.json.JSONObject;

public class GetDateFromJson {

	
	private String json;
	
	public GetDateFromJson(String Json) {
		this.json=Json;
		
	}
	
	
	public String getfromJson() {
		try {
		JSONObject inputdata = new JSONObject(json);
		String sdate=(String) inputdata.get("date");
		return sdate;
		} catch (Exception e) {
			return "";
		}
	
	}
	
	
	
}
