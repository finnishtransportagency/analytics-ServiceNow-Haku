package com.cgi.lambda.apifetch;

import org.json.JSONObject;

public class GetDateFromJsonOld {

	
	private String json;
	
	public GetDateFromJsonOld(String Json) {
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
