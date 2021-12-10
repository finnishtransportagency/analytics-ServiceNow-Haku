package com.cgi.lambda.apifetch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import org.joda.time.DateTime;

public class TestServiceNowApiFetch implements SimpleLogger, SimpleWriter {

	
	
	public static void main(String[] args) {


		TestServiceNowApiFetch t = new TestServiceNowApiFetch();
		
		t.test();
		
		
		

	}

	
	
	
	public void test() {

		String username = "case.reader";
		String password = "xo8bmhQRyTlL8RBaVgK0";
		String url = "https://liikennevirasto.service-now.com/api/now/table/task";
		
		String queryStringDefault = "sysparm_query=sys_class_name%3Dsn_customerservice_casesys_updated_onONYesterday%40javascript%3Ags.beginningOfYesterday()%40javascript%3Ags.endOfYesterday()%5EORsys_created_onONYesterday%40javascript%3Ags.beginningOfYesterday()%40javascript%3Ags.endOfYesterday()&sysparm_display_value=true";
		String queryStringDate    = "sysparm_query=sys_class_name%3Dsn_customerservice_case%5Esys_created_onON{DATEFILTER}@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27start%27)@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27end%27)&sysparm_display_value=true";

		String argOffset = "&sysparm_offset=";
		String argLimit = "&sysparm_limit=";

		Integer increment = Integer.valueOf(1000);
		Integer outputSplitLimit = Integer.valueOf(500);
		boolean coordinateTransform = true;
		
		String startDateStr = "2021-10-29";
		String endDateStr = "2021-10-29";
		
		String sourceName = "servicenow_u_case";
		
		ServiceNowApiFetch api = new ServiceNowApiFetch(this, this, username, password, url,
				queryStringDefault, queryStringDate, argOffset, argLimit, increment, outputSplitLimit, coordinateTransform,
				sourceName);
		
		//api.process(startDateStr, endDateStr);
		
		String dataFileName = this.createOutputFileName(sourceName);
		
		String template = "{\"entries\":[],\"columns\":[\"DATA\"]}";
		
		ManifestCreator mfc = new ManifestCreator();
		
		
		String bucket = "c:\\VM\\shared\\vv\\servicenow\\test_out\\";
		String path = "servicenow_u_case";
		String dataFilePath = bucket + path;
		
		
		mfc.createManifest(this, this, template, dataFilePath, dataFileName);

	}
	
	
	
	
	@Override
	public boolean writeData(String fileName, String data) {
		System.out.println("writer: output file '" + fileName + "'");

		String bucket = "c:\\VM\\shared\\vv\\servicenow\\test_out\\";
		String path = "servicenow_u_case";
		String fn = bucket + path + "\\" + fileName;
		String charset = "UTF-8";
		System.out.println("writer: write to file '" + fn + "'");

		File f = new File(bucket + path);
		if (!f.exists()) {
			try {
				f.mkdirs();
			} catch (Exception e) {
				
			}
		}
		
		try {
		    OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(fn), charset);
			writer.write(data);
			writer.flush();
			writer.close();
		
		} catch (Exception e) {
			System.out.println("writer: '" + e.toString() + "', '" + e.getMessage() + "'");
		}
		
		return true;
	}

	

	
	@Override
	public String createOutputFileName(String sourceFileName) {
		String timestamp = "" + (DateTime.now().getMillis() / 1000);
		boolean fullscanned = false;
		String fileName = "table." + sourceFileName + "." + timestamp + ".batch." + timestamp + ".fullscanned." + fullscanned + ".json";
		return fileName;
	}

	@Override
	public void log(String s) {
		System.out.println(s);
	}

}
