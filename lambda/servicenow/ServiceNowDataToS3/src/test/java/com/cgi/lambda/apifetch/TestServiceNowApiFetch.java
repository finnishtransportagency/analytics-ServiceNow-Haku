package com.cgi.lambda.apifetch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import org.joda.time.DateTime;


public class TestServiceNowApiFetch implements SimpleLogger, SimpleWriter {

	
	
	public static void main(String[] args) {

		TestServiceNowApiFetch t = new TestServiceNowApiFetch();
		
		t.test(args[0], args[1], args[2], args[3], args[4], args[5]);

	}

	

	
	private String outputBucket   = "servicenow-data-output-bucket";
	private String outputPath     = "dummy";
	private String manifestBucket = "manifest-output-bucket";
	private String manifestPath   = "manifest";

	private String mockdata = null;
	private String mockmanifest = null;
	
	

	public void test(String username, String password, String url, String sourceName, String mockdata, String mockmanifest) {

		this.mockdata = mockdata;
		this.mockmanifest = mockmanifest;

		this.outputPath = sourceName;
		
		String queryStringDefault = "sysparm_query=sys_class_name%3Dsn_customerservice_casesys_updated_onONYesterday%40javascript%3Ags.beginningOfYesterday()%40javascript%3Ags.endOfYesterday()%5EORsys_created_onONYesterday%40javascript%3Ags.beginningOfYesterday()%40javascript%3Ags.endOfYesterday()&sysparm_display_value=true";
		String queryStringDate    = "sysparm_query=sys_class_name%3Dsn_customerservice_case%5Esys_created_onON{DATEFILTER}@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27start%27)@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27end%27)&sysparm_display_value=true";

		String argOffset = "&sysparm_offset=";
		String argLimit = "&sysparm_limit=";

		Integer increment = Integer.valueOf(1000);
		Integer outputSplitLimit = Integer.valueOf(200);
		boolean coordinateTransform = true;
		
		String startDateStr = "2021-10-29";
		String endDateStr = "2021-10-29";


		String template = "{\"entries\":[],\"columns\":[\"DATA\"]}";
	
		ManifestCreator mfc = new ManifestCreator(this, this);
		mfc.setTemplate(template);
		
		
		ServiceNowApiFetch api = new ServiceNowApiFetch(this, this, username, password, url,
				queryStringDefault, queryStringDate, argOffset, argLimit, increment, outputSplitLimit, coordinateTransform,
				sourceName);
		api.setManifestCreator(mfc);
		api.process(startDateStr, endDateStr);

	}
	
	
	
	
	
	@Override
	public boolean writeDataFile(FileSpec outputFile, String data) {
		String outputBucket   = this.mockdata;

		System.out.println("writer: output file '" + outputFile.fileName + "'");

		String fn = outputBucket + "\\" + outputFile.path + "\\" + outputFile.fileName;
		String charset = "UTF-8";
		System.out.println("writer: write to file '" + fn + "'");

		File f = new File(outputBucket + "\\" + outputFile.path);
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
	public void log(String s) {
		System.out.println(s);
	}




	
	@Override
	public FileSpec makeDataFileName(String sourceName) {
		FileSpec retval = new FileSpec();
		retval.bucket = this.outputBucket;
		retval.path = this.outputPath;
		retval.timestamp = "" + DateTime.now().getMillis();
		retval.sourceName =  sourceName;
		retval.fullscanned = this.isFullscan(sourceName);
		retval.fileName = "table." + sourceName + "." + retval.timestamp + ".batch." + retval.timestamp + ".fullscanned." + retval.fullscanned + ".json";
		return retval; 
	}


	private boolean isFullscan(String sourceName) {
		return false;
	}



	@Override
	public boolean writeManifestFile(FileSpec outputFile, String data) {
		String outputBucket   = this.mockmanifest;

		System.out.println("writer: manifest file '" + outputFile.fileName + "'");

		String fn = outputBucket + "\\" + outputFile.path + "\\" + outputFile.fileName;
		String charset = "UTF-8";
		System.out.println("writer: write to file '" + fn + "'");

		File f = new File(outputBucket + "\\" + outputFile.path);
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
	public FileSpec makeManifestFileName(FileSpec dataFile) {
		FileSpec retval = new FileSpec();
		retval.bucket = this.manifestBucket;
		retval.path = this.manifestPath;
		retval.timestamp = dataFile.timestamp;
		retval.sourceName =  dataFile.sourceName;
		retval.fullscanned = dataFile.fullscanned;
		retval.fileName = "manifest-table." + retval.sourceName + "." + retval.timestamp + ".batch." + retval.timestamp + ".fullscanned." + retval.fullscanned + ".json";
		return retval; 
	}
	
	


	
	
}
