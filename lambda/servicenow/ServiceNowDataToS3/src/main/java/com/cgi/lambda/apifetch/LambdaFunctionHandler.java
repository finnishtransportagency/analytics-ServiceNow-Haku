package com.cgi.lambda.apifetch;

//import java.io.BufferedInputStream;
//import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.PrintWriter;
//import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
//import java.net.URL;
//import java.net.URLConnection;
import java.time.Instant;
//import java.util.ArrayList;
//import java.util.Base64;
//import java.util.List;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

//import org.json.JSONArray;
//import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

import org.joda.time.DateTime;



public class LambdaFunctionHandler implements RequestHandler<Object, String>, SimpleWriter {

	// Use environmental variables in AWS Lambda to set values of these

	// Haetaan suoraan secrets- managerista user:pass
	// System.getenv("secrets") 
	
	//private String username = null;
	//private String password = null;
	//private String url = null;
	//private String slimit = null;
	
	private String outputPrefix = null;
	private String outputFileName = null;
	//private String queryStringDefault = null;
	//private String queryStringDate = null;
	
	//queryStringDefault = "sysparm_query=sys_class_name%3Dsn_customerservice_casesys_updated_onONYesterday%40javascript%3Ags.beginningOfYesterday()%40javascript%3Ags.endOfYesterday()%5EORsys_created_onONYesterday%40javascript%3Ags.beginningOfYesterday()%40javascript%3Ags.endOfYesterday()&sysparm_display_value=true";
	//queryStringDate    = "sysparm_query=sys_class_name%3Dsn_customerservice_case%5Esys_created_onON{DATEFILTER}@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27start%27)@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27end%27)&sysparm_display_value=true";

	private String argOffset = "&sysparm_offset=";
	private String argLimit = "&sysparm_limit=";
	//private String apiIncrement = null;
	private Integer DEFAULT_INCREMENT = Integer.valueOf(1000);

	private String region = null;
	private String s3Bucket = null;
	
	private String charset = "UTF-8";

	private Context context;

	private SimpleLambdaLogger logger = null;	


	
	@Override
	public String handleRequest(Object input, Context context) {

		this.context = context;
		this.logger = new SimpleLambdaLogger(this.context);
		
		String username = System.getenv("username");
		String password = System.getenv("password");
		String url = System.getenv("service_url");
		String inIncrement = System.getenv("api_limit");
		String inOutputSplitLimit = System.getenv("output_split_limit");
		this.outputPrefix = System.getenv("output_prefix");
		this.outputFileName = System.getenv("output_filename");
		String queryStringDefault = System.getenv("query_string_default");
		String queryStringDate = System.getenv("query_string_date");
		this.region = System.getenv("region");
		this.s3Bucket = System.getenv("s3_bucket_name");

		String inCoordinateTransform = System.getenv("coordinate_transform");
		boolean coordinateTransform = inCoordinateTransform.trim().equalsIgnoreCase("true") ? true : false; 
		
		String sourceName = "u_case";
		
		
		this.logger.log("Input: " + input);
		// Isto Saarinen 2021-12-01: lisätty valinnaiset päivämäärä- inputit
		String datein = getDate(input, "date");
		this.logger.log("input date:  '" + datein + "'\n");
		String startdatein = getDate(input, "startdate");
		this.logger.log("input startdate:  '" + startdatein + "'\n");
		String enddatein = getDate(input, "enddate");
		this.logger.log("input enddate:  '" + enddatein + "'\n");

		String startDateStr = null;
		String endDateStr = null;
		
		// Isto Saarinen 2021-12-01: päivämäärä- inputit
		if (!datein.isEmpty()) {
			 // Annettu date: se yliajaa startdate/enddate
			startDateStr = datein;
			endDateStr = datein;
		} else {
			if (!startdatein.isEmpty()) {
				// Annettu startdate
				startDateStr = startdatein;
				if (!enddatein.isEmpty()) {
					// Annettu enddate
					endDateStr = enddatein;
				} else {
					// Vain startdate => ajetaan kuluvaan päivään asti
					endDateStr = new DateTime().toString("yyyy-MM-dd");
				}
			}
		}

		// Isto Saarinen 2021-12-01: output prefix varmistus
		if (!this.outputPrefix.endsWith("/")) this.outputPrefix += "/";

		Integer increment = null;
		try {
			increment = Integer.parseInt(inIncrement);
		} catch (Exception e) {
			this.logger.log("Invalid increment parameter. Use default value " + this.DEFAULT_INCREMENT);
			increment = this.DEFAULT_INCREMENT;
		}
		
		Integer outputSplitLimit = null;
		try {
			outputSplitLimit = Integer.parseInt(inOutputSplitLimit);
		} catch (Exception e) {
			this.logger.log("Invalid output split limit parameter. Use default value 1500");
			outputSplitLimit = 1500;
		}
		
		ServiceNowApiFetch api = new ServiceNowApiFetch(this.logger, this, username, password, url,
				queryStringDefault, queryStringDate, this.argOffset, this.argLimit, increment, outputSplitLimit, coordinateTransform, sourceName);
		
		api.process(startDateStr, endDateStr);
		
		return "";
	}

	
	
	
	
	
	
	
	
	/**
	 * Uusi parametrien haku nimen mukaan
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 * @param input		input JSON
	 * @param name		haettava avain
	 * @return arvo tai ""
	 */
	protected String getDate(Object input, String name) {
		if (input == null) return "";
		JSONObject o = new JSONObject(input.toString().trim());
		try {
			String s = o.getString(name);
			return s.trim();
		} catch (Exception e) {
		}
		return "";
	}


	

	
	
	
	/**
	 * Kirjoitettavan tiedoston nimen muodostus
	 * 
	 * <output prefix> / <today: dd.MM.yyyy> / <aikaleima: unix timestamp> / <tiedoston nimi>
	 * 
	 * @author Isto Saarinen 2021-12-02
	 * 
	 * @return tiedosto ja polku
	 */
	
	
	// String destinationfilename = "table." + sourceFilename + "." + c.getTime().getTime() + ".batch." + c.getTime().getTime() + ".fullscanned." + this.fullscanned + ".json";
	@Override
	public String createOutputFileName(String sourceFileName) {
		long now = Instant.now().toEpochMilli();
		String today = new DateTime().toString("dd.MM.yyyy");
		return this.outputPrefix + today + "/" + now + "/" + this.outputFileName;
	}

	
	

	@Override
	public boolean writeData(String fileName, String data) {
		boolean result = false;
		try {
			AmazonS3 s3Client = AmazonS3Client.builder().withRegion(region).build();
			byte[] stringByteArray = data.getBytes(this.charset);
			InputStream byteString = new ByteArrayInputStream(stringByteArray);
			ObjectMetadata objMetadata = new ObjectMetadata();
			objMetadata.setContentType("plain/text");
			objMetadata.setContentLength(stringByteArray.length);

			s3Client.putObject(s3Bucket, fileName, byteString, objMetadata);
			this.logger.log("Data loaded to S3 succesfully.");
			result = true;

		} catch (UnsupportedEncodingException e) {
			String errorMessage = "Error: Failure to encode file to load in: " + fileName;
			this.logger.log(errorMessage);

			System.err.println(errorMessage);
			e.printStackTrace();
		} catch (Exception e) {
			String errorMessage = "Error: S3 write error " + fileName;
			this.logger.log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		}
		return result;
	}



}
