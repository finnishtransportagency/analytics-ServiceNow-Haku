package com.cgi.lambda.apifetch;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import org.joda.time.DateTime;



public class LambdaFunctionHandler implements RequestHandler<Object, String>, SimpleWriter {

	// Use environmental variables in AWS Lambda to set values of these

	// Haetaan suoraan secrets- managerista user:pass
	// System.getenv("secrets") 
	
	//private String username = null;
	//private String password = null;
	
	//queryStringDefault = "sysparm_query=sys_class_name%3Dsn_customerservice_casesys_updated_onONYesterday%40javascript%3Ags.beginningOfYesterday()%40javascript%3Ags.endOfYesterday()%5EORsys_created_onONYesterday%40javascript%3Ags.beginningOfYesterday()%40javascript%3Ags.endOfYesterday()&sysparm_display_value=true";
	//queryStringDate    = "sysparm_query=sys_class_name%3Dsn_customerservice_case%5Esys_created_onON{DATEFILTER}@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27start%27)@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27end%27)&sysparm_display_value=true";

	private String argOffset = "&sysparm_offset=";
	private String argLimit = "&sysparm_limit=";
	private Integer DEFAULT_INCREMENT = Integer.valueOf(1000);

	private String region = null;
	private String outputBucket = null;
	private String outputPath = null;
	private String outputFileName = null;

	private String manifestBucket = null;
	private String manifestPath = null;
	private String manifestArn = null;
	
	
	
	private String charset = "UTF-8";

	private Context context;

	private SimpleLambdaLogger logger = null;	

	private String fullscans = "";
	
	

	
	@Override
	public String handleRequest(Object input, Context context) {

		this.context = context;
		this.logger = new SimpleLambdaLogger(this.context);

		
		String secretArn = System.getenv("secret_arn");
		this.region = System.getenv("region");

		String queryStringDefault = System.getenv("query_string_default");
		String queryStringDate = System.getenv("query_string_date");
		
		String inOutputSplitLimit = System.getenv("output_split_limit");
		String inIncrement = System.getenv("api_limit");

		this.outputBucket = System.getenv("output_bucket");
		this.outputPath = System.getenv("output_path");
		this.outputFileName = System.getenv("output_filename");
		
		this.manifestBucket = System.getenv("manifest_bucket"); 
		this.manifestPath = System.getenv("manifest_path"); 
		this.manifestArn = System.getenv("manifest_arn");
		
		String inCoordinateTransform = System.getenv("coordinate_transform");
		boolean coordinateTransform = inCoordinateTransform.trim().equalsIgnoreCase("true") ? true : false; 

		this.fullscans = System.getenv("fullscans");
		
		String username = null;
		String password = null;
		String url = null;
		

		try {
			SecretsManagerClient secretsClient = SecretsManagerClient.builder().region(Region.of(this.region)).build();
			GetSecretValueRequest valueRequest = GetSecretValueRequest.builder().secretId(secretArn).build();
			GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
			String secretJson = valueResponse.secretString();

			JSONObject sj = new JSONObject(secretJson);
			username = sj.getString("username");
			password = sj.getString("password");
			url = sj.getString("url");
			
			secretsClient.close();
			
		} catch (Exception e) {
			this.logger.log("Secret retrieve error: '" + e.toString() + "', '" + e.getMessage() + "'");
			return "";
		}
		
		
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
		if (!this.outputPath.endsWith("/")) this.outputPath += "/";

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
		

		String template = this.readManifestTemplate();
		
		ManifestCreator manifestCreator = new ManifestCreator(this.logger, this);
		manifestCreator.setTemplate(template);
		
		
		ServiceNowApiFetch api = new ServiceNowApiFetch(this.logger, this, username, password, url,
				queryStringDefault, queryStringDate, this.argOffset, this.argLimit, increment, outputSplitLimit,
				coordinateTransform, this.outputFileName);
		
		api.setManifestCreator(manifestCreator);
		
		api.process(startDateStr, endDateStr);
		
		return "";
	}

	
	
	
	public String readManifestTemplate() {
		String content = "";
		try {
			AmazonS3 s3Client = AmazonS3Client.builder().withRegion(this.region).build();
			content = s3Client.getObjectAsString(this.manifestBucket, this.manifestPath + "/" + this.outputFileName + ".json");
		} catch (Exception e) {
			System.err.println("Error:Fatal: could not read manifest template");
			System.out.println("trying to read manifest template from 's3://" + this.manifestBucket + "/" + this.manifestPath + "/" + this.outputFileName + "'");
			content = "";
		}

		return content;
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
	 * Checks if file name is in fullscan list list is constructed so that __ separates files from environmentalvariable set in lambda
	 * Sets fullscanned parameter to true if current file being copied is in the list of fullscan enabled list
	 * @param sourceName with out .csv that of the file that is being manifested and copied for usage of ADE
	 * @return
	 */
	protected boolean isFullscan(String sourceName) {
		String[] fullscanNames = this.fullscans.split("__");
		for  (String listFile : fullscanNames) {
			if (listFile.toLowerCase().equals(sourceName.toLowerCase())) {
				return true;
			}			
		}
		return false;
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
	// s3://<outputBucket>/<outputPath>/table.<outputFileName>.<now>.batch.<now>.fullscanned.false.json
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

	

	@Override
	public boolean writeDataFile(FileSpec outputFile, String data) {
		boolean result = false;

		logger.log("Write data, file name = '" + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName + "'");

		try {
			AmazonS3 s3Client = AmazonS3Client.builder().withRegion(this.region).build();
			byte[] stringByteArray = data.getBytes(this.charset);
			InputStream byteString = new ByteArrayInputStream(stringByteArray);
			ObjectMetadata objMetadata = new ObjectMetadata();
			objMetadata.setContentType("plain/text");
			objMetadata.setContentLength(stringByteArray.length);

			s3Client.putObject(outputFile.bucket, outputFile.path + "/" + outputFile.fileName, byteString, objMetadata);
			result = true;

		} catch (UnsupportedEncodingException e) {
			//String errorMessage = "Error: Failure to encode file to load in: " + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName;
			String errorMessage = "Error: encode '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName + "'";			this.logger.log(errorMessage);

			System.err.println(errorMessage);
			e.printStackTrace();
		} catch (Exception e) {
			//String errorMessage = "Error: S3 write error " + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName;
			String errorMessage = "Error: S3 write '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName + "'";
			this.logger.log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		}
		logger.log("Write data, file name = '" + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName + "' => result = " + result);
		return result;
	}




	@Override
	public boolean writeManifestFile(FileSpec outputFile, String data) {
		boolean result = false;

		String manifestKey = outputFile.path + "/" + outputFile.fileName;

		logger.log("Write manifest, file name = '" + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName + "'");

		try {
			AmazonS3 s3Client = AmazonS3Client.builder().withRegion(this.region).build();
			byte[] stringByteArray = data.getBytes(this.charset);
			InputStream byteString = new ByteArrayInputStream(stringByteArray);
			ObjectMetadata objMetadata = new ObjectMetadata();
			//objMetadata.setContentType("plain/text");
			objMetadata.setContentLength(stringByteArray.length);

	    	PutObjectRequest request = new PutObjectRequest(outputFile.bucket, manifestKey, byteString, objMetadata);

	    	Collection<Grant> grantCollection = new ArrayList<Grant>();
			grantCollection.add( new Grant(new CanonicalGrantee(s3Client.getS3AccountOwner().getId()), Permission.FullControl));
	        grantCollection.add( new Grant(new CanonicalGrantee(this.manifestArn), Permission.FullControl));
	        
			AccessControlList objectAcl = new AccessControlList();
            objectAcl.getGrantsAsList().clear();
            objectAcl.getGrantsAsList().addAll(grantCollection);
            request.setAccessControlList(objectAcl);
            s3Client.putObject(request);

			result = true;

		} catch (UnsupportedEncodingException e) {
			//String errorMessage = "Error: Failure to encode file to load in: " + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName;
			String errorMessage = "Error: encoding '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName + "'";
			this.logger.log(errorMessage);

			System.err.println(errorMessage);
			e.printStackTrace();
		} catch (Exception e) {
			//System.err.println("Error:Fatal: could not create new manifest file with correct acl \n check permissions \n manifest filename: '" + manifestBucket + manifestKey + "'");
			String errorMessage = "Error: S3 write '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName + "'";
			this.logger.log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		}
		
		logger.log("Write manifest, file name = '" + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName + "' => result = " + result);
		return result;
	}

	
	
	
	
	


	// s3://<manifestBucket>/<manifestPath>/manifest-table.<outputFileName>.<now>.batch.<now>.fullscanned.false.json.json
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
