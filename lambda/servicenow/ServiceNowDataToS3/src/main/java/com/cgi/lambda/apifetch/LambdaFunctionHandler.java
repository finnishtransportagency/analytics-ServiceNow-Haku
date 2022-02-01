package com.cgi.lambda.apifetch;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

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



public class LambdaFunctionHandler implements RequestHandler<Map<String, Object>, String>, SimpleWriter {

	// Vakiot joita ei tarvitse muuttaa asetetaan jo täällä. Loput handlerissa
	
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
	private String manifestTemplate = "{\"entries\":[],\"columns\":[\"DATA\"]}";

	private String charset = "UTF-8";

	private Context context;

	private SimpleLambdaLogger logger = null;	

	private String fullscans = "";
	
	private String runYearMonth = "";
	private boolean includeYearMonth = true;

	
	@Override
	public String handleRequest(Map<String, Object> input, Context context) {

		this.runYearMonth = DateTime.now().toString("YYYY-MM");
		String includeYM = System.getenv("include_yearmonth");
		if ("0".equals(includeYM) || "false".equalsIgnoreCase(includeYM)) {
			this.includeYearMonth = false;
		}
		
		this.context = context;
		this.logger = new SimpleLambdaLogger(this.context);

		// TUnnukset- secret
		String secretArn = System.getenv("secret_arn");
		this.region = System.getenv("region");

		// API vaatimukset
		String queryStringDefault = System.getenv("query_string_default");
		String queryStringDate = System.getenv("query_string_date");
		
		String inOutputSplitLimit = System.getenv("output_split_limit");
		String inIncrement = System.getenv("api_limit");

		// Kohde
		this.outputBucket = System.getenv("output_bucket");
		this.outputPath = System.getenv("output_path");
		this.outputFileName = System.getenv("output_filename");

		// Manifest
		this.manifestBucket = System.getenv("manifest_bucket"); 
		this.manifestPath = System.getenv("manifest_path"); 
		this.manifestArn = System.getenv("manifest_arn");

		// Koordinaattimuunnos
		String inCoordinateTransform = System.getenv("coordinate_transform");
		boolean coordinateTransform = inCoordinateTransform.trim().equalsIgnoreCase("true") ? true : false; 

		this.fullscans = System.getenv("fullscans");
		
		String username = null;
		String password = null;
		String url = null;
		
		// Tunnusten ja url haku
		try {
			SecretsManagerClient secretsClient = SecretsManagerClient.builder().region(Region.of(this.region)).build();
			GetSecretValueRequest valueRequest = GetSecretValueRequest.builder().secretId(secretArn).build();
			GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
			String secretJson = valueResponse.secretString();

			JSONObject sj = new JSONObject(secretJson);
			username = sj.getString("username");
			password = sj.getString("password");
			url = sj.getString("url");
			if (!url.endsWith("/")) {
				url += "/";
			}
			secretsClient.close();
			
		} catch (Exception e) {
			this.logger.log("Secret retrieve error: '" + e.toString() + "', '" + e.getMessage() + "'");
			return "";
		}



		String datein = "";
		String startdatein = "";
		String enddatein = "";

		if (input != null) {
			// Päivämäärä- inputit
			this.logger.log("Input: " + input);
			datein = getDate(input, "date");
			this.logger.log("input date:  '" + datein + "'\n");
			startdatein = getDate(input, "startdate");
			this.logger.log("input startdate:  '" + startdatein + "'\n");
			enddatein = getDate(input, "enddate");
			this.logger.log("input enddate:  '" + enddatein + "'\n");
		}








		DateTime startDate = null;
		DateTime endDate = null;
		
		if (!datein.isEmpty()) {
			startDate = new DateTime(datein).withTime(0, 0, 0, 0);
			endDate = startDate;
		} else {
			if (!startdatein.isEmpty()) {
				// Annettu startdate
				startDate = new DateTime(startdatein).withTime(0, 0, 0, 0);
				if (!enddatein.isEmpty()) {
					// Annettu enddate, käytetään sitä
					endDate = new DateTime(enddatein).withTime(0, 0, 0, 0);
				} else {
					// Vain startdate => ajetaan vain startdate päivälle
					endDate = startDate;
				}
			}
		}

		if (!this.outputPath.endsWith("/")) this.outputPath += "/";

		// API increment
		Integer increment = null;
		try {
			increment = Integer.parseInt(inIncrement);
		} catch (Exception e) {
			this.logger.log("Invalid increment parameter. Use default value " + this.DEFAULT_INCREMENT);
			increment = this.DEFAULT_INCREMENT;
		}
		
		// API limit
		Integer outputSplitLimit = null;
		try {
			outputSplitLimit = Integer.parseInt(inOutputSplitLimit);
		} catch (Exception e) {
			this.logger.log("Invalid output split limit parameter. Use default value 1500");
			outputSplitLimit = 1500;
		}

		ManifestCreator manifestCreator = null;

		// HUOM: template on vakio koska taulusta riippumatta vain tallennetaan json eiklä parsita täällä
		if (!this.manifestArn.isEmpty() && !this.manifestBucket.isEmpty() && !this.manifestPath.isEmpty()) {
			manifestCreator = new ManifestCreator(this.logger, this);
			manifestCreator.setTemplate(manifestTemplate);
			this.logger.log("Manifest resources defined and template found. Generate manifest(s) for data files.");
		} else {
			this.logger.log("Manifest resources are not defined. Do not generate manifest(s).");
		}

		// ServiceNow api prosessointi
		ServiceNowApiFetch api = new ServiceNowApiFetch(this.logger, this, username, password, url,
				queryStringDefault, queryStringDate, this.argOffset, this.argLimit, increment, outputSplitLimit,
				coordinateTransform, this.outputFileName);
		
		api.setManifestCreator(manifestCreator);
		
		boolean result = api.process(startDate, endDate);
		if (!result) {
			System.exit(1);
		}
		
		return "";
	}

	
	

	
	
	/**
	 * Parametrien haku nimen mukaan
	 * 
	 * @author Isto Saarinen 2021-12-16
	 * 
	 * @param input		input map
	 * @param name		haettava avain
	 * @return arvo tai ""
	 */
	protected String getDate(Map<String,Object> input, String name) {
		if (input == null) return "";
		try {
			if (input.containsKey(name)) {
				Object s = input.get(name);
				return (s != null) ? s.toString().trim() : "";
			}
			return "";
		} catch (Exception e) {
		}
		return "";
	}


	
	
	
    /**
	 * Check if file name is in fullscan list list is constructed so that __ separates files from environmentalvariable set in lambda
	 * Sets fullscanned parameter to true if current file being copied is in the list of fullscan enabled list
	 *
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
	// s3://<outputBucket>/<outputPath>/[YYYY-MM/]table.<outputFileName>.<now>.batch.<now>.fullscanned.false.json
	@Override
	public FileSpec makeDataFileName(String sourceName) {
		FileSpec retval = new FileSpec();
		retval.bucket = this.outputBucket;
		retval.path = this.outputPath;
		if (this.includeYearMonth) {
			retval.path = this.outputPath + this.runYearMonth + "/";
		}
		retval.timestamp = "" + DateTime.now().getMillis();
		retval.sourceName =  sourceName;
		retval.fullscanned = this.isFullscan(sourceName);
		retval.fileName = "table." + sourceName + "." + retval.timestamp + ".batch." + retval.timestamp + ".fullscanned." + retval.fullscanned + ".json";
		return retval; 

	}

	

	/**
	 * 
	 * Datatiedoston kirjoitus.
	 * 
	 * 
	 */
	@Override
	public boolean writeDataFile(FileSpec outputFile, String data) {
		boolean result = false;

		String path = outputFile.path;
		if (!path.endsWith("/")) {
			path += "/";
		}
		path += outputFile.fileName;
		String fullPath = outputFile.bucket + "/" + path;

		logger.log("Write data, file name = '" + fullPath + "'");

		try {
			AmazonS3 s3Client = AmazonS3Client.builder().withRegion(this.region).build();
			byte[] stringByteArray = data.getBytes(this.charset);
			InputStream byteString = new ByteArrayInputStream(stringByteArray);
			ObjectMetadata objMetadata = new ObjectMetadata();
			objMetadata.setContentType("plain/text");
			objMetadata.setContentLength(stringByteArray.length);

			s3Client.putObject(outputFile.bucket, path, byteString, objMetadata);
			result = true;

		} catch (UnsupportedEncodingException e) {
			//String errorMessage = "Error: Failure to encode file to load in: " + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName;
			String errorMessage = "Error: encode '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + fullPath + "'";
			this.logger.log(errorMessage);

			System.err.println(errorMessage);
			e.printStackTrace();
		} catch (Exception e) {
			//String errorMessage = "Error: S3 write error " + outputFile.bucket + "/" + outputFile.path + "/" + outputFile.fileName;
			String errorMessage = "Error: S3 write '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + fullPath + "'";
			this.logger.log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		}
		logger.log("Write data, file name = '" + fullPath + "' => result = " + result);
		return result;
	}



	/**
	 * 
	 * Manifest- tiedoston kirjoitus
	 * 
	 * 
	 */
	@Override
	public boolean writeManifestFile(FileSpec outputFile, String data) {
		boolean result = false;

		String path = outputFile.path;
		if (!path.endsWith("/")) {
			path += "/";
		}
		path += outputFile.fileName;
		String fullPath = outputFile.bucket + "/" + path;

		logger.log("Write manifest, file name = '" + fullPath + "'");

		try {
			AmazonS3 s3Client = AmazonS3Client.builder().withRegion(this.region).build();
			byte[] stringByteArray = data.getBytes(this.charset);
			InputStream byteString = new ByteArrayInputStream(stringByteArray);
			ObjectMetadata objMetadata = new ObjectMetadata();
			//objMetadata.setContentType("plain/text");
			objMetadata.setContentLength(stringByteArray.length);

	    	PutObjectRequest request = new PutObjectRequest(outputFile.bucket, path, byteString, objMetadata);

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
			String errorMessage = "Error: encoding '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + fullPath + "'";
			this.logger.log(errorMessage);

			System.err.println(errorMessage);
			e.printStackTrace();
		} catch (Exception e) {
			//System.err.println("Error:Fatal: could not create new manifest file with correct acl \n check permissions \n manifest filename: '" + manifestBucket + manifestKey + "'");
			String errorMessage = "Error: S3 write '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + fullPath + "'";
			this.logger.log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		}
		
		logger.log("Write manifest, file name = '" + fullPath + "' => result = " + result);
		return result;
	}

	
	
	
	
	

	/**
	 * 
	 * Manifest- tiedoston nimen muodostus
	 * 
	 */
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
