package com.cgi.lambda.apifetch;



import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;






public class ManifestCreator {
	
	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	private boolean fullscanned = false;
	private String fullscans = "";
	private String destinationBucket = "";
	private String prefix = "";
	
	
	
	public ManifestCreator() {
		
	}
	
	
	public boolean createManifest(SimpleLogger logger, SimpleWriter writer, String template, String dataFilePath, String dataFileName) {
		
		String manifestFileName = "manifest." + dataFileName + ".json";
		String manifestData = this.createManifestContent(template, dataFilePath, dataFileName);
		logger.log("manifest data = '" + manifestData + "'");

		
		logger.log("write manifest data to '" + manifestFileName + "'");
		
		
		return true;
	}

	
	public String createManifestContent(String manifestTemplate, String dataFilePath, String dataFileName) {
		
		if (!dataFilePath.endsWith("/")) {
			dataFilePath += "/";
		}
		
		JSONObject mainObject = new JSONObject(manifestTemplate);
		JSONArray jArray = mainObject.getJSONArray("entries");
		JSONObject entry = new JSONObject();
		entry.put("mandatory", "true");
		entry.put("url", dataFilePath + dataFileName);
		jArray.put(entry);		
		return mainObject.toString().replaceAll("(\n)", "\r\n");
	}
	
	
	
	
	
	
	private boolean createManifest(Context context, String sourceBucket, String sourceKey, String manifestBucket, String arn) {
    	
		// Puuttuva input == ok, ei tarvitse tehdä mitään
    	if ((manifestBucket == null) || (arn == null)) return true;
    	manifestBucket = manifestBucket.trim();
    	arn = arn.trim();
    	if (manifestBucket.isEmpty() || arn.isEmpty()) return true;
    	
	    // Get manifest template from AWS Bucket
		String[] splittedSourceFilename = sourceKey.replaceAll(".json", "").split("/");
		String sourceFilename = splittedSourceFilename[splittedSourceFilename.length-1];//filenamewithout .csv extension
		context.getLogger().log("Started copying file:" + sourceFilename + "\n manifestbucket: " + manifestBucket);
		String manifestTemplate = "";
		try {
			manifestTemplate = this.s3.getObjectAsString(sourceBucket, "manifest/" + sourceFilename + ".json");    	  
		} catch (Exception e){
			System.err.println("Error:Fatal: could not read manifest template");
			System.out.println("trying to read manifesttemplate from bucket:" + sourceBucket + "\n with key: " + sourceFilename);
			//System.exit(-1);
			return false;
		}
		
		Calendar c= Calendar.getInstance();
		

		checkIfFileiSFullscan(sourceFilename);
		String destinationfilename = "table." + sourceFilename + "." + c.getTime().getTime() + ".batch." + c.getTime().getTime() + ".fullscanned." + this.fullscanned + ".json";
		String destinationKey = sourceFilename + "/" + destinationfilename;
		try {	
			context.getLogger().log("Copying file from " + sourceBucket + sourceKey + " to : " + this.destinationBucket + destinationKey);
			this.s3.copyObject(sourceBucket, sourceKey, destinationBucket, destinationKey);
		} catch (Exception e){
			System.err.println("Error:Fatal: could not copy file to ade bucket");

			//System.exit(-1);
			return false;
		}

		
		String manifestKey = "manifest/" + this.prefix + sourceFilename + "/manifest-table." + this.prefix + sourceFilename + "." + c.getTime().getTime() + ".batch." + c.getTime().getTime() + ".fullscanned." + this.fullscanned + ".json.json";
		String manifest = createManifestContent(manifestTemplate, this.destinationBucket, destinationKey);
		ObjectMetadata metadata = new ObjectMetadata();
		Long contentLength = Long.valueOf(manifest.getBytes().length);
		metadata.setContentLength(contentLength);
		
		InputStream stream = new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8));
    	PutObjectRequest request = new PutObjectRequest(manifestBucket, manifestKey, stream, metadata);

    	Collection<Grant> grantCollection = new ArrayList<Grant>();
		grantCollection.add( new Grant(new CanonicalGrantee(this.s3.getS3AccountOwner().getId()), Permission.FullControl));
        grantCollection.add( new Grant(new CanonicalGrantee(arn), Permission.FullControl));

        try {
			context.getLogger().log("manifest file to " + manifestBucket + manifestKey);
			AccessControlList objectAcl = new AccessControlList();
            objectAcl.getGrantsAsList().clear();
            objectAcl.getGrantsAsList().addAll(grantCollection);
            request.setAccessControlList(objectAcl);
            this.s3.putObject(request);
        } catch (Exception e){
			System.err.println("Error:Fatal: could not create new manifest file with correct acl \n check permissions \n manifest filename: " + manifestBucket + manifestKey );
			return false;
		}
        return true;
    }
    
    
    
    
    /**
	 * Checks if file name is in fullscan list list is constructed so that __ separates files from environmentalvariable set in lambda
	 * Sets fullscanned parameter to true if current file being copied is in the list of fullscan enabled list
	 * @param fileName with out .csv that of the file that is being manifested and copied for usage of ADE
	 * @return
	 */
	protected void checkIfFileiSFullscan(String fileName) {
		String[] fullscanNames = fullscans.split("__");
		for  (String listFile : fullscanNames) {
			if (listFile.toLowerCase().equals(fileName.toLowerCase())) {
				fullscanned = true;
				break;
			}			
		}
	}

	public String createManifestContentOrig(String manifestTemplate,String destinatonBucket,String destinationKey) {
		JSONObject mainObject = new JSONObject(manifestTemplate);
		JSONArray jArray = mainObject.getJSONArray("entries");
		JSONObject entry = new JSONObject();
		entry.put("mandatory", "true");
		entry.put("url", "s3://" + destinatonBucket + "/" + destinationKey);
		jArray.put(entry);		
		return mainObject.toString().replaceAll("(\n)", "\r\n");
	}
	
	
	
}
