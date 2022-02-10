package com.cgi.lambda.apifetch;




import org.json.JSONArray;
import org.json.JSONObject;




public class ManifestCreator {
	
	private String template = "";
	
	private SimpleLogger logger = null;
	private SimpleWriter writer = null;
	
	
	public ManifestCreator(SimpleLogger logger, SimpleWriter writer) {
		this.logger = logger;
		this.writer = writer;
	}
	
	
	private void log(String s) {
		if (this.logger != null) this.logger.log(s);
	}
	
	
	public void setTemplate(String template) {
		this.template = template;
	}
	
	
	public boolean createManifest(FileSpec dataFile) {
		String manifestData = this.createManifestContent(this.template, dataFile);
		FileSpec manifestFile = this.writer.makeManifestFileName(dataFile);
		this.log("create manifest = '" + manifestData + "' => '" + manifestFile + "'");
		return this.writer.writeManifestFile(manifestFile, manifestData);
		//return true;
	}


	
	public String createManifestContent(String manifestTemplate, FileSpec dataFile) {
		String dataFileName = "s3://";
		dataFileName += dataFile.bucket;
		if (!dataFileName.endsWith("/")) dataFileName += "/";
		dataFileName += dataFile.path;
		if (!dataFileName.endsWith("/")) dataFileName += "/";
		dataFileName += dataFile.fileName;
		JSONObject mainObject = new JSONObject(manifestTemplate);
		JSONArray jArray = mainObject.getJSONArray("entries");
		JSONObject entry = new JSONObject();
		entry.put("mandatory", "true");
		entry.put("url", dataFileName);
		jArray.put(entry);		
		return mainObject.toString().replaceAll("(\n)", "\r\n");
	}
	
	

	
}
