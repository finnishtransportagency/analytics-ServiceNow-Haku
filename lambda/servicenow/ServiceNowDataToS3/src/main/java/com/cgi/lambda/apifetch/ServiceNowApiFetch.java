package com.cgi.lambda.apifetch;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;



public class ServiceNowApiFetch {

	
	private SimpleLogger logger = null;
	private SimpleWriter writer = null;
	private ManifestCreator manifestCreator = null;
	
	private String username = null;
	private String password = null;
	private String url = null;
	private String queryStringDefault = null;
	private String queryStringDate = null;
	private Integer increment = Integer.valueOf(1000);
	private String argOffset = null;
	private String argLimit = null;
	private String charset = "UTF-8";
	private String alertString = "## VAYLA AWS-HALYTYS: SERVICENOW:";
	private Integer outputSplitLimit = Integer.valueOf(1500);
	private boolean coordinateTransform = false;
	private String sourceName = null;
	

	
	
	public ServiceNowApiFetch(SimpleLogger logger, SimpleWriter writer, String username, String password, String url, String queryStringDefault, String queryStringDate, String argOffset, String argLimit, Integer increment, Integer outputSplitLimit, boolean coordinateTransform, String sourceName) {
		this.logger = logger;
		this.writer = writer;
		this.queryStringDefault = queryStringDefault;
		this.queryStringDate = queryStringDate;
		this.argOffset = argOffset;
		this.argLimit = argLimit;
		if (increment != null) this.increment = increment;
		if (this.increment < 0) this.increment = Integer.valueOf(1);
		this.username = username;
		this.password = password;
		this.url = url;
		if (outputSplitLimit != null) this.outputSplitLimit = outputSplitLimit;
		if (this.outputSplitLimit < 0) this.outputSplitLimit = Integer.valueOf(1500);
		this.coordinateTransform = coordinateTransform;
		this.sourceName = sourceName;
	}


	public void setManifestCreator(ManifestCreator manifestCreator) {
		this.manifestCreator = manifestCreator;
	}
	
	
	/**
	 * Apuluokka datan palautusta varten
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 */
	private class DataHolder {
		public String data;			// JSON data
		public int recordCount;		// JSON record count
		public boolean status;		// Result status
	}	
	



	/**
	 * 
	 * Paikallinen logger, vain null- tarkastus
	 * 
	 * @param s
	 */
	private void log(String s) {
		if (this.logger != null) {
			this.logger.log(s);
		}
	}
	
	
	
	
	/**
	 * 
	 * Hae data ja tee mahdollinen koordinaattimuunnos
	 * 
	 * @param startDateStr
	 * @param endDateStr
	 * @return
	 */	
	public boolean process(DateTime startDate, DateTime endDate) {

		String data = null;
		
		try {
			// Eri kutsu jos haetaan vakio eilinen tai päivämäärällä (tai välillä) 
			if ((startDate != null) && (endDate != null)) {
				this.logger.log("Fetch data created between '" + startDate.toString("yyyy-MM-dd") + "' - '" + endDate.toString("yyyy-MM-dd") + "'");
				data = this.fetchData(startDate, endDate);
			} else {
				this.logger.log("Fetch data created or updated yesterday");
				data = this.fetchData();
			}
				
		} catch (Exception e) {
			// muutettu IOException => Exception
			this.logger.log(this.alertString + " Fatal error: Failed to download data: '" + e.toString() + "', '" + e.getMessage() + "'");
			e.printStackTrace();
			// Hakuvirhe, poistutaan
			return false;
		}

		if (data == null) return false;

		// Save data into S3

		if (!data.isEmpty()) {

			if (coordinateTransform) {
				EnrichServiceNowDataWithCoordinates enrichmentCenter = null;
				
				try {
					// 3067 =ETRS89 / ETRS-TM35FIN, converts to 4326=wgs84
					enrichmentCenter = new EnrichServiceNowDataWithCoordinates(data, "EPSG:3067", this.outputSplitLimit);
					enrichmentCenter.enrichData();
				} catch (Exception e) {
					this.logger.log("Error: Coordinate transform failed: '" + e.toString() + "', '" + e.getMessage() + "'");
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					e.printStackTrace(pw);
					String sStackTrace = sw.toString(); // stack trace as a string
					this.logger.log("Error: Coordinate transform failed: " + sStackTrace);
					// Koordinaattimuunnosvirhe, poistutaan
					return false;
				}

				// adds wgs84 coordinates
				if ((enrichmentCenter == null) || enrichmentCenter.enrichedList.isEmpty()) {
					this.logger.log("Error: Empty dataset after coordinate transform");
					// Koordinaattimuunnosvirhe, poistutaan
					return false;
				/*
				} else if (enrichmentCenter.enrichedList.size() == 1) {
					this.logger.log("Write transformed output start");
					FileSpec outputFile = writer.makeDataFileName(this.sourceName);
					if(writer.writeDataFile(outputFile, dataAndCoordinates)) {
						if (this.manifestCreator != null) {
							this.manifestCreator.createManifest(outputFile);
						}
					}
					this.logger.log("Write transformed output end");
				*/
				} else { // loop through result array
					this.logger.log("Write transformed output start");
					int size = enrichmentCenter.enrichedList.size();
					for (int i = 0; i < size; i++) {
						FileSpec outputFile = writer.makeDataFileName(this.sourceName);
						if (writer.writeDataFile(outputFile, enrichmentCenter.enrichedList.get(i).toString())) {
							if (this.manifestCreator != null) {
								boolean result = this.manifestCreator.createManifest(outputFile);
								if (!result) return false;
							}
						}
					}
					this.logger.log("Write transformed output end. Processed parts = " + size);
				}

			} else {
				this.logger.log("Write output start");
				FileSpec outputFile = writer.makeDataFileName(this.sourceName);
				if (writer.writeDataFile(outputFile, data)) {
					if (this.manifestCreator != null) {
						boolean result = this.manifestCreator.createManifest(outputFile);
						if (!result) return false;
					}
				}
				this.logger.log("Write output end");
			}
			
		}
		return true;
	}





	/**
	 * Datan haku päivämäärävälille (tai yhdelle päivälle jos annettu sama päivä)
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 * @param startdate		Alkupäivä ('yyyy-MM-dd')
	 * @param enddate		Loppupäivä ('yyyy-MM-dd')
	 * @return JSON data
	 */
	public String fetchData(DateTime startDate, DateTime endDate) {
		if ((startDate == null) || (endDate == null)) return "";
		try {
			DateTime processDate = startDate;
			StringBuffer sb = new StringBuffer();
			int counter = 1;
			// Loop: jokaiselle päivälle
			while( processDate.getMillis() <= endDate.getMillis() ) {
				this.log("Fetch data for date '" + processDate.toString("yyyy-MM-dd") + "'");
				if (counter > 1) sb.append(",");
				sb.append(this.fetchData(processDate));

				processDate = processDate.plusDays(1);
				counter++;
			}
			// Lisätään listan ympärille result objekti
			return "{\"result\":[" + sb.toString() + "]}";
		} catch (Exception e) {
			this.log("Fetch data for date range '" + (startDate != null ? startDate.toString("yyyy-MM-dd") : "null") + "' -> '" + (endDate != null ? endDate.toString("yyyy-MM-dd") : "null") + " failed: '" + e.toString() + "', '" + e.getMessage() + "'");
		}
		// Some error: return empty data
		return null;
	}
	
	
	
	
	/**
	 * Datan vakiohaku eilisen tapahtumille
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 * @return JSON data
	 */
	public String fetchData() {
		return"{\"result\":[" + this.fetchData(null) + "]}"; 
	}
	
	
	
	
	/**
	 * Datan haku yhdelle päivälle (tai vakio eiliselle jos annettu pvm == null)
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 * @param processDate   Käsiteltävä päivä
	 * @return JSON data
	 */
	public String fetchData(DateTime processDate) {
		long startts = System.currentTimeMillis();
		List<Long> apicalls = new ArrayList<Long>();

		String data = "";
		int size = 0;
		String dateFilter = "";

		String query = this.queryStringDefault;
		
		if (processDate != null) {
			// Päivä annettu, kasataan hakuehto
			dateFilter = processDate.toString("yyyy-MM-dd");
			query = this.queryStringDate.replace("{DATEFILTER}", dateFilter);
		}
		
		try {
			StringBuffer sb = new StringBuffer();

			// Yritetään aina ensin ilman rajoituksia
			Integer offset = null;
			Integer limit = null;

			//String urlQuery = this.url + query;
			
			while(true) {
				// Kutsu offset+limit (tai ilman 1. kerralla)
				long startf = System.currentTimeMillis();
				DataHolder piece = this.serviceNowApiCall(offset, limit, query);
				long endf = System.currentTimeMillis();
				if (piece.recordCount < 1) {
					// Ei tuloksia, lopetetaan
					this.log("## no more records. Stop fetch");
					break;
				}
				apicalls.add(Long.valueOf(endf - startf));

				if (!piece.status) {
					// Tämä on aina ensimmäinen kutsu jos tänne päädytään, status saadaan vain jos offset+limit ei annettu. Jatketaan offset+limit niin saadaan kaikki
					limit = piece.recordCount;
					if (offset == null) offset = 0;
					offset += piece.recordCount;
					this.log("## fetch error. continue with offset = " + offset + ", limit = " + limit);
					sb.append(piece.data);
				} else {
					// Palautus ok, tuloksia > 0
					if ((offset != null) && (limit != null)) {
						// Jos offset + limit käytössä
						
						// Ensimmäisen palautuksen jälkeen jatketaan pilkulla
						if (offset.intValue() > 0) {
							sb.append(",");
						}
						// Lisätään aina data
						sb.append(piece.data);

						if (piece.recordCount < limit.intValue()) {
							// Saatiin vähemmän kuin pyydettiin => pienennetään limittiä
							offset += piece.recordCount;
							limit = piece.recordCount;
							this.log("Adjust offset = " + offset + ", limit = " + limit);
						} else {
							// Saatiin se mitä pyydettiin, lisätään vakio limit 
							offset += increment;
							this.log("Adjust offset = " + offset + ", limit = " + limit);
						}
					} else {
						// Offset + limit ei käytössä, yhden datasetin haku, varmistetaan ettei ole seuraavaa (aseta offset+limit & uusi haku)
						sb.append(piece.data);
						offset = piece.recordCount;
						limit = piece.recordCount;
					}
				}

			}
			
			data = sb.toString();
			
			this.log("## raw data size: " + data.length());
			
			JSONObject records = new JSONObject("{\"result\":[" + data + "]}");
			JSONArray jsonArray = records.getJSONArray("result");
			size = jsonArray.length();
			
		} catch (Exception e) {
			System.err.println("Fatal error: Failed to download data");
			e.printStackTrace();
			this.log("## fetch error: '" + e.toString() + "', '" + e.getMessage() + "'");
			data = null;
		}
		
		long endts = System.currentTimeMillis();
		
		long callstotal = 0;
		long callsavg = 0;
		if (apicalls.size() > 0) {
			for (Long ts : apicalls) {
				callstotal += ts.longValue();
			}
			callsavg = callstotal / apicalls.size();
		}
		
		this.log("## total records fetched for day '" + (dateFilter != null ? dateFilter : "yesterday") + "' = " + size + ", total time = " + (endts - startts) + " ms, total api fetch time = " + callstotal + " ms, average " + callsavg + " ms/fetch");
		
		return data;
	}
	
	
	

	/**
	 * ServiceNow API kutsu.
	 * 
	 * Offset ja limit lisätään path perään jos molemmat != null
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 * @param offset	API offset parametri
	 * @param limit		API limit parametri
	 * @param username	API käyttäjä
	 * @param password	API salasana
	 * @param url		API url
	 * @param format	API polku & vakioparametrit
	 * @return JSON listan sisältö tapahtumista. Palautus ilman ympäröiviä []
	 */
	private DataHolder serviceNowApiCall(Integer offset, Integer limit, String query) throws Exception {

		long startts = System.currentTimeMillis();
	
		// Prepare login credentials and URL
		String login = this.username + ":" + this.password;
		//String requestUrl = url + "?" + path;
		String url = this.url + query;
		if ((offset != null) && (limit != null)) {
			url += this.argOffset + offset + this.argLimit + limit;
		}

		this.log("## make api call with url: " + url);
		String base64Login = new String(Base64.getEncoder().encode(login.getBytes()));

		// Open connection and read CSV
		URL uurl = new URL(url);
		URLConnection uc = uurl.openConnection();
		uc.setRequestProperty("Authorization", "Basic " + base64Login);
		uc.setRequestProperty("Accept", "application/json");
		
//		InputStream inputstream = uc.getInputStream();
//		BufferedInputStream bufferedstream = new BufferedInputStream(inputstream);
//		BufferedReader in = new BufferedReader(new InputStreamReader(bufferedstream, this.charset));

		// Start reading result
		BufferedReader in = new BufferedReader(new InputStreamReader(new BufferedInputStream(uc.getInputStream()), this.charset));

		StringBuffer sb = new StringBuffer();
		String line;
		while ((line = in.readLine()) != null) {
			sb.append(line + "\n");
		}
		in.close();
		
		String data = sb.toString();
		sb = null;
		
		// Varmistus, loppuun tulee joskus ylimääräinen ',""'
		if (data.endsWith("}],\"\"}")) {
			data = data.replace("}],\"\"}", "}]}");
		}
		
		// Palautettujen tulosten "rivimäärä"
		JSONObject records = new JSONObject(data);
		JSONArray results = records.getJSONArray("result");
		int size = results.length();
		this.log("## api call return records = " + size);

		DataHolder dh = new DataHolder();
		dh.recordCount = results.length();
		dh.status = true;
		try {
			String status = records.getString("status");
			if ((status != null) && (status.equals("failure"))) {
				dh.status = false;
			}
		} catch (Exception e) {
			// "status" ei löydy => kaikki ok ja jatketaan
		}
		
		// Tallennetaan palautettava JSON array- objektista, siivoaa ylimääräiset suoraan pois mutta jättää [] poistettavaksi ympäriltä
		String t = results.toString(); // HUOM: tämä palauttaa [{..},{..},...] => pitää poistaa [] ympäriltä
		if (t.length() > 0) {
			// Varmistus, poistetaan vain jos [] ympärillä
			if (t.startsWith("[")) {
				t = t.substring(1);
			}
			if (t.endsWith("]")) {
				t = t.substring(0, t.length()-1);
			}
		}
		dh.data = t;
		long endts = System.currentTimeMillis();
		this.log("## api call '" + url + "' return status = " + dh.status + ", records = " + dh.recordCount + ", time = " + (endts - startts) + " ms");
		return dh;
	}
	
	
	
	
}
