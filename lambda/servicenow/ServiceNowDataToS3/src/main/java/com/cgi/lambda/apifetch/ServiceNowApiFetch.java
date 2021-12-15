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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
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
	
	private EnrichServiceNowDataWithCoordinates enrichmentCenter = null;
	// regex for accepted or expected characters in Service Now json. most notably alphanumeric chars incl
	// scandic letters, and chars used in json notation
	//private static final String validchars = "[^a-öA-Ö0-9\\ \\t\\.\\,\\\"\\:\\;\\=\\?\\&\\!\\@\\*\\<\\>\\+\\%\\(\\)\\[\\]\\{\\}\\-\\_\\/\\\\\\']";

	
	
	public ServiceNowApiFetch(SimpleLogger logger, SimpleWriter writer, String username, String password, String url, String queryStringDefault, String queryStringDate, String argOffset, String argLimit, Integer increment, Integer outputSplitLimit, boolean coordinateTransform, String sourceName) {
		//this.context = context;
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
	
	
	
	private void log(String s) {
		if (this.logger != null) {
			this.logger.log(s);
		}
	}
	
	
	
	
	
	public boolean process(String startDateStr, String endDateStr) {

		String data = null;
		String newJsonString = null;
		// Get data to be saved to S3
		try {
			// Isto Saarinen 2021-12-01: Eri kutsu jos haetaan vakio eilinen tai päivämäärällä (tai välillä) 
			//data = this.getData();
			if ((startDateStr != null) && (endDateStr != null)) {
				this.logger.log("Fetch data created between '" + startDateStr + "' - '" + endDateStr + "'");
				data = this.fetchData(this.username, this.password, this.url, startDateStr, endDateStr);
			} else {
				this.logger.log("Fetch data created or updated yesterday");
				data = this.fetchData(this.username, this.password, this.url);
			}
				
			//this.logger.log("## raw data: " + data);
			// TODO: check data for invalid characters
			// HUOM: Ei toimi koska json voi sisältää kommentteja joissa mitä tahansa merkkejä (esim <url> tjsp.)
			//this.validate(data);
		} catch (Exception e) {
			// Isto Saarinen 2021-12-01: muutettu IOException => Exception
			this.logger.log("Fatal error: Failed to download data");
			e.printStackTrace();
		}

		// Save data into S3

		if (!data.isEmpty()) {

			if (coordinateTransform) {
				try {
					// 3067 =ETRS89 / ETRS-TM35FIN, converts to 4326=wgs84
					this.enrichmentCenter = new EnrichServiceNowDataWithCoordinates(data, "EPSG:3067", this.outputSplitLimit);
					newJsonString = this.enrichmentCenter.enrichData();
				} catch (Exception e) {
					this.logger.log("Error: Could not add WGS84 coordinates to data or split limit not integer");
					this.logger.log("Error: WGS data: " + data.length());
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					e.printStackTrace(pw);
					String sStackTrace = sw.toString(); // stack trace as a string
					this.logger.log("Error: : " + sStackTrace);
				}

				// adds wgs84 coordinates
				if ((this.enrichmentCenter == null) || this.enrichmentCenter.enrichedList.isEmpty()) {
					this.logger.log("Error: Empty dataset");
					return false;
				} else if (this.enrichmentCenter.enrichedList.size() == 1) {
					this.logger.log("single array found");
					FileSpec outputFile = writer.makeDataFileName(this.sourceName);
					if(writer.writeDataFile(outputFile, newJsonString)) {
						if (this.manifestCreator != null) {
							this.manifestCreator.createManifest(outputFile);
						}
					}
					this.logger.log("single array ended");
				} else { // loop through array
					this.logger.log("multiarray found");
					int size = this.enrichmentCenter.enrichedList.size();
					for (int i = 0; i < size; i++) {
						FileSpec outputFile = writer.makeDataFileName(this.sourceName);
						if (writer.writeDataFile(outputFile, this.enrichmentCenter.enrichedList.get(i).toString())) {
							if (this.manifestCreator != null) {
								this.manifestCreator.createManifest(outputFile);
							}
						}
					}
					this.logger.log("multiarray ended");
				}

			} else {
				FileSpec outputFile = writer.makeDataFileName(this.sourceName);
				if (writer.writeDataFile(outputFile, data)) {
					if (this.manifestCreator != null) {
						this.manifestCreator.createManifest(outputFile);
					}
				}
				this.logger.log("result written");
			}
			
		}
		return true;
	}
	
	
	

	


	/**
	 * 1. Tarkistaa, etta merkijono on jsonia
	 * 2. Tarkistaa  etta sisaltaa vain sallittuja merkkeja
	 * 
	 * Datan latauksessa {@link #getData()} on jo koitettu tunnistaa alkuperainen merkisto
	 * 
	 * @param data, json merkkijono
	 */
	/*
	private void validate(String data) {
		// 1. Onko json
		// todo: tuplatarkastus, eli json muunnokset myohemmassa vaiheessa ajavat saman asian, voi mahd. poistaa
		// riippuen siita, halutaanko virhe havaita jo aiemmin ja keskitetysti
		try {
			new JSONObject(data);
		} catch (JSONException e) {
			try {
				new JSONArray(data);
			} catch (JSONException e2) {
				System.out.println("## Service now tiedosto ei ollut JSON muotoa");
				this.logger.log(this.alertString + "Service now tiedosto ei ollut JSON muotoa");
			}
		}
		
		// 2. Onko sisalto ok
		try {
			Pattern pattern = Pattern.compile(validchars);
			Matcher matcher = pattern.matcher(data);
			if(matcher.find()) {
				String invalidchars = matcher.group();
				this.logger.log(this.alertString + "JSON tiedostosta lyotyi odottamaton merkki: " + invalidchars);
			}
		} catch (Exception e) {
			this.logger.log("## Virhe regex tarkistuksessa: " + e.getMessage());
		}
	}
	*/
	
	/**
	 * Datan haku päivämäärävälille (tai yhdelle päivälle jos annettu sama päivä)
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 * @param startdate		Alkupäivä ('yyyy-MM-dd')
	 * @param enddate		Loppupäivä ('yyyy-MM-dd')
	 * @return JSON data
	 */
	public String fetchData(String username, String password, String url, String startdate, String enddate) {
		try {
			// Muunnos String -> DateTime
			DateTime startDate = new DateTime(startdate).withTime(0, 0, 0, 0);
			DateTime endDate = new DateTime(enddate).withTime(0, 0, 0, 0);
			DateTime processDate = startDate;
			StringBuffer sb = new StringBuffer();
			int counter = 1;
			// Loop: jokaiselle päivälle
			while( processDate.getMillis() <= endDate.getMillis() ) {
				if (counter > 1) sb.append(",");
				sb.append(this.fetchData(username, password, url, processDate.toString("yyyy-MM-dd")));
				this.log("Fetch data for date '" + processDate.toString("yyyy-MM-dd") + "'");
				processDate = processDate.plusDays(1);
				counter++;
			}
			// Lisätään listan ympärille result 
			return "{\"result\":[" + sb.toString() + "]}";
		} catch (Exception e) {
			// TODO: logging?
		}
		// Some error: return empty data
		return "";
	}
	
	
	
	
	/**
	 * Datan vakiohaku eilisen tapahtumille
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 * @return JSON data
	 */
	public String fetchData(String username, String password, String url) {
		return"{\"result\":[" + this.fetchData(username, password, url, null) + "]}"; 
	}
	
	
	
	
	/**
	 * Datan haku yhdelle päivälle (tai vakio eiliselle jos annettu pvm == null)
	 * 
	 * @author Isto Saarinen 2021-12-01
	 * 
	 * @param date		Päivä ('yyyy-MM-dd')
	 * @return JSON data
	 */
	public String fetchData(String username, String password, String url, String date) {
		long startts = System.currentTimeMillis();
		List<Long> apicalls = new ArrayList<Long>();

		String data = "";
		int size = 0;
		
		String query = this.queryStringDefault;
		
		if (date != null) {
			// Päivä annettu, kasataan hakuehto
			query = this.queryStringDate.replace("{DATEFILTER}", date);
		}
		
		try {
			StringBuffer sb = new StringBuffer();

			// Yritetään aina ensin ilman rajoituksia
			Integer offset = null;
			Integer limit = null;
			
			while(true) {
				// Kutsu offset+limit (tai ilman 1. kerralla)
				long startf = System.currentTimeMillis();
				DataHolder piece = this.serviceNowApiCall(offset, limit, username, password, url, query);
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
			data = "";
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
		
		this.log("## total records fetched for day '" + (date != null ? date : "yesterday") + "' = " + size + ", total time = " + (endts - startts) + " ms, total api fetch time = " + callstotal + " ms, average " + callsavg + " ms/fetch");
		
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
	private DataHolder serviceNowApiCall(Integer offset, Integer limit, String username, String password, String url, String path) throws Exception {

		long startts = System.currentTimeMillis();
	
		// Prepare login credentials and URL
		String login = username + ":" + password;
		String requestUrl = url + "?" + path;
		if ((offset != null) && (limit != null)) {
			requestUrl += this.argOffset + offset + this.argLimit + limit;
		}

		this.log("## make api call with url: " + requestUrl);
		String base64Login = new String(Base64.getEncoder().encode(login.getBytes()));

		// Open connection and read CSV
		URL uurl = new URL(requestUrl);
		URLConnection uc = uurl.openConnection();
		uc.setRequestProperty("Authorization", "Basic " + base64Login);
		uc.setRequestProperty("Accept", "application/json");
		
		// Kaytetaan buffered ja tikainputstreameja, jotta mark ja reset toimii
		// ja streami luetaan tunnistamiseen jalkeen taas alusta
		InputStream inputstream = uc.getInputStream();
		BufferedInputStream bufferedstream = new BufferedInputStream(inputstream);
		StringBuffer sb = null;

		
		// Start reading
		BufferedReader in = new BufferedReader(new InputStreamReader(bufferedstream, this.charset));

		sb = new StringBuffer();
		String line;
		while ((line = in.readLine()) != null) {
			sb.append(line + "\n");
		}
		in.close();
		
		String data = sb.toString();
		
		// Varmistus, loppuun tulee joskus ylimääräinen ',""'
		if (data.endsWith("}],\"\"}")) {
			data = data.replace("}],\"\"}", "}]}");
		}
		
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
		this.log("## api call '" + requestUrl + "' return status = " + dh.status + ", records = " + dh.recordCount + ", time = " + (endts - startts) + " ms");
		return dh;
	}
	
	
	
	
}
