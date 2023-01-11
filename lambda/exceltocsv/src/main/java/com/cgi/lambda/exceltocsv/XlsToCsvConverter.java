package com.cgi.lambda.exceltocsv;


import java.util.ArrayList;

import java.util.Date;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.joda.time.DateTime;





/**
 * 
 * Excel => CSV muunnin
 * 
 * Alkuperäinen versio 2013-10-16
 * Korjattu versio 2022-06-01
 * Muutettu kaikille lehdille 2022-11-01
 * 
 * @author isto.saarinen
 * 
 * 
 * 
 * 
 *
 */
public class XlsToCsvConverter {

	private static final String CONVERT_ALL_SHEETS = "*";
	
	private static final String NULLSTRING = "null";
	private boolean hasHeader = false;
	private String replaceCR = "";
	private String replaceNL = " ";
	private String delimiter = ";";
	private String[] sheetNames = null;
	private boolean includeSheetName = false;
	private String charSet = "UTF-8";
	private String eol = System.lineSeparator();
	private String quote = "\"";
	private String quoteEscape = "\"";
	private String[][] replaceHeaderChars = { {"ä","a"},  {"å","a"}, {"ö","o"}, {" ","_"} };
	private boolean trimData = false;
	private int skipheaders = 0;
	
	private SimpleLogger logger = null;
	

	private void log(String s) {
		if (this.logger != null) {
			this.logger.log(s);
		}
	}

	
	// Alkaako merkkijono jollakin UTF-8 miinus- merkillä ?
	// https://jkorpela.fi/dashes.html
	private boolean startsWithSomeDash(String s) {
		boolean b = false;
		int[] dashList = {
				45, 6150, 8208, 8209, 8210, 8211, 8212, 8213, 8315, 8331, 8722,  11834, 11835, 65112, 65123, 65293
		};
		int c = (int)s.charAt(0);
		for (int i : dashList) {
			if (c == i) {
				b = true;
			}
		}
		return(b);
	}
	
	// Viivojen korvaus ascii miinuksella
	private String replaceDashToAscii(String s) {
		boolean b = this.startsWithSomeDash(s);
		String r = s;
		if (b) {
			r = ((char)45) + s.substring(1);
		}
		return r;
	}
	
	
	public void setLogger(SimpleLogger logger) {
		this.logger = logger;
	}
	
	public void setHasHeader(boolean flag) {
		this.hasHeader = flag;
	}

	public void setReplaceCR(String replaceCR) {
		this.replaceCR = replaceCR;
	}
	
	public void setReplaceNL(String replaceNL) {
		this.replaceNL = replaceNL;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
	
	public void setQuote(String quote) {
		this.quote = quote;
	}
	
	public void setQuoteEscape(String quoteEscape) {
		this.quoteEscape = quoteEscape;
	}

	public void setSheetNames(String[] sheetNames) {
		this.sheetNames = sheetNames;
	}

	public void setCharSet(String charSet) {
		this.charSet = charSet;
	}
	
	public void setEOL(String eol) {
		this.eol = eol;
	}
	
	public void setTrimData(boolean trimData) {
		this.trimData = trimData;
	}
	
	public void setSkipheaders(int skipheaders) {
		this.skipheaders = skipheaders;
	}

	public void setIncludeSheetName(boolean includeSheetName) {
		this.includeSheetName = includeSheetName;
	}

	// Tulostettavan tiedon muotoilut
	private String fixOutputValue(String v) {
		String s = (v != null) ? v : "";
		if (s.equals(XlsToCsvConverter.NULLSTRING )) {
			s = "";
		}
		if (s.contains(this.quote)) {
			s = s.replace(this.quote, this.quoteEscape + this.quote);
		}
		if (this.replaceCR != null) {
			s = s.replace("\r", this.replaceCR);
		}
		if (this.replaceNL != null) {
			s = s.replace("\n", this.replaceNL);
		}
		return(s);
	}


	// Otsikon muotoilut
	private String fixHeader(String v, int column) {
		String s = (v != null) ? v : "";
		s = s.toLowerCase().trim();
		if (s.equals(XlsToCsvConverter.NULLSTRING )) {
			s = "";
		}
		if (s.contains(this.quote)) {
			s = s.replace(this.quote, "");
		}
		for (String[] m : this.replaceHeaderChars) {
			if (s.contains(m[0])) {
				s = s.replace(m[0], m[1]);
			}
		}
		if (s.length() < 1) {
			s = "c" + column;
		}
		return(s);
	}
	
	
	
	
	// Numeron muotoilu
	private String formatNumber(Double d, DecimalFormat decimalFormat) {
		String t = decimalFormat.format(d);
	    // Remove trailing zeros
	    t = t.replaceAll("0+$", "");
	    // Remove decimal point (is integer or number without decimals)
	    t = t.replaceAll("\\.$", "");
	    return this.replaceDashToAscii(t);    
	}

	private String formatDate(Date dt) {
		return new DateTime(dt).toString("yyyy-MM-dd HH:mm:ss");
	}


	
	
	
	// Muunna. Kutsuja antaa in ja out streamit
	public RunStatusDto convert(InputStream in, OutputStream out) {
		RunStatusDto result = new RunStatusDto();
		result.setStatus(false);
		Workbook workbook = null;
		DataFormatter formatter = null;
		DecimalFormat decimalFormat = null;
		Sheet xlsheet = null;
        
		try {
            workbook = WorkbookFactory.create(in);
            formatter = new DataFormatter(true);
			DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
			decimalFormatSymbols.setDecimalSeparator('.');
			decimalFormat = new DecimalFormat("0.0000000000", decimalFormatSymbols);
		} catch (Exception e) {
        	this.log("convert(): workbook open failed: '" + e.toString() + "': '" + e.getMessage() + "'");
        	result.setErrorMessage(e.toString() + ", " + e.getMessage());
        	result.setStatus(false);
        	return(result);
        }
		if (workbook != null) {

			// Annettujen lehtien tarkastus
			if (this.sheetNames != null) {
				if (this.sheetNames.length == 1) {
					if (this.sheetNames[0] == null) {
						// 0: Ei annettua lehteä -> muunnetaan ensimmäinen lehti
						this.sheetNames = new String[1];
						this.sheetNames[0] = workbook.getSheetName(0);
					} else if (CONVERT_ALL_SHEETS.equalsIgnoreCase(this.sheetNames[0])) {
						// 1: Annettu lehti "convert all sheets" -> muunnetaan kaikki lehdet samaan kohteeseen.
						int sheets = workbook.getNumberOfSheets();
						this.sheetNames = new String[sheets];
						for (int i = 0; i < sheets; i++) {
							this.sheetNames[i] = workbook.getSheetName(i);
						}
					} // 2: Muunnetaan listatut lehdet
				} // 2: Muunnetaan listatut lehdet
			} else if (this.sheetNames == null) {
				// 3: == 0: Ei annettua lehteä -> muunnetaan ensimmäinen lehti
				this.sheetNames = new String[1];
				this.sheetNames[0] = workbook.getSheetName(0);
			}
			
			// Muunnetaan lehdet
			this.log("Convert " + this.sheetNames.length + " sheets");
			for (int i = 0; i < this.sheetNames.length; i++) {
				String sheetName = this.sheetNames[i];
				this.log("Convert sheet = '" + sheetName + "'");
				xlsheet = workbook.getSheet(sheetName); 
				if (xlsheet != null) {
					boolean useHeader = (i == 0);
					if (!this.hasHeader) useHeader = true;
					result = this.convertSheet(xlsheet, formatter, decimalFormat, useHeader, sheetName, in, out);
					if (!result.getStatus()) {
						// Error: log & exit
						this.log("Conversion error, sheet = '" + sheetName + "' : '" + result.getErrorCode() + "', '" + result.getErrorMessage() + "'. Exit");
						return(result);
					}
				} else {
					// Invalid sheet name: log & continue
					this.log("Convert sheet '" + sheetName + "': not found, continue.");
					
				}
			}
			this.log("Convert done, return status = " + result.getStatus());
		}
		
		return(result);
	}
	
	

	
	
	// Muunna lehti
	public RunStatusDto convertSheet(Sheet xlsheet, DataFormatter formatter, DecimalFormat decimalFormat, boolean useHeader, String sheetName, InputStream in, OutputStream out) {
		RunStatusDto result = new RunStatusDto();
		result.setStatus(false);
		int cols = 0;
		int col = 0;
		int rows = 0;
		int row = 0;
		String value = "";
        Row xlrow = null;
        Cell xlcell = null;
		
		if (xlsheet != null) {
				
			int datalines = 0;
			
			// ohitetaan jos ei datarivejä
			if(xlsheet.getPhysicalNumberOfRows() > 0) {
				rows = xlsheet.getLastRowNum();

				// Käsitellään kaikki rivit annetusta eteenpäin
				for(row = this.skipheaders; row <= rows; row++) {
					xlrow = xlsheet.getRow(row);
					if (xlrow != null) {
						List<String> ln = new ArrayList<String>();

						if (row == this.skipheaders) {
							
							// Jos otsikko on olemassa, otetaan sarakemäärä siitä
							if (this.hasHeader) {
								cols = xlrow.getLastCellNum();
								// Haetaan lopusta ensimmäinen ei- tyhjä sarake ==>> joka sarakkeella pitää olla otsikko määritettynä
								for(col = cols; col >= 0; col--) {
									xlcell = xlrow.getCell(col);
									value = formatter.formatCellValue(xlcell);
									if (value == null) value = "";
									if (value.equalsIgnoreCase(XlsToCsvConverter.NULLSTRING)) {
										value = "";
									}
									if (!value.equals("")) {
										cols = col;
										break;
									}
								}
							} else {
								// Ei otsikkoa, otetaan kaikki sarakkeet ensimmäiseltä luettavalta riviltä
								cols = xlrow.getLastCellNum();
							}
							this.log("Sheet '" + sheetName + "': skip rows before header = " + this.skipheaders + ", columns = " + cols + ", rows = " + rows + ", include sheet name = " + this.includeSheetName);
						}
						
						if (this.includeSheetName) {
							// Lisätään lehden nimi ensimmäiseksi sarakkeeksi
							if (this.hasHeader && (row == this.skipheaders) && useHeader) {
								value = this.fixHeader("SHEET_NAME", 0);
							} else {
								value = this.fixOutputValue(sheetName);
							}
							ln.add(value);
						}
							
						// Muotoillaan sarakkeen data
						for(col = 0; col <= cols; col++) {
							xlcell = xlrow.getCell(col);
							value = ""; 
							if (xlcell != null) {

								if (xlcell.getCellType() == CellType.NUMERIC) {
									// Numero tai pvm
									double d = xlcell.getNumericCellValue();
									if (DateUtil.isCellDateFormatted(xlcell)) {
										// Pvm
										value = this.formatDate(xlcell.getDateCellValue());
	                        		} else {
	                        			// Numero
	                        			value = this.formatNumber(d, decimalFormat);
	   	                        	}
	                        	} else if (xlcell.getCellType() == CellType.FORMULA) {
	                        		// Kaava
	                        		CellType t = xlcell.getCachedFormulaResultType();
                                       if (t == CellType.NUMERIC) {
                                    	   double d = xlcell.getNumericCellValue();
                                    	   if (DateUtil.isCellDateFormatted(xlcell)) {
                                    		   // Pvm
                                    		   value = this.formatDate(xlcell.getDateCellValue());
                                    	   } else {
                                    		   // Numero
                                    		   value = this.formatNumber(d, decimalFormat);
                                    	   }
                                       } else if (t == CellType.BOOLEAN) {
                                    	   // 
                                    	   if (xlcell.getBooleanCellValue()) {
                                    		   value = "1";
                                    	   } else {
                                    		   value = "0";
                                    	   }
                                       } else if (t == CellType.ERROR) {
                                    	   // Virhe => tyhjä arvo
                                    	   value = "";
                                       } else {
                                    	   value = xlcell.getStringCellValue();
                                       }
	                        	
	                        	} else {
	                        		value = formatter.formatCellValue(xlcell);
	                        	}
								
	                        }
							ln.add(this.fixOutputValue(value));
	                    }

						int c = 0;
						String s = "";

						if ((this.hasHeader) && (row == this.skipheaders)) {
							// Otsikon kirjoitus
							if (useHeader) {
								for (String v : ln) {
									if (c > 0) {
										s += this.delimiter;
									}
									s += this.fixHeader(v, c);
									c++;
								}
							}
						} else {
							// Datan kirjoitus
							for (String v : ln) {
								if (c > 0) {
									s += this.delimiter;
								}
								if (this.quote != null) {
									s += this.quote;
								}
								if (this.trimData) {
									s += v.trim();
								} else {
									s += v;
								}
								if (this.quote != null) {
									s += this.quote;
								}
								c = 1;
							}
						}

						if (this.hasHeader && (row == this.skipheaders) && (!useHeader)) {
							// Ohitetaan otsikko
							//logger.log("Sheet '" + sheetName + "': skip data line " + (row + 1) + " = [header]");
						} else {
							//logger.log("Sheet '" + sheetName + "': write data line " + (row + 1) + " = [data]");
							//logger.log("Sheet '" + sheetName + "': write data line " + (row + 1) + " = [" + s.toString() + "]");
							s += this.eol;
							try {
								out.write(s.getBytes(this.charSet));
								datalines++;
							} catch (Exception e) {
								result.setErrorMessage(e.toString() + ", " + e.getMessage());
								result.setStatus(false);
								return(result);
							}
						}
					
					}
                }
            }

			this.log("Sheet '" + sheetName + "': data lines written: " + datalines);				
			result.setStatus(true);
		}

		return result;
	}	


}
