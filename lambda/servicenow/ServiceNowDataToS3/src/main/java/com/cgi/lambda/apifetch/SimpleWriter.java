package com.cgi.lambda.apifetch;

public interface SimpleWriter {

	
	public boolean writeData(String fileName, String data);
	
	public String createOutputFileName(String sourceFileName);
	
}
