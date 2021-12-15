package com.cgi.lambda.apifetch;


import java.io.File;
import java.io.IOException;
//import java.net.URISyntaxException;
import java.nio.file.Files;
//import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
//import java.util.Scanner;

import org.junit.Assert;
//import org.junit.BeforeClass;
import org.junit.Test;

//import com.amazonaws.services.lambda.runtime.Context;
//import com.cgi.lambda.apifetch.LambdaFunctionHandlerOld;


public class EnrichServiceNowDataWithCoordinatesTest {

	   
	/**
	 * Tests that WGS coordinates are added and coordinate conversion has succeeded.
	 */
    @Test
    public void testLambdaFunctionHandler() {
    	TestContext ctx = new TestContext();
    	ClassLoader classLoader = getClass().getClassLoader();
    	File file = new File(classLoader.getResource("testdata.txt").getFile());    	
    	String text="";
    	try {
			text = new String(Files.readAllBytes(file.toPath()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println(new SimpleDateFormat("dd.MM.yyyy").format(new Date()) +"/part"+0+"/" + "u_case.json");
    	text=text.trim();
    	EnrichServiceNowDataWithCoordinatesOld enrich = new EnrichServiceNowDataWithCoordinatesOld(ctx,text,"EPSG:3067",100);
    	String enrichedData=enrich.enrichData();        
        Assert.assertTrue(enrichedData.contains("WGS84-x"));// checks that conversion is successful
        Assert.assertTrue(enrichedData.contains("WGS84-y"));// checks that conversion is successful
        Assert.assertTrue(enrichedData.contains("24.66944311796")); // checks that conversion is successful
        Assert.assertTrue(enrichedData.contains("60.1847232502")); // checks that conversion is successful
        Assert.assertTrue(enrichedData.contains("WGS84-x\":\"\"")); //checks that null value is inserted when no coordinates are given
        
    }
    

    
    
}
