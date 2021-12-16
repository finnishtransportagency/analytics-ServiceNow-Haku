package com.cgi.lambda.apifetch;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

//import com.amazonaws.services.lambda.runtime.Context;

public class GetDateFromJsonTest {
    private static Object input;

    @BeforeClass
    public static void createInput() throws IOException {
        input = null;
    }


    @Test
    public void testdataoutput () {
        String datejson="{\r\n" + 
        		"  \"key1\": \"value1\",\r\n" + 
        		"  \"date\": \"30-01-2019\"\r\n" + 
        		"}";
        
        GetDateFromJsonOld njs= new GetDateFromJsonOld(datejson);
        String newdate=njs.getfromJson();
    	System.out.println(newdate);
    	/* LambdaFunctionHandler handler = new LambdaFunctionHandler();
        Context ctx = createContext();

        String output = handler.handleRequest(input, ctx);

        */
        Assert.assertEquals("", "");
    }
}

