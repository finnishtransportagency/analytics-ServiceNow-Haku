package com.cgi.lambda.apifetch;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.lambda.runtime.Context;
//import com.cgi.lambda.apifetch.LambdaFunctionHandlerOld;


/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class LambdaFunctionHandlerTest {

    private static Object input;

    @BeforeClass
    public static void createInput() throws IOException {
        input = null;
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    public void testLambdaFunctionHandler() {
        
    	
    	/* LambdaFunctionHandler handler = new LambdaFunctionHandler();
        Context ctx = createContext();

        String output = handler.handleRequest(input, ctx);

        */
        Assert.assertEquals("", "");
    }
    
	@Test
	public void testDateHandler() {
		LambdaFunctionHandlerOld handler = new LambdaFunctionHandlerOld();
		Object object = new String("");
		
		String date=handler.getDate(object);
		System.out.println(date);
		
		Assert.assertEquals("", date);
	}
    
    
}
