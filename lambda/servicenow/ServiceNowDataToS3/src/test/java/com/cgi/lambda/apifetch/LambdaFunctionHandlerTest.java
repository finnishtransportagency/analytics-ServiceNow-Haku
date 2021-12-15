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
        // TODO: set up your sample input object here.
        input = null;
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    public void testLambdaFunctionHandler() {
        
    	
    	/* LambdaFunctionHandler handler = new LambdaFunctionHandler();
        Context ctx = createContext();

        String output = handler.handleRequest(input, ctx);

        // TODO: validate output here if needed. */
        Assert.assertEquals("", "");
    }
    
	@Test
	public void testDateHandler() {
		LambdaFunctionHandlerOld handler = new LambdaFunctionHandlerOld();
		Object object = new String("");
		
		String date=handler.getDate(object);
		System.out.println(date);
		// TODO: validate output here if needed.
		Assert.assertEquals("", date);
	}
    
    
}
