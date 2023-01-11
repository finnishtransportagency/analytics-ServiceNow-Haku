package com.cgi.lambda.exceltocsv;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class SimpleLambdaLogger implements SimpleLogger {

	private LambdaLogger logger = null;
	

	public SimpleLambdaLogger(Context context) {
		this.logger = context.getLogger();
	}
	
	
	@Override
	public void log(String s) {
		if (this.logger != null) {
			this.logger.log(s);
		}
	}
	
}
