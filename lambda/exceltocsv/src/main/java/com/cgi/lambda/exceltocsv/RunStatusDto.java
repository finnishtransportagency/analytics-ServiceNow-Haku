package com.cgi.lambda.exceltocsv;

public class RunStatusDto {


	private Long jobKey;
	
	private Boolean status = false;

	private String statusMessage;

	private Object[] statusMessageArgs;
	
	private String job;
	
	private String errorCode;
	
	private String errorMessage;
	
	private long rows = 0;

	public Long getJobKey() {
		return jobKey;
	}

	public void setJobKey(Long jobKey) {
		this.jobKey = jobKey;
	}

	public Boolean getStatus() {
		return status;
	}

	public void setStatus(Boolean status) {
		this.status = status;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	public void setStatusMessage(String statusMessage) {
		this.statusMessage = statusMessage;
	}

	public Object[] getStatusMessageArgs() {
		return statusMessageArgs;
	}

	public void setStatusMessageArgs(Object[] statusMessageArgs) {
		this.statusMessageArgs = statusMessageArgs;
	}

	public String getJob() {
		return job;
	}

	public void setJob(String job) {
		this.job = job;
	}

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public long getRows() {
		return rows;
	}

	public void setRows(long rows) {
		this.rows = rows;
	}

}
