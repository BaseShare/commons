package com.sinoiov.yyzc.commons.kafka.exceptions;

public class YyzcKafkaException extends RuntimeException{
	
	private static final long serialVersionUID = -2401036488100061745L;

	protected String errorCode;
	
	protected String message;

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public YyzcKafkaException() {
		super("NotificationException");

	}

	public YyzcKafkaException(String errorCode) {
		this.errorCode = errorCode;
	}

	public YyzcKafkaException(String errorCode, String message) {
		this.errorCode = errorCode;
		this.message = message;
	}

	public YyzcKafkaException(String errorCode, String message, Throwable cause) {
		super(message, cause);
		this.errorCode = errorCode;
		this.message = message;
	}

	public YyzcKafkaException(String message, Throwable cause) {
		super(message, cause);
		this.message = message;
	}

	public YyzcKafkaException(Throwable cause) {
		super(cause);
	}

	public static void throwExcetion(YyzcKafkaException exception) {
		throw exception;
	}
}
