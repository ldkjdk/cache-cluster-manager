package com.dhgate.ssdb;

/**
 * The class for SSDBException. It will be thrown when processing pool object. 
 * 
 * @author lidingkun
 *
 */
@SuppressWarnings("serial")
public class SSDBException extends RuntimeException {

	public SSDBException() {
		super();
	}

	public SSDBException(String message, Throwable cause) {
		super(message, cause);
	}

	public SSDBException(String message) {
		super(message);
	}

	public SSDBException(Throwable cause) {
		super(cause);
	}

}
