package com.dhgate;

import java.util.List;
/**
 * 
 * @author lidingkun
 *
 * @param <T>
 */
public interface RoutingAlg<T> {

	T getServerObject();
	T getServerObject(String key);
	T getServerObject(byte[] key);
	int getServerIndex();
	List<T> getAllServerObject();
}
