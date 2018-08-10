package com.dhgate.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class ObjectBytesExchange {
	 private static final Logger log = LoggerFactory.getLogger(ObjectBytesExchange.class);
	/**
	 * Object to Bytes
	 * 
	 * @param obj
	 * @return
	 */
	public static byte[] toByteArray(Object obj) {
		if (!(obj instanceof Serializable)) {
			throw new IllegalArgumentException(ObjectBytesExchange.class.getSimpleName() + " requires a Serializable payload " +
					"but received an object of type [" + obj.getClass().getName() + "]");
		}
		byte[] bytes = null;
		ObjectOutputStream oos = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			oos = new ObjectOutputStream(bos);
			oos.writeObject(obj);
			oos.flush();
			bytes = bos.toByteArray();
			oos.close();
			oos = null;
			bos.close();
			bos = null;
		} catch (IOException ex) {
			log.error("ObjectBytesExchange.toByteArray() throw a IOException.",ex);
			
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (IOException e) {
					log.error("ObjectBytesExchange.toByteArray() >>> oos.close() throw a IOException.",e);
				}
			}
			if (null != bos) {
				try {
					bos.close();
				} catch (IOException e) {
					log.error("ObjectBytesExchange.toByteArray() >>> bos.close() throw a IOException.",e);
				}
			}
		}
		return bytes;
	}

	/**
	 * Bytes to Object
	 * 
	 * @param bytes
	 * @return
	 */
	public static Object toObject(byte[] bytes) {
		Object obj = null;
		ObjectInputStream ois = null;
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		try {
			ois = new ObjectInputStream(bis);
			obj = ois.readObject();
			ois.close();
			ois = null;
			bis.close();
			bis = null;
		} catch (IOException ex) {
			
			log.error("ObjectBytesExchange.toObject() throw a IOException.",ex);
		} catch (ClassNotFoundException ex) {
			log.error("ObjectBytesExchange.toObject() throw a ClassNotFoundException.",ex);
		} finally {
			if (ois != null) {
				try {
					ois.close();
				} catch (IOException e) {
					log.error("ObjectBytesExchange.toObject() >>> ois.close() throw a IOException.",e);
				}
			}
			if (null != bis) {
				try {
					bis.close();
				} catch (IOException e) {
					log.error("ObjectBytesExchange.toObject() >>> bis.close() throw a IOException.",e);
				}
			}
		}
		return obj;
	}
}