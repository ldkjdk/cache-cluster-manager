package com.dhgate;

import java.lang.reflect.Method;

/**
 * 
 * @author lidingkun
 *
 */
public class CommandProcess {

	private Object object ;
	private Class<?> [] parameterTypes;
	private String methodName;
	private Object [] parameters;
	public CommandProcess(Object object,String methodName, Class<?>[] parameterTypes,Object [] parameters) {
		super();
		this.object = object;
		this.methodName = methodName;
		this.parameterTypes = parameterTypes;
		this.parameters = parameters;
	}
	
	public void exec () throws Exception {
		Method m = object.getClass().getMethod(this.methodName,this.parameterTypes);
		m.invoke(this.object,this.parameters);
	}
}
