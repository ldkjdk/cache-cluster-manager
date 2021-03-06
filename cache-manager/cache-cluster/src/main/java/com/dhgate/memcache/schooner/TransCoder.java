/*******************************************************************************
 * Copyright (c) 2009 Schooner Information Technology, Inc.
 * All rights reserved.
 * 
 * http://www.schoonerinfotech.com/
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.dhgate.memcache.schooner;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link TransCoder} is used to customize the serialization and deserialization
 * in memcached operations.
 * 
 * @see ObjectTransCoder
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see TransCoder
 */
public interface TransCoder {
	/**
	 * decode the object from the inputstream.
	 * 
	 * @param input
	 *            inputstream.
	 * @return decoded java object.
	 * @throws IOException
	 *             error happened in decoding the input stream.
	 */
	Object decode(final InputStream input) throws IOException;

	/**
	 * encode the java object into outputstream.
	 * 
	 * @param out
	 *            outputstream, you can in get written length of bytes in
	 *            {@link SockOutputStream}.
	 * @param object
	 *            object to be encoded.
	 * @return written size, which is used in memcached set operation.
	 * @throws IOException
	 *             error happened in encoding the output stream.
	 */
	int encode(final SockOutputStream out, final Object object) throws IOException;
}
