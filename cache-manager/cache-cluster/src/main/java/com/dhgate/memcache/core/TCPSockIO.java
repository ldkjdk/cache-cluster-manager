package com.dhgate.memcache.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.memcache.schooner.SchoonerSockIO;

public class TCPSockIO extends SchoonerSockIO {

	private static Logger log = LoggerFactory.getLogger(TCPSockIO.class);
	// data
	private String host;
	private Socket sock;

	public java.nio.channels.SocketChannel sockChannel;

	private int hash = 0;

	/**
	 * creates a new SockIO object wrapping a socket connection to
	 * host:port, and its input and output streams
	 * 
	 * @param host
	 *            hostname:port
	 * @param timeout
	 *            read timeout value for connected socket
	 * @param connectTimeout
	 *            timeout for initial connections
	 * @param noDelay
	 *            TCP NODELAY option?
	 * @throws IOException
	 *             if an io error occurrs when creating socket
	 * @throws UnknownHostException
	 *             if hostname is invalid
	 */
	public TCPSockIO(String host, int bufferSize, int timeout, int connectTimeout,
			boolean noDelay, boolean isPooled) throws IOException, UnknownHostException {

		super(bufferSize);

		// allocate a new receive buffer
		String[] ip = host.split(":");

		// get socket: default is to use non-blocking connect
		sock = getSocket(ip[0], Integer.parseInt(ip[1]), connectTimeout);
		
		if (isPooled)
			writeBuf = ByteBuffer.allocateDirect(bufferSize);
//		    writeBuf = ByteBuffer.allocate(bufferSize);
		else
			writeBuf = ByteBuffer.allocate(bufferSize);

		if (timeout >= 0)
			this.sock.setSoTimeout(timeout);

		// testing only
		sock.setTcpNoDelay(noDelay);

		// wrap streams
		sockChannel = sock.getChannel();
		hash = sock.hashCode();
		this.host = host;
	}

	/**
	 * Method which gets a connection from SocketChannel.
	 * 
	 * @param host
	 *            host to establish connection to
	 * @param port
	 *            port on that host
	 * @param timeout
	 *            connection timeout in ms
	 * 
	 * @return connected socket
	 * @throws IOException
	 *             if errors connecting or if connection times out
	 */
	protected final static Socket getSocket(String host, int port, int timeout) throws IOException {
		SocketChannel sock = SocketChannel.open();
		sock.socket().connect(new InetSocketAddress(host, port), timeout);
		return sock.socket();
	}

	/**
	 * Lets caller get access to underlying channel.
	 * 
	 * @return the backing SocketChannel
	 */
	@Override
	public final SocketChannel getChannel() {
		return sock.getChannel();
	}

	/**
	 * returns the host this socket is connected to
	 * 
	 * @return String representation of host (hostname:port)
	 */
	@Override
	public final String getHost() {
		return this.host;
	}

	/**
	 * closes socket and all streams connected to it
	 * 
	 * @throws IOException
	 *             if fails to close streams or socket
	 */
	@Override
	public final void trueClose() throws IOException {
		readBuf.clear();

		boolean err = false;
		StringBuilder errMsg = new StringBuilder();

		if (sockChannel == null || sock == null) {
			err = true;
			errMsg.append("++++ socket or its streams already null in trueClose call");
		}

		if (sockChannel != null) {
			try {
				sockChannel.close();
			} catch (IOException ioe) {
				log.error("++++ error closing input stream for socket: " + toString() + " for host: " + getHost());
				log.error(ioe.getMessage(), ioe);
				errMsg.append("++++ error closing input stream for socket: " + toString() + " for host: "
						+ getHost() + "\n");
				errMsg.append(ioe.getMessage());
				err = true;
			}
		}

		if (sock != null) {
			try {
				sock.close();
			} catch (IOException ioe) {
				log.error("++++ error closing socket: " + toString() + " for host: " + getHost());
				log.error(ioe.getMessage(), ioe);
				errMsg.append("++++ error closing socket: " + toString() + " for host: " + getHost() + "\n");
				errMsg.append(ioe.getMessage());
				err = true;
			}
		}

		sockChannel = null;
		sock = null;

		if (err)
			throw new IOException(errMsg.toString());
	}

	/**
	 * sets closed flag and checks in to connection pool but does not close
	 * connections
	 */
	public final void close() {
		readBuf.clear();
		try {
			this.getPool().returnResource(this);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			try {
				this.getPool().returnBrokenResource(this);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
	}

	/**
	 * checks if the connection is open
	 * 
	 * @return true if connected
	 */
	public boolean isConnected() {
		return (sock != null && sock.isConnected());
	}

	/**
	 * checks to see that the connection is still working
	 * 
	 * @return true if still alive
	 */
	
	@Override
	public final boolean isAlive() {
		if (!isConnected())
			return false;

		// try to talk to the server w/ a dumb query to ask its version
		try {
			write("version\r\n".getBytes());
			readBuf.clear();
			sockChannel.read(readBuf);
		} catch (IOException ex) {
			log.error("isAlive",ex);
			return false;
		}

		return true;
	}

	/**
	 * read fix length data from server and store it in the readBuf
	 * 
	 * @param length
	 *            data length
	 * @throws IOException
	 *             if an io error happens
	 */
	public final void readBytes(int length) throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to read from closed socket");
			throw new IOException("++++ attempting to read from closed socket");
		}

		int readCount;
		while (length > 0) {
			readCount = sockChannel.read(readBuf);
			length -= readCount;
		}
	}

	/**
	 * writes a byte array to the output stream
	 * 
	 * @param b
	 *            byte array to write
	 * @throws IOException
	 *             if an io error happens
	 */
	@Override
	public void write(byte[] b) throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to write to closed socket");
			throw new IOException("++++ attempting to write to closed socket");
		}
		sockChannel.write(ByteBuffer.wrap(b));
	}

	/**
	 * writes data stored in writeBuf to server
	 * 
	 * @throws IOException
	 *             if an io error happens
	 */
	@Override
	public void flush() throws IOException {
		writeBuf.flip();
		sockChannel.write(writeBuf);
	}

	/**
	 * use the sockets hashcode for this object so we can key off of SockIOs
	 * 
	 * @return int hashcode
	 */
	public final int hashCode() {
		return (sock == null) ? 0 : hash;
	}

	/**
	 * returns the string representation of this socket
	 * 
	 * @return string
	 */
	public final String toString() {
		return (sock == null) ? "" : sock.toString();
	}

	/**
	 * Hack to reap any leaking children.
	 */
	protected final void finalize() throws Throwable {
		try {
			if (sock != null) {
				// log.error("++++ closing potentially leaked socket in finalize");
				sock.close();
				sock = null;
			}
		} catch (Throwable t) {
			log.error(t.getMessage(), t);

		} finally {
			super.finalize();
		}
	}

	//@Override
	public short preWrite() {
		// should do nothing, this method is for UDP only.
		return 0;
	}

	//@Override
	public byte[] getResponse(short rid) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	//@Override
	public void clearEOL() throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to read from closed socket");
			throw new IOException("++++ attempting to read from closed socket");
		}

		byte[] b = new byte[1];
		boolean eol = false;
		InputStream in = sock.getInputStream();
		while (in.read(b, 0, 1) != -1) {

			// only stop when we see
			// \r (13) followed by \n (10)
			if (b[0] == 13) {
				eol = true;
				continue;
			}

			if (eol) {
				if (b[0] == 10)
					break;

				eol = false;
			}
		}
	}

	//@Override
	public int read(byte[] b) throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to read from closed socket");
			throw new IOException("++++ attempting to read from closed socket");
		}

		int count = 0;
		InputStream in = sock.getInputStream();
		while (count < b.length) {
			int cnt = in.read(b, count, (b.length - count));
			count += cnt;
		}

		return count;
	}

	//@Override
	public String readLine() throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to read from closed socket");
			throw new IOException("++++ attempting to read from closed socket");
		}

		byte[] b = new byte[1];
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		boolean eol = false;
		InputStream in = sock.getInputStream();
		while (in.read(b, 0, 1) != -1) {

			if (b[0] == 13) {
				eol = true;
			} else {
				if (eol) {
					if (b[0] == 10)
						break;

					eol = false;
				}
			}

			// cast byte into char array
			bos.write(b, 0, 1);
		}

		if (bos == null || bos.size() <= 0) {
			throw new IOException("++++ Stream appears to be dead, so closing it down");
		}

		// else return the string
		return bos.toString().trim();
	}

	//@Override
	public void trueClose(boolean addToDeadPool) throws IOException {
		trueClose();
	}

	@Override
	public ByteChannel getByteChannel() {
		// TODO Auto-generated method stub
		return null;
	}
}
