package com.dhgate.memcache.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.dhgate.memcache.schooner.SchoonerSockIO;

public class UDPSockIO extends SchoonerSockIO {

	/**
	 * 
	 * <p>
	 * UDP datagram frame header:
	 * </p>
	 * <p>
	 * +------------+------------+------------+------------+
	 * </p>
	 * <p>
	 * | Request ID | Sequence | Total | Reserved |
	 * </p>
	 * <p>
	 * +------------+------------+------------+------------+
	 * </p>
	 * <p>
	 * | 2bytes | 2bytes | 2bytes | 2bytes |
	 * </p>
	 * <p>
	 * +------------+------------+------------+------------+
	 * </p>
	 */
	public static Short REQUESTID = (short) 0;
	public static final short SEQENCE = (short) 0x0000;
	public static final short TOTAL = (short) 0x0001;
	public static final short RESERVED = (short) 0x0000;

	private static ConcurrentMap<String, byte[]> data = new ConcurrentHashMap<String, byte[]>();

	private class UDPDataItem {

		private short counter = 0;
		private boolean isFinished = false;
		private int length = 0;

		private short total;

		public synchronized short getTotal() {
			return total;
		}

		public synchronized void setTotal(short total) {
			if (this.total == 0)
				this.total = total;
		}

		public synchronized short getCounter() {
			return counter;
		}

		public synchronized short incrCounter() {
			return ++counter;
		}

		public synchronized boolean isFinished() {
			return isFinished;
		}

		public synchronized void setFinished(boolean isFinished) {
			this.isFinished = isFinished;
		}

		public synchronized int getLength() {
			return length;
		}

		public synchronized void addLength(int alength) {
			this.length += alength;
		}
	}

	public static ConcurrentMap<Short, UDPDataItem> dataStore = new ConcurrentHashMap<Short, UDPDataItem>();

	public DatagramChannel channel;

	private Selector selector;

	@Override
	public void trueClose() throws IOException {
		if (selector != null) {
			selector.close();
			channel.close();
		}
		
	}

	public UDPSockIO(String host, int bufferSize, int timeout, boolean isPooled) throws IOException,
			UnknownHostException {
		super(bufferSize);

		String[] ip = host.split(":");
		channel = DatagramChannel.open();
		channel.configureBlocking(false);
		SocketAddress address = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]));
		channel.connect(address);
		channel.socket().setSoTimeout(timeout);
		selector = Selector.open();
		((DatagramChannel) channel).register(selector, SelectionKey.OP_READ);
		if (isPooled)
			writeBuf = ByteBuffer.allocateDirect(bufferSize);
//		    writeBuf = ByteBuffer.allocate(bufferSize);
		else
			writeBuf = ByteBuffer.allocate(bufferSize);
	}

	@Override
	public ByteChannel getByteChannel() {
		return channel;
	}

	@Override
	public short preWrite() {
		writeBuf.clear();
		short rid = 0;
		synchronized (REQUESTID) {
			REQUESTID++;
			rid = REQUESTID.shortValue();
		}
		writeBuf.putShort(rid);
		writeBuf.putShort(SEQENCE);
		writeBuf.putShort(TOTAL);
		writeBuf.putShort(RESERVED);
		return rid;
	}

	@Override
	public byte[] getResponse(short rid) throws IOException {

		long timeout = 1000;
		long timeRemaining = timeout;

		int length = 0;
		byte[] ret = null;
		short requestID;
		short sequence;

		UDPDataItem mItem = new UDPDataItem();
		UDPSockIO.dataStore.put(rid, mItem);

		long startTime = System.currentTimeMillis();
		while (timeRemaining > 0 && !mItem.isFinished()) {
			int n = selector.select(500);
			if (n > 0) {
				// we've got some activity; handle it
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				while (it.hasNext()) {
					SelectionKey skey = it.next();
					it.remove();
					if (skey.isReadable()) {
						DatagramChannel sc = (DatagramChannel) skey.channel();
						// every loop handles one datagram.
						do {
							readBuf.clear();
							sc.read(readBuf);
							length = readBuf.position();
							if (length <= 8) {
								break;
							}
							readBuf.flip();
							// get the udpheader of the response.
							requestID = readBuf.getShort();
							UDPDataItem item = UDPSockIO.dataStore.get(requestID);
							if (item != null && !item.isFinished) {
								item.addLength(length - 8);
								sequence = readBuf.getShort();
								item.setTotal(readBuf.getShort());
								readBuf.getShort(); // get reserved

								// save those datagram into a map, so we
								// can change the sequence of them.
								byte[] tmp = new byte[length - 8];
								readBuf.get(tmp);
								item.incrCounter();
								data.put(requestID + "_" + sequence, tmp);
								if (item.getCounter() == item.getTotal()) {
									item.setFinished(true);
								}
							}
						} while (true);
					}
				}

			} else {
				// timeout likely... better check
				// TODO: This seems like a problem area that we need to
				// figure out how to handle.
				// log.error("selector timed out waiting for activity");
				break;
			}

			timeRemaining = timeout - (System.currentTimeMillis() - startTime);
		}

		if (!mItem.isFinished) {
			UDPSockIO.dataStore.remove(rid);
			for (short sq = 0; sq < mItem.getTotal(); sq = (short) (sq + 1)) {
				data.remove(rid + "_" + sq);
			}
			return null;
		}

		// rearrange the datagram's sequence.
		int counter = mItem.getLength();
		ret = new byte[counter];
		counter = 0;
		boolean isOk = true;
		for (short sq = 0; sq < mItem.getTotal(); sq = (short) (sq + 1)) {
			byte[] src = data.remove(rid + "_" + sq);
			if (src == null)
				isOk = false;
			if (isOk) {
				System.arraycopy(src, 0, ret, counter, src.length);
				counter += src.length;
			}
		}
		UDPSockIO.dataStore.remove(rid);
		// selector.close();

		if (!isOk)
			return null;

		return ret;
	}

	@Override
	public void close() {
		readBuf.clear();
		writeBuf.clear();
		try {
			getPool().returnResource(this);
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

	@Override
	public String getHost() {
		return channel.socket().getInetAddress().getHostName();
	}

	//@Override
	public void clearEOL() throws IOException {
		// TODO Auto-generated method stub

	}

	//@Override
	public int read(byte[] b) {
		// TODO Auto-generated method stub
		return 0;
	}

	//@Override
	public String readLine() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	//@Override
	public void trueClose(boolean addToDeadPool) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public final SocketChannel getChannel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAlive() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		
	}
}