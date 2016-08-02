package com.ery.ertc.collect.forwards;

import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Bytes;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;
import com.ery.base.support.utils.hash.HashFunctions;

public class TestQueue {

	public static ConcurrentLinkedQueue<String> collList = new ConcurrentLinkedQueue<String>();
	private static AtomicLong queueSize = new AtomicLong(0);// 队列大小
	private static AtomicLong coll = new AtomicLong(0);// 采集
	private static AtomicLong send = new AtomicLong(0);// 发送
	static AtomicBoolean state = new AtomicBoolean(false);

	static class TestFilter {
		byte[] b;

		TestFilter(byte[] b) {
			this.b = b;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof TestFilter) {
				TestFilter tf = (TestFilter) o;
				return Bytes.equals(this.b, tf.b);
			}
			return false;
		}
	}

	private static byte[] getHash(byte[] data, byte[] part, int bucketIdxHash) {
		byte[] hashs = new byte[3 * 4];// 一个hashNum计算后占4个字节
		for (int i = 0; i < 3; i++) {
			if (i == 0 && part != null) {
				System.arraycopy(Convert.toBytes(bucketIdxHash), 0, hashs, 0, 4);
			} else {
				System.arraycopy(Convert.toBytes(HashFunctions.hash(data, i)), 0, hashs, i * 4, 4);
			}
		}
		return hashs;
	}

	static class Key {
		byte[] hash;
		byte canDel;
		Key next;

		Key(byte[] hash) {
			this.hash = hash;
		}
	}

	private static volatile Instrumentation globalInstr;

	public static void premain(String args, Instrumentation inst) {
		globalInstr = inst;
	}

	public static long getObjectSize(Object obj) {
		if (globalInstr == null)
			throw new IllegalStateException("Agent not initted");
		return globalInstr.getObjectSize(obj);
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		long stime = 0;
		final Random random = new Random();
		ExecutorService pool = Executors.newFixedThreadPool(3);
		Object[] arr = new Object[2];
		int iii = 0;
		arr[iii++] = 1;

		Utils.sleep(100000000);
		final Thread th = new Thread() {
			public void run() {
				while (true) {
					if (state.compareAndSet(true, false)) {
						LogUtils.info("1");
					}
				}
			}
		};
		th.start();
		Thread test = new Thread() {
			public void run() {
				int i = 0;
				while (true) {
					Utils.sleep(1);
					try {
						if (i % 1000 == 0) {
							state.compareAndSet(false, true);
						}
					} catch (Exception e) {
						LogUtils.info(e);
					}
					i++;
				}
			}
		};

		pool.submit(test);
		Utils.sleep(1000000);
		//
		// BloomFilter bf = new BloomFilter(1000000,7,false);
		// LogUtils.info(bf.getCapacitySize()+":"+bf.getFalsePositiveProbability());
		// Object[] a = new Object[]{1,"asdfa",123.3,"ss",new byte[]{1,2,3}};
		// LogUtils.info(StringUtils.join(a,","));

		// SystemConstant.setSYS_HAS_CONF_FILE(false);// 无需配置文件
		ExecutorService ES = Executors.newFixedThreadPool(30);
		byte[] b = new byte[500];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) (i % 127);
		}
		final String c = new String(b);

		Runnable collRun = new Runnable() {
			@Override
			public void run() {
				int i = 0;
				while (i < 10000000) {
					try {
						collList.offer(c);
						queueSize.addAndGet(1);
						coll.addAndGet(1);
					} catch (Exception e) {
						e.printStackTrace();
					}
					i++;
				}
			}
		};

		Runnable sendRun = new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						boolean ret = false;
						ret = collList.poll() != null;
						if (ret) {
							queueSize.addAndGet(-1);
							send.addAndGet(1);
						}
					} catch (Exception e) {
						System.out.println(e.getMessage());
						Utils.sleep(1000);
					}
				}
			}
		};

		ES.submit(collRun);
		ES.submit(sendRun);

		long lc = 0;
		long ls = 0;
		while (true) {
			Utils.sleep(1000);
			long _lc = coll.get();
			long _ls = send.get();
			long qs = queueSize.get();
			System.out.println("listSize:" + qs + ",send总数:" + _ls + ",  coll总数:" + _lc + ",  sendTps:" + (_ls - ls) +
					",  collTps:" + (_lc - lc));
			lc = _lc;
			ls = _ls;
		}

	}

}
