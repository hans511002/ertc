package com.ery.ertc.collect.handler.distinct;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.handler.Distinct;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Bytes;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;
import com.ery.base.support.utils.hash.HashFunctions;

/**
 * Copyrights @ 2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @author wangcs
 * @description 用于去重过滤的缓存Hash（用于大数据实时去重）
 * @date 14-5-9　原理： 1，借助内存缓存文件位置索引，从文件读取数据来提高元素定位能力，通过hash提高并发，通过链表提供索引速度
 *       2，内存中维护一个固定大小的hash桶，每个桶分内存和文件缓存两部分，内存中存的是量固定大小的链表数据结构，文件中分块顺序存放。
 *       3，文件搜索，文件头占8字节，存文件元数据信息，后续分块存放数据，每个桶按顺序对应一个块，每个块占用1024b的空间：
 *       块头用4b存当前块的记录数，4字节存下一个块的索引，（块头8个字节）
 *       剩余部分存数据，已默认每条记录12字节来说。一个块可存84条记录（可根据数据动态变化）。= (1024-8)/(每个记录的大小)。
 *       写文件一个单独的线程进行，保证了各个桶数据不混乱。
 *       读文件，利用扩展的随机读写缓冲接口实现，1次磁盘操作，便可快速检索整个桶(压力峰值情况下也只需要多一次磁盘操作)。
 *       3，重复判定，通过h1(hash算法
 *       )算出桶，在桶内的链表从第一个元素查找，先找内存，再找文件。如果h1,h2,h3完全相同则认定重复。否则视为新元素，加入到当前桶。
 *       每个被判定的元素都会无条件加入到内存中(加入链表头，如果超出则淘汰链表尾——类LRU算法，一般来说认为时间上接近的两个元素有更大的几率重复)。
 *       同时也保证了，并发情况下，连续两个相同元素能被识别。 异步线程将新元素缓存到文件。
 *       4，资源占用。内存中维护桶的数量N。(默认桶内每个元素占40字节,其他文件列表内存可忽略) occupySize ≈ N*(100*40)
 *       。。。 百万级，1万个桶，每桶100个元素 40mb 千万级消息，400 m 亿级，4g
 *       10亿级，只能缩小桶数，1000W个桶，每桶10个。4g
 *       数据量级，桶内存容量。桶文件块大小。皆可配置。更大的数据量级也可通过划分时间粒度来减少内存消耗。
 * 
 * 
 * @modify
 * @modifyDate -
 */
public class FilterCacheHash {
	// 基本信息(不变)
	private int numLevel;// 容量级别
	private int bucketNum;// 桶数
	private byte memBucketCap;
	private int blockBitLen;// 块 位长度
	private int blockKeyNum;// 块最大存储数
	private long fileCycle;// 业务排重。时间范围（毫秒）。一个小时为3600*1000毫秒
	private String filePath;// 文件名
	private Conf conf;// 配置

	/**
	 * hash桶，提高并发。一个空桶8byte。 每隔固定时间将数据缓存到文件，存到文件后。从内存中清除掉数据 内部存放链表，提高搜索速度。
	 */
	private LinkedKey[] buckets;// 桶
	// 线程安全的文件队列。时间早的在前面
	private Hashtable<Byte, FileCache> fileCacheHash = new Hashtable<Byte, FileCache>();
	private byte fileIdx = 0;// 文件索引循环计数(0 > 127> -128 > 0 > 127 ……)
	private Thread lruThread;// 缓存淘汰线程

	/**
	 * @param conf
	 *            配置信息
	 */
	public FilterCacheHash(Conf conf) {
		this.conf = conf;
		init(conf);
	}

	// 初始
	private void init(final Conf conf) {
		blockBitLen = conf.getBlockBitLen();
		blockKeyNum = ((1 << blockBitLen) - 8) / 12;
		numLevel = conf.getNumLevel();
		bucketNum = (int) Utils.getMinDistancePrimeNum(numLevel / 100, true);// 每个桶预装100个元素算出桶数。桶数取质数
		if (conf.getMemBucketCap() != 0) {
			memBucketCap = conf.getMemBucketCap();
		} else if (conf.getMemLimit() != 0) {
			int mem = (int) ((conf.getMemLimit() * 1024 * 1024 - 3) / (4 * 2.8));
			if (mem > Conf.DEFAULT_MEM_BUCKET_CAP) {
				memBucketCap = Conf.DEFAULT_MEM_BUCKET_CAP;
			} else if (mem >= 5) {
				memBucketCap = (byte) mem;
			} else {
				throw new IllegalArgumentException("[" + conf.getBusKey() + "]容量级别与内存限制不对应,请至少保证内存中每个桶至少放5个元素");
			}
		} else {
			memBucketCap = Conf.DEFAULT_MEM_BUCKET_CAP;
		}

		buckets = new LinkedKey[bucketNum];// 定义容量
		fileCycle = conf.getFileCycle();
		filePath = conf.getFilePath();
		File pf = new File(filePath).getParentFile();
		if (!pf.exists()) {
			pf.mkdirs();
		}
		final long currentTm = System.currentTimeMillis();
		final List<FileCache> fcList = new ArrayList<FileCache>();
		pf.listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				String name = f.getName();
				if (name.startsWith(Distinct.DISTINCT_CACHE_FILE_NAME) && !name.endsWith(".tmp")) {
					long tm = currentTm - Convert.toLong(name.substring(21));
					int idx = (int) (tm / fileCycle);
					if (idx < conf.getCheckRange()) {
						fcList.add(new FileCache(f));
					}
				}
				return false;// 返回false，标示被过滤掉
			}
		});
		if (fcList.size() > 1) {
			Collections.sort(fcList, new Comparator<FileCache>() {
				@Override
				public int compare(FileCache o1, FileCache o2) {
					return (int) (o1.stime - o2.stime);
				}
			});
		}

		if ((fcList.size() > 0 && (currentTm - fcList.get(0).stime) * 1.0 / fileCycle > 1) || fcList.size() == 0) {
			// 时间过期，生成新文件
			File file = new File(filePath + "." + Convert.toTimeStr(new Date(currentTm), "MMdd-HHmmss") + "." +
					currentTm);
			fcList.add(new FileCache(file));// 加入一个新文件在末尾
			if (fcList.size() > conf.getCheckRange()) {
				fcList.remove(0).destory(true);// 超过。删除第一个文件
			}
		}

		for (FileCache fc : fcList) {
			fileIdx++;
			fc.fileByte = fileIdx;
			fileCacheHash.put(fileIdx, fc);
		}

		lruThread = new Thread() {
			@Override
			public void run() {
				while (true) {
					Utils.sleep(10000);// 10秒淘汰一次
					List<LinkedKey> keys = new ArrayList<LinkedKey>(127);// 每个key
					List<LinkedKey> parKeys = new ArrayList<LinkedKey>(127);// 每个key对应的父key
					for (int idx = 0; idx < bucketNum; idx++) {
						LinkedKey key = buckets[idx];
						if (key != null && key.addedNum + 128 >= memBucketCap) {
							keys.clear();
							parKeys.clear();
							int lruNum = key.addedNum + 128 - memBucketCap + memBucketCap / 4;// 每次至少淘汰1/4的数据
							LinkedKey parKey = null;
							for (; key != null; key = key.next) {
								keys.add(key);
								parKeys.add(parKey);
								parKey = key;
							}

							// 不淘汰链表头，依次从最后一位淘汰满足条件的
							int realLruNum = 0;// 实际淘汰数量
							for (int x = keys.size() - 1; x > 0 && realLruNum < lruNum; x--) {
								LinkedKey tk = keys.get(x);
								LinkedKey tk_p = parKeys.get(x);
								if (tk.rw_state == 1 && tk_p != null) {
									synchronized (tk_p) {
										tk_p.next = tk.next;// 移除当前指针，将当前后续的链，连接到父
									}
									realLruNum++;
								}
							}
							if (realLruNum > 0) {
								synchronized (buckets[idx]) {
									buckets[idx].addedNum = (byte) (buckets[idx].addedNum - realLruNum);
								}
							}
						}
					}
				}
			}
		};
	}

	public void shutup() {
		for (FileCache fileCache : fileCacheHash.values()) {
			fileCache.start();
		}
		lruThread.start();
	}

	// 释放资源
	public void shutdown() {
		lruThread.stop();
		long tm = System.currentTimeMillis();
		for (FileCache fileCache : fileCacheHash.values()) {
			if (tm - fileCache.stime <= fileCycle * conf.getCheckRange()) {
				fileCache.destory(false);
			} else {
				fileCache.destory(true);
			}
		}
		fileCacheHash.clear();
		buckets = null;
	}

	// 获取摘要信息
	public String getSummaryInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append("[桶数:").append(bucketNum);
		sb.append(",缓存:").append(fileCacheHash.size());
		sb.append(",最大桶容:").append(memBucketCap);
		int min = Integer.MAX_VALUE;
		int max = 0;
		int d = 0;
		int g = 0;
		int sum = 0;
		int zero = 0;
		for (int i = 0; i < bucketNum; i++) {
			byte size = -128;
			if (buckets[i] != null) {
				size = buckets[i].addedNum;
			} else {
				zero++;
			}
			size += 128;
			min = Math.min(size, min);
			max = Math.max(size, max);
			if (size < 15) {
				d += 1;
			}
			if (size > 85) {
				g += 1;
			}
			sum += size;
		}
		sb.append(",空桶:").append(zero);
		sb.append(",桶容<15:").append(d);
		sb.append(",桶容>85:").append(g);
		sb.append(",最小:").append(min);
		sb.append(",最大:").append(max);
		sb.append(",总数:").append(sum);
		double avg = sum * 1.0 / bucketNum;
		sb.append(",平均:").append(avg);

		double fc = 0;
		for (int i = 0; i < bucketNum; i++) {
			byte size = -128;
			if (buckets[i] != null) {
				size = buckets[i].addedNum;
			}
			size += 128;
			fc += Math.pow(size - avg, 2);
		}
		fc /= bucketNum;
		sb.append(",方差:").append(fc);
		sb.append("]");
		return sb.toString();
	}

	public boolean containsFilter(byte[] data, long time) {
		return containsFilter(data, null, time);
	}

	public boolean containsFilter(byte[] data, byte[] part) {
		return containsFilter(data, part, 0);
	}

	public boolean containsFilter(byte[] data) {
		return containsFilter(data, null, 0);
	}

	/**
	 * 如果包含则过滤掉此元素。否则将元素加入集合
	 * 
	 * @param data
	 *            数据
	 * @param part
	 *            分区key
	 * @param time
	 *            时间（标示出数据该落地在哪个文件区上）
	 * @return false标示没找到元素
	 */
	public boolean containsFilter(byte[] data, byte[] part, long time) {
		int[] hashs = getHash(data, part);
		int bucketIdxHash = hashs[0];
		int bucketIdx = bucketIdxHash > 0 ? bucketIdxHash % bucketNum : -bucketIdxHash % bucketNum;
		LinkedKey thisKey = new LinkedKey(hashs);// 本次数据的key
		// 准备将新对象加到链表头，避免这条记录其他线程无法获取到
		// 超出范围后，建立基于时间先后顺序的缓存淘汰机制（淘汰尾）
		// 加入时继承上个头存储的noCacheSize。这样写磁盘线程可直接访问头即可知有当前桶有多少数据未缓存(当达到一定量时才刷新磁盘。避免频繁写磁盘)
		LinkedKey key = buckets[bucketIdx];
		if (!selectFileCache(thisKey, time)) {
			LogUtils.warn("[" + conf.getBusKey() + "]记录时间[" + time + "]超出时间范围!默认返回找到!");
			return true;
		}
		if (key == null) {
			synchronized (buckets) {
				if (buckets[bucketIdx] == null) {
					buckets[bucketIdx] = new LinkedKey(hashs);
					buckets[bucketIdx].noCacheSize++;
					buckets[bucketIdx].addedNum++;
					return false;
				}
			}
		}
		synchronized (buckets[bucketIdx]) {
			thisKey.noCacheSize = (byte) (buckets[bucketIdx].noCacheSize + 1);
			thisKey.addedNum = (byte) (buckets[bucketIdx].addedNum + 1);
			thisKey.next = buckets[bucketIdx];
			buckets[bucketIdx] = thisKey;
		}
		LinkedKey lastKey = thisKey;// 从头开始搜索
		for (key = lastKey.next; key != null; key = key.next) {
			if (key.equals(hashs)) {
				// 因为当前记录已被加入头。相当于内存里存在2条记录，需要删除此条记录，将后面的链链上
				synchronized (buckets[bucketIdx]) {
					buckets[bucketIdx].addedNum--;
					buckets[bucketIdx].noCacheSize--;
				}
				synchronized (lastKey) {
					lastKey.next = key.next;
				}
				return true;// 找到了。返回
			}
			lastKey = key;
		}

		// 去文件中找。如果有time参数，需要判断，确定去那个文件区找
		boolean found = false;
		byte[] buf = new byte[1 << blockBitLen];// 读一个块的内容
		try {
			long threadId = Thread.currentThread().getId();
			int offset = 8 + bucketIdx * (1 << blockBitLen);// 块初始位置
			byte[] hashBytes = convertHashBytes(hashs);// 文件中搜索时用字节判断，减少转换
			if (time == 0) {
				// 去所有复合时间范围的文件找
				long tm = System.currentTimeMillis();
				byte bt = fileIdx;
				for (FileCache fileCache = fileCacheHash.get(bt); fileCache != null; fileCache = fileCacheHash.get(bt)) {
					if (tm - fileCache.stime <= fileCycle * conf.getCheckRange()) {
						if (!fileCache.rafReadOpt.containsKey(threadId)) {
							fileCache.rafReadOpt.put(threadId, new RandomAccessFile(fileCache.file, "rw"));
						}
						found = fileCache.find(hashBytes, offset, buf, fileCache.rafReadOpt.get(threadId));
					} else {
						break;// 时间已越界
					}
					bt--;
				}
			} else {
				// 去指定时间文件找
				FileCache fileCache = fileCacheHash.get(thisKey.fileByte);
				if (fileCache != null) {
					if (!fileCache.rafReadOpt.containsKey(threadId)) {
						fileCache.rafReadOpt.put(threadId, new RandomAccessFile(fileCache.file, "rw"));
					}
					found = fileCache.find(hashBytes, offset, buf, fileCache.rafReadOpt.get(threadId));
				}
			}
		} catch (Exception e) {
			LogUtils.warn("[" + conf.getBusKey() + "]文件搜索出错!", e);
		}
		if (found) {
			thisKey.rw_state = 1;// 此记录可被删除（文件中存在）
			synchronized (buckets[bucketIdx]) {
				buckets[bucketIdx].noCacheSize--;
			}
			return true;// 找到了
		} else {
			thisKey.rw_state = 2;// 可缓存
			return false;
		}
	}

	private long t;

	/**
	 * 选择文件缓存，主要是为当前key计算出需要落地的文件索引
	 * 
	 * @param key
	 * @param time
	 * @return 返回true标示此记录找到了对应的文件，返回false。标示为找到对应的文件
	 */
	boolean selectFileCache(LinkedKey key, long time) {
		try {
			if (time == 0) {
				key.fileByte = fileCacheHash.get(fileIdx).fileByte;// 直接指向当前
				return true;
			} else {
				// 如果有时间，则对应到指定时间段上的文件
				byte bt = fileIdx;
				for (FileCache fileCache = fileCacheHash.get(bt); fileCache != null; fileCache = fileCacheHash.get(bt)) {
					if (fileCache.stime <= time) {// 正好在此区间
						key.fileByte = fileCache.fileByte;
						return true;
					}
					bt--;
				}
				return false;
			}
		} finally {
			long tt = System.currentTimeMillis();
			if (t == 0) {
				t = tt;
			}
			if (tt - t > fileCycle) {
				// 超出一个文件周期，生成新文件
				File file = new File(filePath + "." + Convert.toTimeStr(new Date(tt), "MMdd-HHmmss") + "." + tt);
				FileCache fileCache = new FileCache(file);
				fileCache.fileByte = (byte) (fileIdx + 1);
				fileCacheHash.put((byte) (fileIdx + 1), fileCache);
				fileIdx++;
				fileCache.start();

				byte bt = fileIdx;
				for (fileCache = fileCacheHash.get(fileIdx); fileCache != null; fileCache = fileCacheHash.get(bt)) {
					if (tt - fileCache.stime > fileCycle * conf.getCheckRange()) {
						fileCacheHash.remove(bt).destory(true); // 超期
					}
					bt--;
				}

				t = tt;
			}
		}
	}

	// 获取hash
	private static int[] getHash(byte[] data, byte[] part) {
		int[] hashs = new int[3];// 3个hash
		if (part != null) {// 放循环外判断，减少判断
			hashs[0] = HashFunctions.hash(part, 0);
			for (int i = 1; i < 3; i++) {
				hashs[i] = HashFunctions.hash(data, i);
			}
		} else {
			for (int i = 0; i < 3; i++) {
				hashs[i] = HashFunctions.hash(data, i);
			}
		}
		return hashs;
	}

	private static byte[] convertHashBytes(int... hashInt) {
		byte[] hashs = new byte[12];// 3个hash
		for (int i = 0; i < 3; i++) {
			System.arraycopy(Bytes.toBytes(hashInt[i]), 0, hashs, i * 4, 4);
		}
		return hashs;
	}

	private static int[] convertHashInts(byte[] src) {
		int[] hashs = new int[3];
		for (int i = 0; i < 3; i++) {
			hashs[i] = Bytes.toInt(src, i * 4, 4);
		}
		return hashs;
	}

	// 文件缓存
	class FileCache {
		private byte fileByte;// 文件字节标识
		private long stime;// 起始时间
		private File file;// 文件
		private String fileName;// 文件名
		// 为什么用两个列表呢？一个写，一个读
		private Thread cacheTh;
		private Thread destoryTh;// 释放资源线程，定期释放掉不活跃的文件指针

		private RandomAccessFile rafRWOpt; // 随机读写操作
		private Map<Long, RandomAccessFile> rafReadOpt = new HashMap<Long, RandomAccessFile>(); // 随机读操作(一个线程对一个)
		private int state = 0;

		FileCache(File f) {
			this.file = f;
			fileName = file.getName();
			stime = Convert.toLong(fileName.substring(fileName.lastIndexOf(".") + 1));
			cacheTh = new Thread() {
				public void run() {
					while (state == 1) {
						Utils.sleep(Conf.DEFAULT_CACHE_CYCLE * 1000);// 每5秒缓存一次数据到磁盘
						try {
							cache(false);
						} catch (Exception e) {
							LogUtils.error("[" + conf.getBusKey() + "]刷新数据到磁盘[" + fileName + "]出错!", e);
						}
					}
				}
			};
			if (!file.exists()) {
				try {
					file.createNewFile();
				} catch (IOException e) {
					LogUtils.error("[" + conf.getBusKey() + "]创建文件[" + fileName + "]出错!", e);
				}
			}

			// 读取文件头。代表了文件元数据信息
			RandomAccessFile raf = null;
			RandomAccessFile raft = null;
			try {
				if (file.length() > 0) {
					raf = new RandomAccessFile(file, "r");
					byte[] b = new byte[4];
					raf.read(b);
					int bsize = Convert.toInt(b);// 桶大小
					raf.read(b);
					int blkl = Convert.toInt(b);// 块大小
					int blkn = ((1 << blkl) - 8) / 12;// 块最大可存储数
					if (bsize != bucketNum && blockBitLen != blkl) {
						// 桶大小不一致，或者桶块大小不一致，说明元数据定义发生过改变。需要将数据挪移重新计算到新文件
						File tmpFile = new File(file.getPath() + ".tmp");
						if (!tmpFile.exists()) {
							tmpFile.createNewFile();
						}
						initNewFile(tmpFile);

						raft = new RandomAccessFile(tmpFile, "rw");// 新文件随机写指针
						List<byte[]> keys = new ArrayList<byte[]>();
						byte[] buf = new byte[1 << blkl];// 块缓冲区。一次申明，避免new
						b = new byte[8];
						for (int i = 0; i < bsize; i++) {
							int offset = 8 + i * buf.length;// 起始位置索引
							readToMem(file.length(), offset, raf, keys, buf, blkn);// 将每个桶对应的数据加载到内存中
							if (keys.size() > 0) {
								int bhash = Bytes.toInt(keys.get(0), 0, 4);
								int newIdx = bhash > 0 ? bhash % bucketNum : -bhash % bucketNum;// 桶索引
								// 如果bucket为空，则填充一条数据在其中
								if (buckets[newIdx] == null) {
									int hash1 = Bytes.toInt(keys.get(0), 4, 4);
									int hash2 = Bytes.toInt(keys.get(0), 8, 4);
									synchronized (buckets) {
										if (buckets[newIdx] == null) {
											LinkedKey key = new LinkedKey(bhash, hash1, hash2);
											key.rw_state = 1;// 标示此记录属于文件中（可从内存中删除)
											key.addedNum = -127;
											buckets[newIdx] = key;
										}
									}
								}
								offset = 8 + newIdx * (1 << blockBitLen);// 新文件块位置
								boolean newblockFlag = true;
								boolean changed = false;
								int fillsize = 0;// 已填充大小
								for (int j = 0; j < keys.size();) {
									if (newblockFlag) {
										raft.seek(offset);
										raft.read(b);// 读出块头
										fillsize = Bytes.toInt(b, 0, 4);
										newblockFlag = false;
										changed = false;
										raft.seek(offset + 8);// 跳到数据起始位
									}
									if (fillsize == blockKeyNum) {
										if (changed) {// 改变过，将新数据写入磁盘
											raft.seek(offset);
											raft.write(Bytes.toBytes(fillsize));
										}
										offset = Bytes.toInt(b, 4, 4);
										if (offset == 0) {
											raft.seek(offset + 4);
											// 需要添加一个新块
											offset = (int) raft.length();
											raft.setLength(offset + (1 << blockBitLen));
											raft.write(Bytes.toBytes(offset));
										}
										fillsize = 0;
										newblockFlag = true;
										continue;
									}
									byte[] hash = keys.get(j);
									fillsize++;
									raft.write(hash);
									changed = true;
									j++;
								}
								if (fillsize > 0) {// 填充有数据，将数据写入磁盘
									raft.seek(offset);
									raft.write(Bytes.toBytes(fillsize));
								}
							}
						}
						raf.close();
						raft.close();
						file.delete();// 删除原始文件
						tmpFile.renameTo(new File(file.getPath()));// 临时文件重命名
						file = new File(file.getPath());
					} else {
						// 读出至少一条数据到内存中
						b = new byte[8];
						byte[] hash = new byte[12];
						for (int i = 0; i < bsize; i++) {
							if (buckets[i] == null) {
								int offset = 8 + i * (1 << blkl);// 其实位置索引
								raf.seek(offset);
								raf.read(b);
								int fillsize = Bytes.toInt(b, 0, 4);
								if (fillsize == 0) {
									continue;
								}
								raf.read(hash);
								int[] hashints = convertHashInts(hash);
								synchronized (buckets) {
									if (buckets[i] == null) {
										LinkedKey key = new LinkedKey(hashints);
										key.rw_state = 1;
										key.addedNum = -127;
										buckets[i] = key;
									}
								}
							}
						}
					}
				} else {
					initNewFile(file);
				}
			} catch (Exception e) {
				LogUtils.error("[" + conf.getBusKey() + "]初始头数据[" + fileName + "]出错!", e);
			} finally {
				try {
					if (raf != null)
						raf.close();
					if (raft != null)
						raft.close();
				} catch (IOException e) {
					LogUtils.error(null, e);
				}
				try {
					rafRWOpt = new RandomAccessFile(file.getPath(), "rw");
				} catch (IOException e) {
					LogUtils.error("[" + conf.getBusKey() + "]初始文件[" + fileName + "]流操作接口出错!", e);
				}
			}
		}

		// 初始一个新文件
		void initNewFile(File file) throws IOException {
			RandomAccessFile wrt = null;
			try {
				wrt = new RandomAccessFile(file, "rw");// 有数据的话覆盖原数据
				wrt.setLength(0);// 清空文件
				int size = 8 + bucketNum * (1 << blockBitLen);
				wrt.setLength(size);// 设置新长度
				// 先8字节存元数据，然后分块存数据
				wrt.write(Convert.toBytes(bucketNum));// 桶数
				wrt.write(Convert.toBytes(blockBitLen));// 块长度
			} finally {
				if (wrt != null) {
					wrt.close();
				}
			}
		}

		// 读取数据到内存
		void readToMem(long size, int offset, RandomAccessFile raf, List<byte[]> hashList, byte[] buf, int maxNum)
				throws IOException {
			if (offset != 0 && offset < size) {
				raf.seek(offset);
				raf.read(buf);// 一次读一个块的数据
				int num = Bytes.toInt(buf, 0, 4);// 数量
				int nextOf = Bytes.toInt(buf, 4, 4);// 下一个块的位置（如果没有则为0）
				byte[] b;
				for (int i = 0; i < num; i++) {
					b = new byte[12];
					System.arraycopy(buf, 8 + i * b.length, b, 0, b.length);
					hashList.add(b);
				}
				if (num == maxNum && nextOf > 0) {// 本块已达到最大数量
					readToMem(size, nextOf, raf, hashList, buf, maxNum);
				}
			}
		}

		/**
		 * 缓存到磁盘(此线程每5秒执行1次)
		 * 
		 * @param igstate
		 *            忽略状态
		 * @throws IOException
		 */
		void cache(boolean igstate) throws IOException {
			byte[] b = new byte[8];// 用来存储块头
			for (int i = 0; i < bucketNum && (state != 0 || igstate); i++) {
				if (buckets[i] != null) {
					if (buckets[i].noCacheSize > -122 || igstate) {// 未缓存的超过5个.刷磁盘
						int len = buckets[i].addedNum + 128;
						LinkedKey[] keys = new LinkedKey[len];// 本次未写入的数据
						int j = 0, idx = 0;
						for (LinkedKey key = buckets[i]; key != null && j < len; key = key.next, j++) {
							if (key.fileByte == this.fileByte && key.rw_state == 2) {// 可缓存
								keys[idx] = key;
								idx++;
							}
						}
						// 开始落地
						int offset = 8 + i * (1 << blockBitLen);// 起始块的位置
						boolean newblockFlag = true;
						boolean changed = false;
						int fillsize = 0;// 已填充大小
						for (int x = 0; x < idx;) {
							if (newblockFlag) {
								rafRWOpt.seek(offset);
								rafRWOpt.read(b);// 读出块头
								fillsize = Bytes.toInt(b, 0, 4);
								newblockFlag = false;
								changed = false;
								rafRWOpt.seek(offset + 8);// 跳到数据起始位
							}
							if (fillsize == blockKeyNum) {
								if (changed) {// 改变过，将新数据写入磁盘
									rafRWOpt.seek(offset);
									rafRWOpt.write(Bytes.toBytes(fillsize));
								}
								offset = Bytes.toInt(b, 4, 4);
								if (offset == 0) {
									rafRWOpt.seek(offset + 4);
									// 需要添加一个新块
									offset = (int) rafRWOpt.length();
									rafRWOpt.setLength(offset + (1 << blockBitLen));
									rafRWOpt.write(Bytes.toBytes(offset));
								}
								fillsize = 0;
								newblockFlag = true;
								continue;
							}
							LinkedKey key = keys[x];
							fillsize++;
							rafRWOpt.write(convertHashBytes(key.hash0, key.hash1, key.hash2));
							changed = true;
							key.rw_state = 1;// 已写入磁盘。标记为可删除状态
							x++;
						}
						if (fillsize > 0) {
							rafRWOpt.seek(offset);
							rafRWOpt.write(Bytes.toBytes(fillsize));
							synchronized (buckets[i]) {
								buckets[i].noCacheSize -= idx;
							}
						}
					}
				}
			}
		}

		// 存储
		private void cache(PriorityQueue<HashKey> keys, RandomAccessFile raft, boolean igstate) throws IOException {
			if (keys.size() > 0) {
				// 有数据
				int idx = -1;
				byte[] b = new byte[8];
				long offset = 0;
				boolean newblockFlag = false;
				boolean changed = false;
				int fillsize = 0;// 已填充大小
				HashKey key = null;
				for (;;) {
					if (!igstate && state == 0 && !newblockFlag)
						break;
					if (key == null) {
						key = keys.poll();
					}
					if (key == null)
						break;
					if (idx != key.bucketIdx) {
						if (idx != -1) {
							if (fillsize > 0) {// 保存上次记录数据
								raft.seek(offset);
								raft.write(Bytes.toBytes(fillsize));
							}
						}
						idx = key.bucketIdx;
						offset = 8 + idx * (1 << blockBitLen);// 块位置
						newblockFlag = true;
					}
					if (newblockFlag) {
						raft.seek(offset);// 随机找到块
						raft.read(b);// 读出块头
						fillsize = Bytes.toInt(b, 0, 4);
						newblockFlag = false;
						changed = false;
						raft.seek(offset + 8);// 数据起始位置
					}
					if (fillsize == blockKeyNum) {
						if (changed) {// 改变过，将新数据写入磁盘
							raft.seek(offset);
							raft.write(Bytes.toBytes(fillsize));
						}
						offset = Bytes.toInt(b, 4, 4);
						if (offset == 0) {
							raft.seek(offset + 4);
							offset = raft.length();
							raft.setLength(raft.length() + (1 << blockBitLen));// 添加一个新块
							raft.write(Bytes.toBytes(offset));
						}
						fillsize = 0;
						newblockFlag = true;
						continue;
					}
					fillsize++;
					raft.write(key.hash);
					changed = true;
					key = null;
				}
				if (fillsize > 0) {// 填充有数据，将数据写入磁盘
					raft.seek(offset);
					raft.write(Convert.toBytes(fillsize));
				}
			}
		}

		/**
		 * 从文件中找。
		 * 
		 * @param key
		 *            需要搜索的key
		 * @param offset
		 *            起始索引
		 * @param buf
		 *            一个块的内容【由外部传入，避免每次new】
		 * @param raf
		 *            文件指针
		 * @return
		 */
		boolean find(byte[] key, int offset, byte[] buf, RandomAccessFile raf) throws IOException {
			if (offset > 0 && offset < raf.length()) {
				try {
					raf.seek(offset);
					raf.read(buf);// 一次读一个块
					int num = Bytes.toInt(buf, 0, 4);
					for (int i = 0; i < num; i++) {
						if (Bytes.equals(buf, 8 + i * key.length, key.length, key)) {
							return true;
						}
					}
					if (num == blockKeyNum) {
						offset = Bytes.toInt(buf, 4, 4);
						return find(key, offset, buf, raf);
					}
				} catch (Exception e) {
					return false;
				}
			}
			return false;
		}

		void start() {
			state = 1;
			cacheTh.start();
		}

		// 释放资源。delFile标示是否删除资源
		void destory(boolean delFile) {
			if (state == 1) {
				state = 0;
				Utils.sleep(100);
				cacheTh.stop();
			}
			try {
				if (!delFile) {
					try {
						cache(true);
					} catch (Exception e) {
						LogUtils.error(null, e);
					}
				}

				// 关闭连接
				for (RandomAccessFile raf : rafReadOpt.values()) {
					raf.close();
				}
				rafReadOpt.clear();
				rafRWOpt.close();
				if (delFile) {
					file.delete();
				}
			} catch (Exception e) {
				LogUtils.error("[" + conf.getBusKey() + "]释放文件[" + fileName + "]缓存资源出错!", e);
			}
		}

	}

	// 链表结构的key【一个对象32字节 = this(8) + 3个int(12) + next指针(8) = 28 什么内存对齐之类的，最终占32.
	// （因此其实多余了4个字节，未被使用，不用白不用，可用来计数、标识状态等,正好用来记录一系列状态）】
	public static class LinkedKey {
		int hash0;
		int hash1;
		int hash2;

		byte rw_state = 0;// 读写状态。0标示刚加入内存，1标示已缓存可删。2标示可缓存（为什么要加入此数据呢，保证不重复写入）
		byte fileByte;// 文件索引byte (循环索引)
		// 下面两个元素皆在链表头传递
		byte addedNum = -128;// 已加入元素数量.从-128开始计数
		byte noCacheSize = -128;// 当前桶未缓存的数量。因为内存中每个桶限制了大小不可超过127。所以用byte足矣

		LinkedKey next;// 下一个key引用指针，4字节

		public LinkedKey(int... hashs) {
			this.hash0 = hashs[0];
			this.hash1 = hashs[1];
			this.hash2 = hashs[2];
		}

		public boolean equals(int[] hashs) {
			// 为什么从最后个开始比较呢？
			// 搜索时，同个hash位搜索，第一个hash很可能一样，后面两hash有更大几率不一样，有利于判断提早结束
			// 为什么不忽略第一个hash，因为hash有正负值
			return hashs != null && hashs.length == 3 && hash2 == hashs[2] && hash1 == hashs[1] && hash0 == hashs[0];
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof LinkedKey && equals((LinkedKey) o);
		}

		public boolean equals(LinkedKey key) {
			return key == this || this.hash2 == key.hash2 && this.hash1 == key.hash1 && this.hash0 == key.hash0;
		}
	}

	//
	public static class HashKey {
		int bucketIdx;// 桶索引
		byte[] hash;// hash

		public HashKey(int bucketIdx, byte[] hash) {
			this.bucketIdx = bucketIdx;
			this.hash = hash;
		}
	}

	public static void main(String[] args) {

		Conf conf = new Conf();
		conf.setBusKey("test");
		conf.setFileCycle(1200000);// 20分钟
		conf.setCheckRange(2);// 检查范围=3
		conf.setNumLevel(100000000);// 亿级数据。
		// conf.setMemBucketCap((byte) 10);
		if (Config.getDistinctDir().startsWith("/")) {
			conf.setFilePath(Config.getDistinctDir() + "/test/" + Distinct.DISTINCT_CACHE_FILE_NAME);
		} else {
			conf.setFilePath(Config.getCurrentWorkDir() + "/" + Config.getDistinctDir() + "/test/" +
					Distinct.DISTINCT_CACHE_FILE_NAME);
		}
		final FilterCacheHash fch = new FilterCacheHash(conf);
		fch.shutup();

		final Random random = new Random();
		final AtomicInteger x = new AtomicInteger(0);
		final AtomicInteger total = new AtomicInteger(0);
		String str = "测试测试测试adfasdf";
		byte[] b = str.getBytes();
		final byte[] bytes = new byte[b.length + 4];
		System.arraycopy(b, 0, bytes, 0, b.length);
		Thread th = new Thread() {
			public void run() {
				int i = 0;
				while (true) {
					if (i > 0 && i % 1000000 == 0) {
						// 每隔100万条数据，手动制造一个重复
						System.arraycopy(Bytes.toBytes(random.nextInt(1000000)), 0, bytes, bytes.length - 5, 4);
					} else {
						System.arraycopy(Bytes.toBytes(i), 0, bytes, bytes.length - 5, 4);
					}
					if (fch.containsFilter(bytes)) {
						LogUtils.info("重复:" + i);
						x.incrementAndGet();
					}
					total.incrementAndGet();
					i++;
				}
			}
		};
		th.start();

		final short[] npsarr = new short[7200];// 2个小时
		Thread th1 = new Thread() {
			public void run() {
				int ttt = 0;
				int i = 0;
				while (true) {
					Utils.sleep(1000);
					int xtt = total.get();
					npsarr[i] = (short) (xtt - ttt);
					LogUtils.info("\n\n检测:" + xtt + ",已重复:" + x.get() + ",NPS:" + (xtt - ttt) + ",摘要=>\n" +
							fch.getSummaryInfo());
					ttt = xtt;
					i++;
					if (i == 7200) {
						break;
					}
				}
			}
		};
		th1.start();

		Utils.sleep(10000000);
	}
}
