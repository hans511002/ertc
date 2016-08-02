package com.ery.ertc.collect.forwards;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.handler.Distinct;
import com.ery.ertc.collect.handler.Filter;
import com.ery.ertc.collect.log.CollectLog;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.Pair;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.nordb.HDFSDataSource;
import com.ery.base.support.nordb.HbaseDataSource;
import com.ery.base.support.sys.DataSourceManager;
import com.ery.base.support.sys.podo.DataSrcPO;
import com.ery.base.support.utils.Bytes;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class MsgCache {
	public final DaemonMaster daemonMaster;
	final MsgPO msgPO;
	final MsgQueue msgQueue;

	String cacheDS;
	DataSrcPO srcPO;

	ConcurrentLinkedQueue<Object[]> cacheList = new ConcurrentLinkedQueue<Object[]>();// 用来接收外部的数据，并准备持久化到磁盘
	AtomicLong cacheSize = new AtomicLong(0);// 缓存区内存数据大小
	AtomicLong storeSize = new AtomicLong(0);// 缓存区磁盘数据大小
	long MaxSendBuffSize;

	CacheType type = null;
	Thread cacheThread = null;
	String cacheDir;

	LinkedList<File> cacheFiles = new LinkedList<File>();// 缓存文件队列
	File currentWrite = null;// 当前正在写的文件
	long maxFileId = 0;// 文件ID

	final String dbCacheTable;
	long minID = 0;
	long maxID = 0;
	public final String CACHE_DIR;
	public final String CACHE_NAME_PREFIX = "cache.";// 缓存文件前缀，collect.cache.1

	// 计算大致速率
	long addTime = 0;
	int addNum;
	int addNps;// 速率
	FileChannel shmChannel = null;
	MappedByteBuffer shmMapbuff = null;
	long shmSize = 0;
	Map<Long, Pair<String, Long>> shmHeadMap = new HashMap<Long, Pair<String, Long>>();
	final Map<Long, ExtMsgCache> extMsgCaches = new HashMap<Long, ExtMsgCache>();

	class ExtMsgCache {
		File currentRead = null;// 当前正在读的文件
		private Thread fillTh;// 填充线程
		// 计算大致速率
		private long sendTime = 0;
		int sendNum;
		int sendNps;// 速率
		final ConcurrentLinkedQueue<Object[]> sendQueue = new ConcurrentLinkedQueue<Object[]>();// 待发送区，消息生命周期:list->发送。
		final AtomicInteger queueSize = new AtomicInteger(0);// 队列大小
		final MsgExtPO extPo;
		// 填充
		Object fillMutex = new Object();

		boolean running = false;
		Filter filter;
		Distinct distinct;

		ExtMsgCache(MsgExtPO extPo) throws IOException {
			this.extPo = extPo;
			String filterRule = extPo.getFilterRule();
			if (filterRule == null || filterRule.trim().equals("")) {
				filter = null;
			} else {
				filter = new Filter(msgPO, extPo);
			}
			distinct = new Distinct(msgPO, extPo);
			if (!distinct.ready()) {
				distinct = null;
			}
			readSendList();
			if (fillTh == null) {// 从cache中读出数据到内存
				fillTh = new Thread() {
					public void run() {
						while (msgQueue.state == 1) {
							try {
								fillData();
							} catch (Exception e) {
							} finally {
							}
						}
					}
				};
			}

		}

		void fillData() throws Exception {
			synchronized (fillMutex) {
				switch (type) {
				case HDFS:
					break;
				case RDB:
					break;
				case HBASE:
					break;
				default:
					fillFromLocalFile();
					break;
				}
				fillMutex.notifyAll();
				fillMutex.wait();
			}
		}

		private void fillFromLocalFile() {
			try {
				int cacheFilen = 0;
				synchronized (cacheFiles) {
					cacheFilen = cacheFiles.size();
				}
				if (cacheFilen == 0) {
					shmHeadMap.remove(extPo.getSTORE_ID());
					writeShmHeadMap();
					addCacheToExtBuffer();
					if (this.sendQueue.isEmpty()) {
						this.queueSize.set(0);
						Utils.sleep(200);
					}
					return;
				}
				if (queueSize.get() >= MaxSendBuffSize) {
					return;
				}
				Pair<String, Long> fileReadPair = shmHeadMap.get(extPo.getSTORE_ID());
				while (msgQueue.state == 1 && !cacheFiles.isEmpty()) {
					long readPos = 0;
					long readFileId = 0;
					long fileLen = 0;
					long lastModified = 0;
					String fileName = null;
					int readFileIndex = 0;
					synchronized (cacheFiles) {
						if (fileReadPair != null) {
							readPos = fileReadPair.getSecond();
							fileName = fileReadPair.getFirst();
							readFileId = getFileId(fileName);
							fileName = CACHE_DIR + fileName;
							for (int i = 0; i < cacheFiles.size(); i++) {
								if (getFileId(cacheFiles.get(i).getName()) == readFileId) {
									currentRead = cacheFiles.get(i);
									readFileIndex = i;
								}
							}
							if (readPos == currentRead.length()) {// 已读取完
								long oldReadFileId = readFileId;
								long oldReadPos = readPos;
								if ((currentWrite == null || !currentWrite.getName().equals(currentRead.getName()))) {
									currentRead = null;
									if (cacheFiles.size() > readFileIndex + 1) {
										readFileIndex++;
										currentRead = cacheFiles.get(readFileIndex);
										readPos = 0;
										fileName = currentRead.getAbsolutePath();
										fileReadPair.setFirst(getFileName(fileName));
										readFileId = getFileId(fileName);
										fileReadPair.setSecond(readPos);
									}
								}
								// 判断上一个文件是否需要删除
								boolean isAllSameEnd = true;
								long minFileId = 0x7fffffffffffffffl;
								for (Long storeId : extMsgCaches.keySet()) {
									Pair<String, Long> pair = shmHeadMap.get(storeId);
									if (pair != null) {
										long _readFileId = getFileId(pair.getFirst());
										if (minFileId > _readFileId) {
											minFileId = _readFileId;
										}
										if ((oldReadFileId == _readFileId && pair.getSecond() != oldReadPos) ||
												oldReadFileId > _readFileId) {
											isAllSameEnd = false;
										}
									} else {// 有未读取文件缓存的
										isAllSameEnd = false;
										minFileId = 0xefffffffffffffffl;
										break;
									}
								}
								if (isAllSameEnd || minFileId > oldReadFileId) {//
									List<File> fs = new ArrayList<File>(cacheFiles);
									for (File file : fs) {
										long _readFileId = getFileId(getFileName(file.getAbsolutePath()));
										if (_readFileId <= oldReadFileId) {
											if (cacheFiles.remove(file)) {
												boolean delres = file.delete();
												storeSize.addAndGet(-file.length());
											}
										}
									}
								}
							}
						} else {
							currentRead = cacheFiles.getFirst();
							if (currentRead.length() == 0) {// 等待下轮读取
								return;
							}
							fileReadPair = new Pair<String, Long>(getFileName(currentRead.getAbsolutePath()), 0l);
							shmHeadMap.put(extPo.getSTORE_ID(), fileReadPair);
							readFileId = getFileId(fileReadPair.getFirst());
							writeShmHeadMap();
						}
						synchronized (currentRead) {
							fileLen = currentRead.length();
							fileName = currentRead.getName();
							lastModified = currentRead.lastModified();
						}
					}
					BufferedInputStream bis = new BufferedInputStream(new FileInputStream(currentRead), 65536);// 一次缓冲64k
					bis.skip(readPos);
					while (msgQueue.state == 1) {
						if (readPos < fileLen) {
							byte[] row = null;
							row = new byte[4];
							bis.read(row);
							int rowLen = Convert.toInt(row);
							row = new byte[rowLen];
							bis.read(row);
							readPos += row.length + 4;// 已读取增加
							String rowStr = new String(row);
							String tmps[] = rowStr.split("\\^");
							Object[] filds = new Object[tmps.length];
							int i = 0;
							filds[i] = tmps[i];// hostName
							i++;
							filds[i] = Convert.toLong(tmps[i]);// logId
							i++;
							for (MsgFieldPO fieldPO : msgPO.getMsgFields()) {
								filds[i] = QueueUtils.fmtData(fieldPO, tmps[i]);
								i++;
							}
							fileReadPair.setSecond(readPos);
							synchronized (sendQueue) {
								sendQueue.add(filds);
								queueSize.addAndGet(1);
								if (queueSize.get() >= MaxSendBuffSize) {
									break;
								}
							}
						} else {
							Utils.sleep(100);
							if (lastModified != currentRead.lastModified()) {
								fileLen = currentRead.length();// 文件可能发生变化
								lastModified = currentRead.lastModified();
							} else {
								break;
							}
						}
					}
					bis.close();
					writeShmHeadMap();
					// 已读完且未在写
					if (readPos == fileLen &&
							(currentWrite == null || !currentWrite.getName().equals(currentRead.getName()))) {
						// 判断其它的存储是否已发送完成
						synchronized (cacheFiles) {
							if (cacheFiles.size() > readFileIndex + 1) {// 移向下一个文件
								readFileIndex++;
								File nextReadFile = cacheFiles.get(readFileIndex);
								fileReadPair.setFirst(getFileName(nextReadFile.getAbsolutePath()));
								fileReadPair.setSecond(0l);
							}
							boolean isAllSameEnd = true;
							long minFileId = 0x7fffffffffffffffl;

							for (Long storeId : extMsgCaches.keySet()) {
								Pair<String, Long> pair = shmHeadMap.get(storeId);
								if (pair != null) {
									long _readFileId = getFileId(pair.getFirst());
									if (minFileId > _readFileId) {
										minFileId = _readFileId;
									}
									if ((readFileId == _readFileId && pair.getSecond() != readPos) ||
											readFileId > _readFileId) {
										isAllSameEnd = false;
									}
								} else {// 有未读取文件缓存的
									isAllSameEnd = false;
									minFileId = 0xefffffffffffffffl;
									break;
								}
							}
							if (isAllSameEnd || minFileId > readFileId) {//
								List<File> fs = new ArrayList<File>(cacheFiles);
								for (File file : fs) {
									long _readFileId = getFileId(getFileName(file.getAbsolutePath()));
									if (_readFileId <= readFileId) {
										if (cacheFiles.remove(file)) {
											file.delete();
											storeSize.addAndGet(-file.length());
										}
									}
								}
							}
						}
					}
					currentRead = null;
				}
			} catch (Exception e) {
				LogUtils.warn(null, e);
			} finally {
			}
		}

		void calcSendPS(int inc) {
			sendNum += inc;
			long t = System.currentTimeMillis();
			if (t - sendTime >= 5000l) {
				sendTime = t;
				sendNps = sendNum / 5;
				sendNum = 0;
			}
		}

		public Object[] getOne() {
			if (sendQueue.isEmpty()) {
				synchronized (fillMutex) {
					fillMutex.notifyAll();
				}
				Utils.sleep(1000);
			}
			Object[] bs = null;
			while (!sendQueue.isEmpty()) {
				synchronized (sendQueue) {
					bs = sendQueue.poll();
					queueSize.addAndGet(-1);
				}
				calcSendPS(1);
				if (!filterRow(bs)) {
					break;
				}
				if (!distinctRow(bs)) {
					break;
				}
			}
			return bs;
		}

		// 过滤
		boolean filterRow(Object[] bs) {
			if (filter != null && filter.filter(bs)) { // 被过滤
				long logIdSn = Convert.toLong(bs[bs.length - 1]);
				daemonMaster.workerService.notifyFilter(msgPO.getMSG_ID(), this.extPo.getSTORE_ID(), bs);
				CollectLog.filterLog(msgPO, extPo, bs, logIdSn);
				return true;
			}
			return false;
		}

		// 去重
		boolean distinctRow(Object[] tuples) {
			if (distinct != null) {
				if (distinct.distinct(tuples)) { // 重复记录
					daemonMaster.workerService.notifyDistinct(msgPO.getMSG_ID(), this.extPo.getSTORE_ID(), tuples);
					long logIdSn = Convert.toLong(tuples[tuples.length - 1]);
					CollectLog.distinctLog(msgPO, extPo, tuples, logIdSn);
					return true;
				}
			}
			return false;
		}

		protected List<Object[]> getMore(int more) {
			// ExtMsgCache ext = extMsgCaches.get(extPo.getSTORE_ID());
			if (this.sendQueue.isEmpty()) {
				synchronized (fillMutex) {
					fillMutex.notifyAll();
				}
				Utils.sleep(1000);
			}
			int len = 0;
			List<Object[]> ls = new ArrayList<Object[]>();
			while (!this.sendQueue.isEmpty() && len < more) {
				Object[] bs = null;
				synchronized (this.sendQueue) {
					bs = this.sendQueue.poll();
					this.queueSize.addAndGet(-1);
				}
				if (filterRow(bs)) {
					continue;
				}
				if (distinctRow(bs)) {
					continue;
				}
				ls.add(bs);
				len++;
			}
			calcSendPS(ls.size());
			return ls;
		}

		void stop() {
			this.fillTh.stop();
		}

		public void join() {
			try {
				// 停止排重计算
				distinct.shutdown();
				// daemonMaster.queueUtils.shutdownDistinct(msgPO.getMSG_ID());
				if (this.running) {
					this.fillTh.join();
				}
			} catch (InterruptedException e) {
			}
		}

		public void start() {
			this.fillTh.start();
		}

		public void flushSendList() throws IOException {
			switch (type) {
			case HDFS:
				break;
			case RDB:
				break;
			case HBASE:
				break;
			default:
				flushSendListToLocalFile();
				break;
			}
		}

		public void flushSendListToLocalFile() throws IOException {
			File sendList = new File(cacheDir + "sendList_" + this.extPo.getSTORE_ID());
			if (sendList.exists())
				sendList.delete();
			if (sendQueue.isEmpty())
				return;
			FileOutputStream fos = new FileOutputStream(sendList);
			try {
				StringBuffer sb = new StringBuffer(1024);
				for (;;) {
					Object[] kms = sendQueue.poll();
					if (kms == null) {
						break;
					}
					sb.setLength(0);
					for (int i = 0; i < kms.length; i++) {
						if (i > 0)
							sb.append("^");
						sb.append(kms[i]);
					}
					byte[] row = sb.toString().getBytes();
					fos.write(Bytes.toBytes(row.length));// 存一条消息
					fos.write(sb.toString().getBytes());// 存一条消息
					fos.flush();
				}
			} finally {
				fos.close();
			}
		}

		public void readSendList() throws IOException {
			switch (type) {
			case HDFS:
				break;
			case RDB:
				break;
			case HBASE:
				break;
			default:
				readSendListFromLocalFile();
				break;
			}
		}

		private void readSendListFromLocalFile() throws IOException {
			File sendList = new File(cacheDir + "sendList_" + this.extPo.getSTORE_ID());
			if (!sendList.exists())
				return;
			long fileLen = sendList.length();
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(sendList), 65536);// 一次缓冲64k
			int readLen = 0;
			try {
				while (msgQueue.state == 1 && readLen < fileLen) {
					byte[] key = new byte[4];
					int len = Bytes.toInt(key);
					bis.read(key);
					byte[] val = new byte[len];
					int rl = bis.read(val);
					readLen += 4 + rl;
					if (rl != len) {
						// warn
						break;
					}
					String strVal = new String(val);
					String tmps[] = strVal.split("\\^");
					Object[] filds = new Object[tmps.length];
					int i = 0;
					filds[i] = tmps[i];// hostName
					i++;
					filds[i] = Convert.toLong(tmps[i]);// logId
					i++;
					for (MsgFieldPO fieldPO : msgPO.getMsgFields()) {
						filds[i] = QueueUtils.fmtData(fieldPO, tmps[i]);
						i++;
					}
					synchronized (sendQueue) {
						sendQueue.add(filds);
						queueSize.addAndGet(1);
					}
					daemonMaster.workerService.notifyLeaveNoSend(msgPO.getMSG_ID(), extPo.getSTORE_ID(),
							strVal.getBytes().length);
				}
				sendList.delete();
			} finally {
				bis.close();
			}
		}
	}

	public Object[] getOne(MsgExtPO extPo) {
		ExtMsgCache ext = extMsgCaches.get(extPo.getSTORE_ID());
		return ext.getOne();
	}

	protected List<Object[]> getMore(MsgExtPO extPo, int more) {
		ExtMsgCache ext = extMsgCaches.get(extPo.getSTORE_ID());
		return ext.getMore(more);
	}

	MsgCache(final MsgQueue msgQueue, final MsgPO msgPO) {
		this.msgQueue = msgQueue;
		this.msgPO = msgPO;
		this.daemonMaster = msgQueue.daemonMaster;
		dbCacheTable = "TMP_COLLECT_CACHE_" + msgPO.getMSG_ID();
		CACHE_DIR = Config.getCacheDir();
		this.cacheDS = Convert.toString(msgPO.getCACHE_DS());
		for (MsgExtPO extPo : msgPO.getExtPO()) {
			if (MaxSendBuffSize < msgPO.getTODO_SEND_QUEUE_SIZE() * extPo.getSEND_THD_POOL_SIZE())
				MaxSendBuffSize = msgPO.getTODO_SEND_QUEUE_SIZE() * extPo.getSEND_THD_POOL_SIZE();
		}
		try {
			init();
			for (MsgExtPO extPo : msgPO.getExtPO()) {
				extMsgCaches.put(extPo.getSTORE_ID(), new ExtMsgCache(extPo));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// 从内存中写入数据到文件
		cacheThread = new Thread() {
			@Override
			public void run() {
				while (msgQueue.state == 1) {
					try {
						synchronized (flushMutex) {
							flushMutex.notifyAll();
							flushMutex.wait();
							flashCache(msgQueue.state != 1);
						}
					} catch (Exception e) {
						LogUtils.error("消息[" + msgPO.getMSG_TAG_NAME() + "]缓存出错!", e);
					}
				}
			}
		};

	}

	void shmMap(long shmSize) throws IOException {
		if (shmChannel != null)
			shmChannel.close();
		shmChannel = null;
		File shmHead = new File(CACHE_DIR + "/shm_head_" + msgPO.getMSG_ID());
		boolean shmExists = shmHead.exists();
		if (!shmExists) {// 初始化控制文件大小
			shmHead.createNewFile();
			RandomAccessFile raf = new RandomAccessFile(shmHead, "rw");
			raf.seek(shmSize - 1);
			raf.write(0);
			raf.close();
		} else {
			if (shmHead.length() < shmSize) {
				RandomAccessFile raf = new RandomAccessFile(shmHead, "rw");
				raf.seek(shmSize - 1);
				raf.write(0);
				raf.close();
			}
		}
		RandomAccessFile raf = new RandomAccessFile(shmHead, "rw");
		shmChannel = raf.getChannel();
		shmMapbuff = shmChannel.map(FileChannel.MapMode.READ_WRITE, 0, shmSize);
	}

	void writeShmHeadMap() {
		if (shmMapbuff == null)
			return;
		synchronized (shmMapbuff) {
			shmMapbuff.clear();
			for (Long storeId : shmHeadMap.keySet()) {
				Pair<String, Long> filePair = shmHeadMap.get(storeId);
				shmMapbuff.putLong(storeId);
				byte[] bt = filePair.getFirst().getBytes();
				shmMapbuff.put(bt);
				for (int i = 0; i < 32 - bt.length; i++) {
					shmMapbuff.put((byte) 32);// 空格
				}
				shmMapbuff.putLong(filePair.getSecond());
			}
		}
	}

	void readShmHeadMap() {
		if (shmMapbuff == null)
			return;
		synchronized (shmMapbuff) {
			shmMapbuff.flip();
			while (shmMapbuff.remaining() > 0) {
				long storeId = shmMapbuff.getLong();
				if (storeId <= 0 || storeId > 0xeffffffffffffffl) {
					break;
				}
				byte[] dst = new byte[32];
				shmMapbuff.get(dst);
				String fileName = new String(dst).trim();
				long pos = shmMapbuff.getLong();
				this.shmHeadMap.put(storeId, new Pair<String, Long>(fileName, pos));
			}
		}
		List<Long> stids = new ArrayList<Long>(shmHeadMap.keySet());
		Map<Long, MsgExtPO> storeIdRel = new HashMap<Long, MsgExtPO>();
		for (MsgExtPO extPo : this.msgPO.getExtPO()) {
			storeIdRel.put(extPo.getSTORE_ID(), extPo);
		}
		int osi = shmHeadMap.size();
		for (Long storeId : stids) {
			if (!storeIdRel.containsKey(storeId))
				shmHeadMap.remove(storeId);
		}
		if (osi == shmHeadMap.size()) {
			writeShmHeadMap();
		}
	}

	public long getFileId(String fileName) {
		return Convert.toLong(getFileName(fileName).substring(CACHE_NAME_PREFIX.length()));
	}

	public String getFileName(String fileFullName) {
		return fileFullName.substring(fileFullName.lastIndexOf(File.separator) + 1);
	}

	void init() throws Exception {
		Map<Long, Long> leaveNoSend = new HashMap<Long, Long>();
		for (MsgExtPO extPo : msgPO.getExtPO()) {
			leaveNoSend.put(extPo.getSTORE_ID(), 0l);
			if (MaxSendBuffSize < msgPO.getTODO_SEND_QUEUE_SIZE() * extPo.getSEND_THD_POOL_SIZE())
				MaxSendBuffSize = msgPO.getTODO_SEND_QUEUE_SIZE() * extPo.getSEND_THD_POOL_SIZE();
		}
		if (cacheDS == null || cacheDS.equals("") || Convert.toInt(cacheDS, 0) <= 0) {// 本地缓存
			String _cacheDir = Config.getCacheDir();
			if (_cacheDir.startsWith("/")) {
				cacheDir = _cacheDir + "/" + msgPO.getMSG_TAG_NAME() + "/";
			} else {
				cacheDir = Config.getCurrentWorkDir() + "/" + _cacheDir + "/" + msgPO.getMSG_TAG_NAME() + "/";
			}
			// 本地
			File f = new File(cacheDir);
			if (!f.exists()) {
				f.mkdirs();
			}
			File[] fs = f.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.startsWith(CACHE_NAME_PREFIX);
				}
			});
			for (File f_ : fs) {
				cacheFiles.add(f_);
				storeSize.addAndGet(f_.length());
			}
			if (cacheFiles.size() > 0) {
				Collections.sort(cacheFiles, new Comparator<File>() {
					@Override
					public int compare(File o1, File o2) {
						int a1 = Convert.toInt(o1.getName().substring(CACHE_NAME_PREFIX.length()));
						int a2 = Convert.toInt(o2.getName().substring(CACHE_NAME_PREFIX.length()));
						return a1 - a2;
					}
				});
				maxFileId = getFileId(cacheFiles.getLast().getName());
			}
			type = CacheType.LOCAL;
			int once = 8 + 32 + 8;// storeId+filename+pos
			shmSize = msgPO.getExtPO().size() * once;
			shmMap(shmSize);
			// 初始化控制头信息
			readShmHeadMap();

			List<Long> stids = new ArrayList<Long>(shmHeadMap.keySet());
			for (Long storeId : stids) {
				long read = shmHeadMap.get(storeId).getSecond();
				String fileName = shmHeadMap.get(storeId).getFirst();
				long crid = getFileId(fileName);
				for (File file : cacheFiles) {
					long fid = getFileId(file.getAbsolutePath());
					if (fid < crid) {
						leaveNoSend.put(storeId, leaveNoSend.get(storeId) + file.length());
					} else if (fid == crid) {
						leaveNoSend.put(storeId, leaveNoSend.get(storeId) + read);
					} else {
						break;
					}
				}
			}
		} else {
			// 缓存到某个数据源（可能是rdb，可能是HDFS）
			Object dsobj = DataSourceManager.getAllDataSource().get(cacheDS);
			if (dsobj == null) {
				throw new RuntimeException("缓存队列初始出错，无法识别缓存源[" + cacheDS + "]!");
			}
			srcPO = DataSourceManager.getSrcPO(cacheDS);
			if (dsobj instanceof DataSource) {
				// 缓存至关系型数据库
				String sql = null;
				CacheDAO dao = new CacheDAO();
				if (srcPO.getDATA_SOURCE_TYPE() == DataSrcPO.DS_TYPE_MYSQL) {
					sql = "CREATE TABLE " + dbCacheTable + "(" + " ID BIGINT NOT NULL," + " MSG BLOB," + " SIZE INT," +
							" PRIMARY KEY (ID)" + ")";
				} else if (srcPO.getDATA_SOURCE_TYPE() == DataSrcPO.DS_TYPE_ORACLE) {
					sql = "CREATE TABLE " + dbCacheTable + "(" + " ID NUMBER(18) NOT NULL," + " MSG BLOB," +
							" SIZE NUMBER(9)," + " PRIMARY KEY (ID)" + ")";
				}
				if (sql != null) {
					dao.tryCreateCacheTbl(cacheDS, sql);
					// 读取最小,最大记录
					sql = "SELECT MIN(ID),MAX(ID),SUM(SIZE) FROM " + dbCacheTable;
					Long[] mm = dao.getMinAndMaxCacheItem(cacheDS, sql);
					if (mm != null) {
						minID = mm[0];
						maxID = mm[1];
						storeSize.set(mm[2]);
					}
					dao.close();
					type = CacheType.RDB;
				}
			} else if (dsobj instanceof HDFSDataSource) {
				// 缓存至hdfs
				cacheDir = "/" + Config.getCacheDir() + "/" + msgPO.getMSG_TAG_NAME() + "/" + Config.getHostName() +
						"/";
				// 读取hdfs上缓存内容
				type = CacheType.HDFS;
			} else if (dsobj instanceof HbaseDataSource) {
				type = CacheType.HBASE;
			}

		}

		if (storeSize.get() > 0) {
			for (MsgExtPO extPo : this.msgPO.getExtPO()) {
				daemonMaster.workerService.notifyLeaveNoSend(msgPO.getMSG_ID(), extPo.getSTORE_ID(), storeSize.get() -
						leaveNoSend.get(extPo.getSTORE_ID()));
			}
			if (type == CacheType.RDB) {
				LogUtils.info("初始消息[" + msgPO.getMSG_TAG_NAME() + "]缓存,遗留表记录[" + minID + "-" + maxID + "],共[" +
						storeSize.get() + "] b未发送!");
			} else {
				LogUtils.info("初始消息[" + msgPO.getMSG_TAG_NAME() + "]缓存,剩余队列文件[" + cacheFiles.size() + "]个,共[" +
						storeSize.get() + "] b未发送!");
			}
		} else {
			LogUtils.info("初始消息[" + msgPO.getMSG_TAG_NAME() + "]缓存,无遗留消息!");
		}
	}

	public void addCacheToExtBuffer() { // 无文件
		if (!this.cacheFiles.isEmpty())
			return;
		synchronized (cacheList) {
			if (!this.cacheFiles.isEmpty())
				return;
			for (Long storeId : this.extMsgCaches.keySet()) {
				ExtMsgCache ext = extMsgCaches.get(storeId);
				synchronized (ext.sendQueue) {
					while (cacheList.size() > 0) {
						Object[] b = cacheList.poll();
						if (b == null) {
							continue;
						}
						ext.sendQueue.add(b);
						ext.queueSize.addAndGet(1);
					}
				}
				cacheSize.set(0);
			}
		}

	}

	// 直接加入缓存队列
	void add(List<Object> keyOrContent) {
		if (this.cacheFiles.isEmpty()) {// 无文件
			addCacheToExtBuffer();
			synchronized (cacheList) {
				for (Long storeId : this.extMsgCaches.keySet()) {
					ExtMsgCache ext = extMsgCaches.get(storeId);
					synchronized (ext.sendQueue) {
						ext.sendQueue.add(keyOrContent.toArray());
					}
					cacheSize.set(0);
				}
			}
		} else {
			long s = 0;
			synchronized (cacheList) {
				cacheList.add(keyOrContent.toArray());
				s = cacheSize.addAndGet(1);
				calcAddPS(1);
			}
			if (s >= Config.getCacheMaxSize()) {
				synchronized (flushMutex) {
					flushMutex.notifyAll();// 启动刷新到磁盘,此处会阻塞
				}
				if (s >= Config.getCacheMaxSize() * 2) {
					Utils.sleep((long) (((float) s / Config.getCacheMaxSize() - 1) * 1000));
				}
			}
		}
	}

	void calcAddPS(int inc) {
		addNum += inc;
		long t = System.currentTimeMillis();
		if (t - addTime >= 5000l) {
			addTime = t;
			addNps = addNum / 5;
			addNum = 0;
		}
	}

	protected void sendFail(MsgExtPO extPo, Object[] kms) {
		ExtMsgCache ext = extMsgCaches.get(extPo.getSTORE_ID());
		synchronized (ext.sendQueue) {
			ext.sendQueue.add(kms);
			ext.queueSize.addAndGet(1);
		}
	}

	protected void sendFail(MsgExtPO extPo, List<Object[]> sendList) {
		ExtMsgCache ext = extMsgCaches.get(extPo.getSTORE_ID());
		synchronized (ext.sendQueue) {
			ext.sendQueue.addAll(sendList);
			ext.queueSize.addAndGet(sendList.size());
		}
	}

	// 创建一个文件并加入到文件队列
	private void createCacheFile() throws IOException {
		maxFileId++;
		File f = new File(cacheDir + CACHE_NAME_PREFIX + maxFileId);
		f.createNewFile();
		cacheFiles.add(f);
	}

	// 刷新缓存到磁盘，参数表示是否强制。（因为state的关系，需要fouce来避免冲突）
	// 此方法目前是阻塞的。用AtomicBoolean实现非阻塞
	AtomicBoolean flushMutex = new AtomicBoolean(false);// 是否正在刷磁盘

	// boolean fillTriWaitFlush = false;// 填充触发刷磁盘挂起

	// 缓存类型
	enum CacheType {
		LOCAL, HDFS, RDB, HBASE
	}

	void flashCache(boolean force) {
		switch (type) {
		case HDFS:

			break;
		case RDB:

			break;
		case HBASE:

			break;
		default:
			flashToLocalFile(force);
			break;
		}
	}

	private void flashToLocalFile(boolean force) {
		if (!force && cacheSize.get() < Config.getCacheMaxSize() / 2) {
			return;
		}
		FileOutputStream fos = null;
		try {
			if (cacheList.isEmpty())
				return;
			synchronized (cacheFiles) {
				if (cacheFiles.isEmpty()) {
					createCacheFile();// 生成一个新文件
				}
				currentWrite = cacheFiles.getLast();
				// 3倍，生成新文件，不让文件过大
				if (currentWrite.length() > Config.getCacheMaxSize() * 3) {
					try {
						createCacheFile();
					} catch (IOException e) {
						LogUtils.warn(null, e);
					}
					currentWrite = cacheFiles.getLast();
				}
			}
			// 刷新到磁盘
			fos = new FileOutputStream(currentWrite, true);// 向文件追加
			long fsLen = currentWrite.length();
			StringBuffer sb = new StringBuffer(1024);
			for (; this.msgQueue.state == 1 || force;) {
				Object[] kms = cacheList.poll();
				if (kms == null) {
					break;
				}
				sb.setLength(0);
				for (int i = 0; i < kms.length; i++) {
					if (i > 0)
						sb.append("^");
					sb.append(kms[i]);
				}
				byte[] row = sb.toString().getBytes();
				fos.write(Bytes.toBytes(row.length));// 存一条消息
				fos.write(sb.toString().getBytes());// 存一条消息
				fos.flush();
				storeSize.addAndGet(row.length + 4);
				cacheSize.addAndGet(-1);
				fsLen += row.length + 4;
				if (fsLen >= Config.getCacheMaxSize() * 3) {
					fos.close();
					synchronized (cacheFiles) {
						createCacheFile();// 生成一个新文件
						currentWrite = cacheFiles.getLast();
						fos = new FileOutputStream(currentWrite, true);// 向文件追加
						fsLen = currentWrite.length();
					}
				}
			}
		} catch (Exception e) {
			LogUtils.error("刷新缓存到磁盘出错!", e);
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					LogUtils.error(null, e);
				}
			}
			currentWrite = null;
		}
	}

	// 停止时刷新待发送列表数据到磁盘
	private void flushSendList() {
		for (Long storeId : extMsgCaches.keySet()) {
			try {
				extMsgCaches.get(storeId).flushSendList();
			} catch (IOException e) {
				LogUtils.error("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "]停止，刷出待发送数据!", e);
			}
		}
	}

	void start() {
		for (Long storeId : this.extMsgCaches.keySet()) {
			this.extMsgCaches.get(storeId).start();
		}

		LogUtils.debug("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "]填充线程启动!");
		this.cacheThread.start();
		LogUtils.debug("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "]缓存线程启动!");
	}

	void shutdown() {
		msgQueue.state = -1;
		for (Long storeId : extMsgCaches.keySet()) {
			ExtMsgCache extCache = extMsgCaches.get(storeId);
			synchronized (extCache.fillMutex) {
				extCache.fillMutex.notifyAll();
			}
		}
		synchronized (flushMutex) {
			flushMutex.notifyAll();
		}
		for (Long storeId : extMsgCaches.keySet()) {
			ExtMsgCache ext = extMsgCaches.get(storeId);
			if (ext.running) {
				ext.join();
			}
		}
		try {
			cacheThread.join();
		} catch (InterruptedException e1) {
		}
		flushSendList();
		LogUtils.info("消息[" + msgPO.getMSG_TAG_NAME() + "]缓存待发送区域数据!");
		try {
			writeShmHeadMap();
		} catch (Exception e) {
		} finally {
			try {
				shmChannel.close();
			} catch (IOException e) {
			}
		}
		shmChannel = null;

	}

}
