package com.ery.ertc.collect.forwards;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.CollectConstant;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.handler.Distinct;
import com.ery.ertc.collect.handler.Filter;
import com.ery.ertc.collect.log.CollectLog;
import com.ery.ertc.collect.watch.CollectMonitor;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Utils;

public class MsgQueue {

	public static final int DEFAULT_SEND_QUEUE_SIZE = 10000;// 发送队列最多存放10000条消息，超过后将缓存
	public static final int DEFAULT_SEND_POOL_SIZE = 3;// 默认发送线程池大小
	public static final int DEFAULT_CACHE_MAX_SIZE = 10000;//
	public static final int DEFAULT_CACHE_FILE_SIZE = 10485760;// 10M 达到10M就刷磁盘
	public static final int DEFAULT_CACHE_INTERVAL_MS = 5000;// 5秒刷一次磁盘
	public static final String CACHE_DIR = "../collect_cache";

	// 线程安全队列
	private MsgCache cache;// 缓存区，消息放入此处后，会经过 cachelist->磁盘->待发送区->发送

	private ExecutorService sendPool;
	// storeId,runable
	private Map<Long, List<Runnable>> sendRunnable = new HashMap<Long, List<Runnable>>();
	private final Map<Long, Filter> filterMap = new HashMap<Long, Filter>();// 所有过滤器
	private final Map<Long, Distinct> distinctMap = new HashMap<Long, Distinct>();// 所有去重器

	protected int state;// 状态0停止，1运行中，<0待停止
	int threadPoolSize = 0;

	private MsgPO msgPO;
	public DaemonMaster daemonMaster;

	public MsgQueue(DaemonMaster daemonMaster, MsgPO msgPO) {
		this.msgPO = msgPO;
		this.daemonMaster = daemonMaster;
		state = 0;
		initThreads();// 初始各种线程逻辑
	}

	public MsgPO getMsgPO() {
		return msgPO;
	}

	// 初始各种线程逻辑// 需要为第个外部存储初始各自的线程
	private void initThreads() {
		if (cache == null) {// 存储内容格式按字节存，每条消息之前存4字节标示消息长度，再存消息内容
			cache = new MsgCache(this, msgPO);
		}
		threadPoolSize = 0;
		for (MsgExtPO extPo : msgPO.getExtPO()) {
			List<Runnable> extRuns = new ArrayList<Runnable>();
			sendRunnable.put(extPo.getSTORE_ID(), extRuns);
			for (int i = 0; i < extPo.getSEND_THD_POOL_SIZE(); i++) {
				extRuns.add(new MsgSendRunnable(daemonMaster, msgPO, extPo, cache));
			}
			threadPoolSize += extPo.getSEND_THD_POOL_SIZE();
		}
		if (sendPool == null) {
			sendPool = Executors.newFixedThreadPool(threadPoolSize,
					new DefaultThreadFactory("MsgSendPool_" + msgPO.getMSG_ID(), true));
		}

		// // 初始过滤规则
		// if (msgPO.getExtPO().getFilterRule() != null) {
		// filterMap.put(msgPO.getMSG_ID(), new Filter(msgPO));
		// } else {
		// filterMap.remove(msgPO.getMSG_ID());
		// }
		//
		// // 初始去重规则
		// if (msgPO.getExtPO().getDistinctRule() != null) {
		// distinctMap.put(msgPO.getMSG_ID(), new Distinct(msgPO));
		// } else {
		// Distinct distinct = distinctMap.remove(msgPO.getMSG_ID());
		// if (distinct != null) {
		// distinct.shutdown();
		// }
		// }

	}

	public boolean isStarted() {
		return state == 1;
	}

	public int getState() {
		return state;
	}

	// 内存缓存大小
	public long getCacheMemSize() {
		return cache.cacheSize.get();
	}

	/**
	 * 向队列追加一条消息，此处的效率影响采集消息的吞吐
	 * 
	 * @param keyOrContent
	 *            arr0 key arr1 消息内容 arr2 日志序列号
	 */
	public void put(List<Object> tuples) {
		Long logIdSn = CollectMonitor.getNewLogId(msgPO.getMSG_ID());
		// List<Object> tup = new ArrayList<Object>(Arrays.asList(tuples));
		tuples.set(0, Config.getHostName());// 序列号不唯一，必需与主机名组合
		tuples.set(1, logIdSn);
		cache.add(tuples);
		CollectLog.collectLog(msgPO, tuples, logIdSn);
		// byte[][] kv = QueueUtils.getKeyOrContentForTuples(msgPO, tuples);
	}

	public synchronized void start(QueueStartOrStopCall call) {
		if (state == 1)
			return;
		state = 1;
		for (MsgExtPO extPo : msgPO.getExtPO()) {
			List<Runnable> extRuns = sendRunnable.get(extPo.getSTORE_ID());
			for (Runnable runnable : extRuns) {
				sendPool.submit(runnable);
			}
		}
		LogUtils.debug("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "]发送线程池(" + threadPoolSize + ")启动!");
		cache.start();
		if (call != null) {
			call.call(msgPO, state);
		}
	}

	public synchronized StopThread stop(final QueueStartOrStopCall call) {
		if (state != 1) {
			this.daemonMaster.workerService.monitor
					.setRunStatus(this.msgPO, "stoped:" + CollectConstant.getNowString());
			return null;
		}
		// 停止队列需要时间，异步实现，不可阻塞
		StopThread stop = new StopThread(call);
		stop.start();
		return stop;
	}

	public class StopThread extends Thread {
		final QueueStartOrStopCall call;

		public StopThread(QueueStartOrStopCall call) {
			this.call = call;
		}

		@Override
		public void run() {
			try {
				state = -1;// 待停止
				daemonMaster.workerService.monitor.setRunStatus(msgPO, "stoping");
				LogUtils.info("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "]发送队列准备停止!");
				// 停顿2秒。足以让所有运行线程内的循环OK，并且保证所有状态不会是中间状态
				Utils.sleep(2000);
				LogUtils.debug("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "]填充线程停止!");
				sendPool.shutdown();
				LogUtils.debug("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "]发送线程池停止!");
				cache.shutdown();
				LogUtils.debug("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "]缓冲线程停止!");
			} catch (Exception e) {
				LogUtils.error("消息[" + msgPO.getMSG_ID() + "," + msgPO.getMSG_TAG_NAME() + "] 停止异常!", e);
			} finally {
				state = 0;
				daemonMaster.workerService.monitor.setRunStatus(msgPO, "stoped");
				if (call != null) {
					call.call(msgPO, state);
				}
			}
		}
	}

	/**
	 * 队列启动和停止回调，有些事有时必须在队列完全启动或停止后才能做，比如： 启动完成之后，开始采集
	 * 停止完成之后，QueueUtils.bufferQueue 中缓存的队列对象 重启过程，停止采集->停止队列->启动队列->启动采集
	 */
	public interface QueueStartOrStopCall {
		void call(MsgPO msgpo, int state);// 1启动，0停止
	}

}
