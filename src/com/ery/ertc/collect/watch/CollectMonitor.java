package com.ery.ertc.collect.watch;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.ZkClient;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.task.dao.CollectDAO;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.ertc.estorm.po.CollectErrorLogPO;
import com.ery.ertc.estorm.po.CollectLogPO;
import com.ery.ertc.estorm.po.CollectSendErrorLogPO;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.StringUtils;
import com.ery.base.support.utils.Utils;

public class CollectMonitor {

	private static String hostName = Config.getHostName();// 本机hostName
	private static Map<Long, MonitorAttr> monitorAttMap = new HashMap<Long, MonitorAttr>();// 实时监控
	private static Map<Long, Boolean> currentStartd = new HashMap<Long, Boolean>();// 启动状态

	/**
	 * 系统不可能每条日志都记录，数据量太大了 因此以时间粒度进行简单汇总后再存储，比如每5秒存储一次。标示最近5秒总共采集数 达到分析波动的目的
	 */
	private static int writeIntervalMs = Config.getDetailLogWriteIntervalMs();// 采集明细日志间隔，每隔这么久会生成一条记录,此值最好不小于5秒
	private static Map<Long, AtomicBoolean> detail1_isAva = new HashMap<Long, AtomicBoolean>();// 为false时，日志存2，为true时，存1
	private static Map<Long, CollectLogPO> detailLogMap1 = new HashMap<Long, CollectLogPO>();// 采集明细
	private static Map<Long, CollectLogPO> detailLogMap2 = new HashMap<Long, CollectLogPO>();// 采集明细

	private static int errorWriteIntervalMs = Config.getErrorLogWriteIntervalMs();
	private static AtomicBoolean error1_isAva = new AtomicBoolean(true);// 为false时，日志存列表2，为true时，存列表1
	private static List<CollectErrorLogPO> collectErrorLogs1 = new ArrayList<CollectErrorLogPO>();// 采集异常日志
	private static List<CollectErrorLogPO> collectErrorLogs2 = new ArrayList<CollectErrorLogPO>();// 采集异常日志

	private static int sendWriteIntervalMs = Config.getSendLogWriteIntervalMs();
	private static AtomicBoolean send1_isAva = new AtomicBoolean(true);// 为false时，日志存列表2，为true时，存列表1
	private static List<CollectSendErrorLogPO> collectKafkaLogs1 = new ArrayList<CollectSendErrorLogPO>();// 发送异常日志
	private static List<CollectSendErrorLogPO> collectKafkaLogs2 = new ArrayList<CollectSendErrorLogPO>();// 发送异常日志

	private static int totalWriteIntervalMs = Config.getTotalLogWriteIntervalMs();// 统计信息写入频率

	public static long getNewLogId(long msgId) {
		MonitorAttr att = monitorAttMap.get(msgId);
		if (att != null) {
			return att.getNewLogId();
		}
		return 0;
	}

	// 存储线程，存数据库，存zk等
	private Thread detailWriteTh;
	// 异常写入线程
	private Thread errorWriteTh;
	// 发送信息写入线程
	private Thread sendWriteTh;
	// 统计信息写入线程
	private Thread totalWriteTh;
	private ZkClient zkClient;
	private Thread recordZkTh;

	public CollectMonitor(ZkClient zkClient) {
		this.zkClient = zkClient;
		initThreads();
		detailWriteTh.start();
		errorWriteTh.start();
		sendWriteTh.start();
		totalWriteTh.start();
		recordZkTh.start();
	}

	private void initThreads() {
		// 初始明细写入线程
		detailWriteTh = new Thread() {
			@Override
			public void run() {
				Map<Long, Long> msgLogTime = new HashMap<Long, Long>();
				while (true) {
					Utils.sleep(3000);
					try {
						long now = System.currentTimeMillis();
						Long[] msgIds = new Long[monitorAttMap.size()];
						monitorAttMap.keySet().toArray(msgIds);
						for (long msgId : msgIds) {
							try {
								CollectLogPO logPO = null;
								if (detail1_isAva.get(msgId) == null) {
									detail1_isAva.put(msgId, new AtomicBoolean(true));// 先使用1
								}
								if (!msgLogTime.containsKey(msgId)) {
									msgLogTime.put(msgId, now);
								}
								if (detail1_isAva.get(msgId).get()) {
									logPO = detailLogMap1.get(msgId);

								} else {
									logPO = detailLogMap2.get(msgId);

								}
								if ((logPO != null && logPO.getMSG_NUM().get() > 1000) ||
										(now - msgLogTime.get(msgId) > writeIntervalMs && logPO != null && logPO
												.getMSG_NUM().get() > 0)) {
									if (detail1_isAva.get(msgId).get()) {
										detailLogMap2.put(msgId, new CollectLogPO(msgId, hostName));
										detail1_isAva.get(msgId).set(false);
									} else {
										detailLogMap1.put(msgId, new CollectLogPO(msgId, hostName));
										detail1_isAva.get(msgId).set(true);
									}
									// 有记录才存储
									CollectDAO dao = new CollectDAO();
									dao.writeCollectLog(logPO);
									dao.close();
								}
							} catch (Exception e) {
								LogUtils.error("消息[" + msgId + "]细粒度统计存储异常!", e);
							}
						}
					} catch (Exception e) {
						LogUtils.error(null, e);
					}
				}
			}
		};

		// 初始采集异常写入线程
		errorWriteTh = new Thread() {
			@Override
			public void run() {
				while (true) {
					Utils.sleep(errorWriteIntervalMs);
					try {
						CollectDAO dao = new CollectDAO();
						if (error1_isAva.get()) {
							error1_isAva.compareAndSet(true, false);
							dao.writeCollectErrorLogs(collectErrorLogs1);
							collectErrorLogs1.clear();
						} else {
							error1_isAva.compareAndSet(false, true);
							dao.writeCollectErrorLogs(collectErrorLogs2);
							collectErrorLogs2.clear();
						}
						dao.close();
					} catch (Exception e) {
						LogUtils.error("采集异常信息存储异常!", e);
					}
				}
			}
		};

		// 初始发送异常写入线程
		sendWriteTh = new Thread() {
			@Override
			public void run() {
				while (true) {
					Utils.sleep(sendWriteIntervalMs);
					try {
						CollectDAO dao = new CollectDAO();
						if (send1_isAva.get()) {
							send1_isAva.compareAndSet(true, false);
							dao.writeCollectSendLogs(collectKafkaLogs1);
							collectKafkaLogs1.clear();
						} else {
							send1_isAva.compareAndSet(false, true);
							dao.writeCollectSendLogs(collectKafkaLogs2);
							collectKafkaLogs2.clear();
						}
						dao.close();
					} catch (Exception e) {
						LogUtils.error("发送异常信息存储异常!", e);
					}
				}
			}
		};

		// 初始统计信息写入线程
		totalWriteTh = new Thread() {
			@Override
			public void run() {
				while (true) {
					Utils.sleep(totalWriteIntervalMs);
					try {

					} catch (Exception e) {
						LogUtils.error("统计信息存储异常!", e);
					}
				}
			}
		};

		// 初始实时zk写入线程
		recordZkTh = new Thread() {
			@Override
			public void run() {
				while (true) {
					Utils.sleep((long) ((double) MonitorAttr.SNAP_NUM / 3 * 1000));
					wrtieMonitorRecordToZK();
				}
			}
		};
	}

	void wrtieMonitorRecordToZK() {
		try {
			Long[] msgIds = new Long[monitorAttMap.size()];
			monitorAttMap.keySet().toArray(msgIds);
			for (long msgId : msgIds) {
				MonitorAttr matt = monitorAttMap.get(msgId);
				if (matt != null) {
					try {
						String path = ZkConstant.BASE_NODE + "/" + ZkConstant.TASKINFO_NODE + "/" +
								Config.getHostName();
						if (!zkClient.exists(path)) {
							zkClient.createPersistent(path);
						}
						path = path + "/" + msgId;
						if (zkClient.exists(path)) {
							zkClient.writeData(path, matt.toJSONString());
						} else {
							zkClient.createPersistent(path, matt.toJSONString());
						}
					} catch (Exception e) {
						LogUtils.error("消息[" + msgId + "]实时写入ZK异常!", e);
					}
				}
			}
		} catch (Exception e) {
			LogUtils.error(null, e);
		}
	}

	public void stopMonitor() {
		detailWriteTh.stop();
		errorWriteTh.stop();
		sendWriteTh.stop();
		totalWriteTh.stop();
		recordZkTh.stop();
	}

	public void setRunStatus(MsgPO msgPo, String runStatus) {
		MonitorAttr att = null;
		long msgId = msgPo.getMSG_ID();
		synchronized (monitorAttMap) {
			att = monitorAttMap.get(msgId);
			if (att == null) {
				att = new MonitorAttr(msgPo);
				monitorAttMap.put(msgId, att);
			}
		}
		for (MsgExtPO extPO : msgPo.getExtPO()) {
			att.setRunStatus(extPO.getSTORE_ID(), runStatus);
		}
	}

	public void setRunStatus(long msgId, String runStatus) {
		MonitorAttr att = monitorAttMap.get(msgId);
		for (Long extId : att.runStatus.keySet()) {
			att.setRunStatus(extId, runStatus);
		}
	}

	public void setRunStatus(long msgId, long extId, String runStatus) {
		MonitorAttr att = monitorAttMap.get(msgId);
		att.setRunStatus(extId, runStatus);
	}

	public void startCollect(MsgPO msgPo) {
		try {
			long msgId = msgPo.getMSG_ID();
			MonitorAttr att = null;
			synchronized (monitorAttMap) {
				att = monitorAttMap.get(msgId);
				if (att == null) {
					att = new MonitorAttr(msgPo);
					monitorAttMap.put(msgId, att);
				}
			}

			currentStartd.put(msgId, true);
			String path = ZkConstant.BASE_NODE + "/" + ZkConstant.TASKINFO_NODE + "/" + Config.getHostName();
			if (!zkClient.exists(path)) {
				zkClient.createPersistent(path);
			}
			path = path + "/" + msgId;
			List<List<Object>> list = new ArrayList<List<Object>>();
			if (zkClient.exists(path)) {
				list = JSON.parseObject(zkClient.readData(path).toString(), list.getClass());
			}
			if (list.size() > 0) {
				List<Object> o = list.get(list.size() - 1);
				att.initMaxLogId(Convert.toLong(o.get(o.size() - 3)));// 倒数第三个标示日志序号
			}
			// 初始
			detail1_isAva.put(msgId, new AtomicBoolean(true));
			detailLogMap1.put(msgId, new CollectLogPO(msgId, hostName));// 将第一个列表初始
		} catch (Exception e) {
			LogUtils.error("通知启动采集[" + this.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void stopCollect(long msgId) {
		MonitorAttr matt = null;
		currentStartd.put(msgId, false);
		synchronized (monitorAttMap) {
			matt = monitorAttMap.remove(msgId);
		}
		Utils.sleep(1000);
		wrtieMonitorRecordToZK();
		if (matt != null) {// 停止
			matt.destroy();
		}
	}

	public void collectOK(long msgId, long size, String clientIp) {
		MonitorAttr matt = monitorAttMap.get(msgId);
		matt.collectNumAdd(1);
		matt.collectSizeAdd(size);
		CollectLogPO logPO = null;
		if (detail1_isAva.get(msgId).get()) {
			logPO = detailLogMap1.get(msgId);
		} else {
			logPO = detailLogMap2.get(msgId);
		}
		logPO.getBYTE_SIZE().addAndGet(size);
		logPO.getMSG_NUM().addAndGet(1);
	}

	public void collectError(long msgId, String error, String clientIp) {
		MonitorAttr matt = monitorAttMap.get(msgId);
		matt.collectErrorNumAdd(1);
		CollectErrorLogPO logPO = new CollectErrorLogPO();
		logPO.setMSG_ID(msgId);
		logPO.setSRC_CLIENT(clientIp);
		logPO.setSERVER_HOST(hostName);
		logPO.setERROR_INFO(error);
		logPO.setERROR_TIME(Convert.toTimeStr(new Date(), null));
		if (error1_isAva.get()) {
			collectErrorLogs1.add(logPO);
		} else {
			collectErrorLogs2.add(logPO);
		}
	}

	public void filter(long msgId, long storeId, Object[] tuples) {
		MonitorAttr matt = monitorAttMap.get(msgId);
		matt.filterNumAdd(storeId, 1);
		// 写日志？
		LogUtils.info("消息[" + msgId + "]消息被过滤：" + StringUtils.join(tuples, ","));
	}

	public void distinct(long msgId, long storeId, Object[] tuples) {
		MonitorAttr matt = monitorAttMap.get(msgId);
		matt.distinctNumAdd(storeId, 1);
		// 写日志?
		LogUtils.info("消息[" + msgId + "]重复记录：" + StringUtils.join(tuples, ","));
	}

	public void sendOK(long msgId, long storeId, int num, long size) {
		MonitorAttr matt = monitorAttMap.get(msgId);
		matt.sendNumAdd(storeId, num);
		matt.sendSizeAdd(storeId, size);
	}

	public void sendError(long msgId, long storeId, String error, Object[] tup) {
		MonitorAttr matt = monitorAttMap.get(msgId);
		matt.sendErrorNumAdd(storeId, 1);
		CollectSendErrorLogPO logPO = new CollectSendErrorLogPO();
		logPO.setMSG_ID(msgId);
		logPO.setSTORE_ID(storeId);
		logPO.setSERVER_HOST(hostName);
		logPO.setERROR_DATA(error);
		logPO.setSEND_TIME(Convert.toTimeStr(new Date(), null));
		logPO.setMSG_CONTENT(JSON.toJSONString(tup));
		if (send1_isAva.get()) {
			collectKafkaLogs1.add(logPO);
		} else {
			collectKafkaLogs2.add(logPO);
		}
	}

	public void sendError(long msgId, long storeId, String error, List<Object[]> todoSends) {
		for (Object[] objects : todoSends) {
			sendError(msgId, storeId, error, objects);
		}
	}

	public void leaveNoSend(long msgId, long storeId, long size) {
		if (currentStartd.get(msgId)) {
			MonitorAttr matt = monitorAttMap.get(msgId);
			matt.leaveNoSendSizeAdd(storeId, size);
		}
	}

	public boolean isStared(long msgId) {
		return currentStartd.containsKey(msgId) && currentStartd.get(msgId);
	}

	public MonitorAttr getMsgMonitorAttr(long msgId) {
		return monitorAttMap.get(msgId);
	}

}
