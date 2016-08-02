package com.ery.ertc.collect.watch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.conf.CollectConstant;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.HasThread;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class MonitorAttr implements Serializable {

	private static final long serialVersionUID = -1745071346184869468L;
	private long MSG_ID;// 消息ID
	private AtomicLong logIDSn = new AtomicLong(0);// 日志序号

	private String startTime;// 启动时间
	public Map<Long, String> runStatus = new HashMap<Long, String>();// 运行状态
																		// running
																		// stoped:stopTime
	// starFail:errmsg
	private AtomicLong collectNum = new AtomicLong(0);// 采集数
	private AtomicLong collectSize = new AtomicLong(0);// 采集消息大小
	private AtomicLong collectErrorNum = new AtomicLong(0);// 采集异常次数
	// ///////////////////////////////////放入MAP针对不同外部存储计数///////////////////////////////////////
	Map<Long, List<AtomicLong>> expStoreSendInfo = new HashMap<Long, List<AtomicLong>>();

	// static class expStoreSendInfo {
	// private AtomicLong filterNum = new AtomicLong(0);// 被过滤的数量
	// private AtomicLong distinctNum = new AtomicLong(0);// 重复记录的数量
	// private AtomicLong sendNum = new AtomicLong(0);// 发送数
	// private AtomicLong sendSize = new AtomicLong(0);// 发送消息大小
	// private AtomicLong sendErrorNum = new AtomicLong(0);// 发送异常次数
	// private AtomicLong leaveNoSendSize = new AtomicLong(0);// 遗留未发送消息
	// }

	private LinkedList<List<Object>> snapMonitor = new LinkedList<List<Object>>();// 每一秒快照

	public static final int SNAP_NUM = 5;// 快照数量
	private HasThread snapThread;

	public MonitorAttr(MsgPO msgPo) {
		this.MSG_ID = msgPo.getMSG_ID();
		this.startTime = Convert.toTimeStr(new Date(), null);
		for (MsgExtPO extPo : msgPo.getExtPO()) {
			List<AtomicLong> extInfo = new ArrayList<AtomicLong>();
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			expStoreSendInfo.put(extPo.getSTORE_ID(), extInfo);
			this.runStatus.put(extPo.getSTORE_ID(), "starting");
		}
		snapThread = new HasThread() {
			public void run() {
				while (true) {
					synchronized (snapMonitor) {
						try {
							snapMonitor.add(new ArrayList<Object>() {
								{
									add(collectNum.get());
									add(collectSize.get());
									add(collectErrorNum.get());
									add(CollectConstant.objectMapper.writeValueAsString(expStoreSendInfo));
									// add(sendNum.get());
									// add(sendSize.get());
									// add(sendErrorNum.get());
									// add(leaveNoSendSize.get());
									// add(filterNum.get());
									// add(distinctNum.get());
									add(logIDSn.get());
								}
							});
							if (snapMonitor.size() == SNAP_NUM + 1) {
								snapMonitor.removeFirst();
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					Utils.sleep(1000);
				}
			}
		};
		snapThread.setName("SnapMonitor").setDaemon(true).start();
	}

	// 释放资源
	public void destroy() {
		try {
			snapThread.interrupt();
		} catch (Throwable e) {
		} finally {
			snapThread = null;
		}
	}

	// 返回监控json字符串
	public String toJSONString() {
		synchronized (snapMonitor) {
			snapMonitor.getLast().add(startTime);
			snapMonitor.getLast().add(runStatus);
			return JSON.toJSONString(snapMonitor);
		}
	}

	// 获取值
	public long getMSG_ID() {
		return MSG_ID;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setRunStatus(long extId, String runStatus) {
		this.runStatus.put(extId, runStatus);
	}

	public String getRunStatus(long extId) {
		return runStatus.get(extId);
	}

	public Map<Long, String> getRunStatus() {
		return runStatus;
	}

	public long getCollectNum() {
		return collectNum.get();
	}

	public long getCollectSize() {
		return collectSize.get();
	}

	public long getCollectErrorNum() {
		return collectErrorNum.get();
	}

	public long getFilterNum(long storeId) {
		return getExpStoreInfoList(storeId).get(0).get();
	}

	public long getDistinctNum(long storeId) {
		return getExpStoreInfoList(storeId).get(1).get();
	}

	public long getSendNum(long storeId) {
		return getExpStoreInfoList(storeId).get(2).get();
	}

	public long getSendSize(long storeId) {
		return getExpStoreInfoList(storeId).get(3).get();
	}

	public long getSendErrorNum(long storeId) {
		return getExpStoreInfoList(storeId).get(4).get();
	}

	public long getLeaveNoSendSize(long storeId) {
		return getExpStoreInfoList(storeId).get(5).get();
	}

	// 操作
	public void initMaxLogId(long maxLogId) {
		logIDSn.set(maxLogId);
	}

	public long getNewLogId() {
		synchronized (logIDSn) {
			return logIDSn.incrementAndGet();
		}
	}

	public long collectNumAdd(long num) {
		return collectNum.addAndGet(num);
	}

	public long collectSizeAdd(long size) {
		return collectSize.addAndGet(size);
	}

	public long collectErrorNumAdd(long errorNum) {
		return collectErrorNum.addAndGet(errorNum);
	}

	public long filterNumAdd(long storeId, long num) {
		return getExpStoreInfoList(storeId).get(0).addAndGet(num);
	}

	public long distinctNumAdd(long storeId, long num) {
		return getExpStoreInfoList(storeId).get(1).addAndGet(num);
	}

	public long sendNumAdd(long storeId, long num) {
		return getExpStoreInfoList(storeId).get(2).addAndGet(num);
	}

	public long sendSizeAdd(long storeId, long size) {
		return getExpStoreInfoList(storeId).get(3).addAndGet(size);
	}

	public long sendErrorNumAdd(long storeId, long errorNum) {
		return getExpStoreInfoList(storeId).get(4).addAndGet(errorNum);
	}

	public long leaveNoSendSizeAdd(long storeId, long leaveNoSend) {
		return getExpStoreInfoList(storeId).get(5).addAndGet(leaveNoSend);
	}

	List<AtomicLong> getExpStoreInfoList(long storeId) {
		List<AtomicLong> extInfo = expStoreSendInfo.get(storeId);
		if (extInfo == null) {
			extInfo = new ArrayList<AtomicLong>();
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			extInfo.add(new AtomicLong(0));
			expStoreSendInfo.put(storeId, extInfo);
		} else if (extInfo.size() < 6) {
			for (int i = extInfo.size(); i < 6; i++) {
				extInfo.add(new AtomicLong(0));
			}
		}
		return extInfo;
	}
}
