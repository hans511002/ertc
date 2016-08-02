package com.ery.ertc.collect.forwards;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.forwards.MsgCache.ExtMsgCache;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.StringUtils;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Utils;

public class MsgSendRunnable implements Runnable {
	public DaemonMaster daemonMaster;
	private MsgPO msgPO;
	private Send send;// 发送接口
	final MsgCache cache;
	final MsgQueue msgQueue;
	final MsgExtPO extPo;

	// 计算大致速率
	private long stime = 0;
	private int num;
	private int nps;// 速率
	ExtMsgCache extCache;

	public MsgSendRunnable(DaemonMaster daemonMaster, MsgPO msgPO, MsgExtPO extPo, MsgCache cache) {
		this.msgPO = msgPO;
		this.daemonMaster = daemonMaster;
		this.cache = cache;
		this.extPo = extPo;
		this.msgQueue = cache.msgQueue;
		extCache = this.cache.extMsgCaches.get(extPo.getSTORE_ID());
	}

	public void getSender() {
		long retryTime = 0;
		while (this.msgQueue.state == 1 && send == null) {
			try {
				if (System.currentTimeMillis() - retryTime >= 6000) {// 60秒
					retryTime = System.currentTimeMillis();
					this.send = Send.newInstance(msgPO, extPo, cache);
					break;
				}
				Utils.sleep(10000);
			} catch (Throwable e) {
				LogUtils.error("初始化send异常", e);
				cache.daemonMaster.workerService.monitor.setRunStatus(
						msgPO.getMSG_ID(),
						this.extPo.getSTORE_ID(),
						"存储异常" + extPo.getSTORE_ID() + " startFail:" +
								StringUtils.printStackTrace(e).replaceAll("\t", "&nbsp;").replaceAll("\n", "<br/>"));
				Utils.sleep(10000);
			} finally {
			}
		}
	}

	@Override
	public void run() {
		getSender();
		while (this.msgQueue.state == 1) {
			try {
				if (send == null) {
					getSender();
				}
				int len = send.send();
				if (len >= 0) {
					num += len;
				}
				long t = System.currentTimeMillis();
				if (t - stime >= 5000l) {
					stime = t;
					nps = num / 5;
					num = 0;
					if (nps > 10000) {
						Utils.sleep(10);
					} else if (nps > 3000) {
						Utils.sleep(50);
					} else if (nps > 1000) {
						Utils.sleep(100);
					} else if (nps > 500) {
						Utils.sleep(150);
					} else if (nps > 100) {
						Utils.sleep(200);
					} else if (nps > 50) {
						Utils.sleep(400);
					} else if (nps > 10) {
						Utils.sleep(500);
					} else {
						Utils.sleep(1000);
					}
				}
				if (extCache.queueSize.get() == 0) {
					synchronized (extCache.fillMutex) {
						extCache.fillMutex.notifyAll();// 填充
					}
				}
			} catch (Throwable e) {
				LogUtils.error(e.getMessage(), e);
				send.stop();
				send = null;
			}
		}
		if (send != null)
			send.stop();
	}
}
