package com.ery.ertc.collect.forwards;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.exception.ConfigException;
import com.ery.ertc.collect.forwards.send.ISendPlugin;
import com.ery.ertc.collect.forwards.send.SendHDFS;
import com.ery.ertc.collect.forwards.send.SendHbase;
import com.ery.ertc.collect.forwards.send.SendKafka;
import com.ery.ertc.collect.forwards.send.SendRDBMS;
import com.ery.ertc.collect.log.CollectLog;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.po.POConstant;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;

public abstract class Send {

	protected MsgPO msgPO;
	protected MsgExtPO extPo;
	final MsgCache cache;
	public final DaemonMaster daemonMaster;
	final MsgQueue msgQueue;
	public ISendPlugin sendPlugin;
	public final long msgId;
	public final long stroeId;

	public MsgPO getMsgPO() {
		return msgPO;
	}

	public void setMsgPO(MsgPO msgPO) {
		this.msgPO = msgPO;
	}

	public MsgExtPO getExtPo() {
		return extPo;
	}

	public void setExtPo(MsgExtPO extPo) {
		this.extPo = extPo;
	}

	public MsgCache getCache() {
		return cache;
	}

	public DaemonMaster getDaemonMaster() {
		return daemonMaster;
	}

	public MsgQueue getMsgQueue() {
		return msgQueue;
	}

	public Send(MsgPO msgPO, MsgExtPO extPo, MsgCache cache) {
		this.msgPO = msgPO;
		this.cache = cache;
		this.extPo = extPo;
		this.msgId = msgPO.getMSG_ID();
		this.stroeId = extPo.getSTORE_ID();
		msgQueue = cache.msgQueue;
		daemonMaster = cache.daemonMaster;
		sendPlugin = ISendPlugin.configurePlugin(this, extPo);
	}

	/**
	 * 发送
	 */
	public void stop() {
		try {
			if (sendPlugin != null) {
				ISendPlugin.closePlugin(sendPlugin);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public abstract int send() throws IOException;

	/**
	 * 从发送队列获取一条记录，此处效率影响发送吞吐，如果没取到，则触发填充
	 * 
	 * @return
	 */
	protected Object[] getOne() {
		return cache.getOne(extPo);
	}

	/**
	 * 获取一些数据
	 * 
	 * @param more
	 *            数量
	 * @return
	 */
	protected List<Object[]> getMore(int more) {
		return cache.getMore(extPo, more);
	}

	// 发送OK
	protected void sendOK(Object[] kms) {
		if (extPo.getLOG_LEVEL() != 0) {
			CollectLog.sendLog(msgPO, extPo, kms, Convert.toLong(kms[1]));
		}
	}

	// 发送OK
	protected void sendOK(List<Object[]> sendList) {
		if (extPo.getLOG_LEVEL() != 0)
			for (Object[] kms : sendList) {
				CollectLog.sendLog(msgPO, extPo, kms, Convert.toLong(kms[1]));
			}
	}

	// 发送失败，重发
	protected void sendFail(Object[] kms) {
		cache.sendFail(extPo, kms);
	}

	// 发送失败重发
	protected void sendFail(List<Object[]> sendList) {
		cache.sendFail(extPo, sendList);
	}

	public static Send newInstance(MsgPO msgPO, MsgExtPO extPo, MsgCache cache) {
		try {
			switch (extPo.getSEND_TARGET()) {
			case POConstant.SEND_TARGET_KAFKA:
				return new SendKafka(msgPO, extPo, cache);
			case POConstant.SEND_TARGET_HBASE:
				return new SendHbase(msgPO, extPo, cache);
			case POConstant.SEND_TARGET_HDFS:
				return new SendHDFS(msgPO, extPo, cache);
			case POConstant.SEND_TARGET_RDBMS:
				return new SendRDBMS(msgPO, extPo, cache);
			}
		} catch (Exception e) {
			LogUtils.error("消息[" + msgPO.getMSG_TAG_NAME() + "]发送目标[" + extPo.getSTORE_ID() + "] 初始化目标写入体异常", e);
			if (e instanceof ConfigException) {
				throw (ConfigException) e;
			}
			return null;
		}
		throw new RuntimeException("消息[" + msgPO.getMSG_TAG_NAME() + "]发送目标[" + extPo.getSEND_TARGET() + "]无法识别!");
	}

}
