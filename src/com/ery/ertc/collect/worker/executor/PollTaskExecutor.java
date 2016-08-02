package com.ery.ertc.collect.worker.executor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Random;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.remote.client.ClassCollectExec;
import com.ery.ertc.collect.remote.client.CollectExec;
import com.ery.ertc.collect.remote.client.FileCollectExec;
import com.ery.ertc.collect.remote.client.HbaseCollectExec;
import com.ery.ertc.collect.remote.client.HttpCollectExec;
import com.ery.ertc.collect.remote.client.SQLCollectExec;
import com.ery.ertc.collect.remote.client.SocketCollectExec;
import com.ery.ertc.collect.remote.client.WSCollectExec;
import com.ery.ertc.collect.remote.client.ZkCollectExec;
import com.ery.ertc.collect.task.dao.CollectDAO;
import com.ery.ertc.collect.worker.WorkerService;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.po.POConstant;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class PollTaskExecutor extends AbstractTaskExecutor {

	private int triggerType;
	private Thread collThread;
	private CollectExec collectExec;
	private long period = 5000;// 采集频率(5秒一次)

	public PollTaskExecutor(final MsgPO msgPO, WorkerService workerService) {
		this.workerService = workerService;
		this.msgPO = msgPO;
		triggerType = msgPO.getTRIGGER_TYPE();
		Map<String, Object> reqMap = msgPO.getParamMap();
		if (reqMap != null) {
			period = Convert.toLong(reqMap.get(POConstant.Msg_REQ_PARAM_period), period);
		}
		switch (triggerType) {
		case POConstant.TRI_TYPE_I_HTTP:
			collectExec = new HttpCollectExec(this);
			break;
		case POConstant.TRI_TYPE_I_SOCKET:
			collectExec = new SocketCollectExec(this);
			break;
		case POConstant.TRI_TYPE_I_SQL:
			collectExec = new SQLCollectExec(this);
			break;
		case POConstant.TRI_TYPE_I_WS:
			collectExec = new WSCollectExec(this);
			break;
		case POConstant.TRI_TYPE_I_ZK:
			collectExec = new ZkCollectExec(this);
			break;
		case POConstant.TRI_TYPE_I_CLASS:
			collectExec = new ClassCollectExec(this);
			break;
		case POConstant.TRI_TYPE_I_FILE:
			collectExec = new FileCollectExec(this);
			break;
		case POConstant.TRI_TYPE_I_HBASE:
			collectExec = new HbaseCollectExec(this);
			break;
		}
		if (triggerType != POConstant.TRI_TYPE_I_ZK) {
			collThread = new Thread() {
				public void run() {
					Utils.sleep(new Random().nextInt(6000) + 1);// 随机在0-6秒后启动采集，避免所有采集任务在时间上扎堆
					while (true) {
						try {
							collectExec.execute();
							collectExec.setStateOrInfo(1, null);
						} catch (Exception e) {
							StringWriter sw = new StringWriter();
							PrintWriter pw = new PrintWriter(sw);
							e.printStackTrace(pw);
							String estr = sw.toString();
							DaemonMaster.daemonMaster.workerService.notifyCollectError(msgPO.getMSG_ID(), estr,
									collectExec.getSrcClient());
							LogUtils.error("消息[" + msgPO.getMSG_TAG_NAME() + "]执行" + getCollectName(triggerType) +
									"采集出错!" + e.getMessage(), e);
							collectExec.setStateOrInfo(0, estr);
						}
						Utils.sleep(period);
					}
				}
			};
		}
	}

	private static String getCollectName(int type) {
		switch (type) {
		case POConstant.TRI_TYPE_I_HTTP:
			return "Http";
		case POConstant.TRI_TYPE_I_SOCKET:
			return "Socket";
		case POConstant.TRI_TYPE_I_SQL:
			return "Sql";
		case POConstant.TRI_TYPE_I_WS:
			return "WebService";
		case POConstant.TRI_TYPE_I_ZK:
			return "ZK";
		case POConstant.TRI_TYPE_I_CLASS:
			return "Class";
		case POConstant.TRI_TYPE_I_FILE:
			return "File";
		case POConstant.TRI_TYPE_I_HBASE:
			return "Hbase";
		}
		return "Unknown";
	}

	@Override
	public void start() {
		if (triggerType != POConstant.TRI_TYPE_I_ZK) {
			collThread.start();
			collectExec.setStateOrInfo(1, null);
			LogUtils.info("轮询(间隔:" + period + ")消息[" + msgPO.getMSG_TAG_NAME() + "]采集启动 OK!");
		} else {
			// ZK直接注册监听
			try {
				collectExec.execute();
				collectExec.setStateOrInfo(1, null);
				LogUtils.info("ZK监听消息[" + msgPO.getMSG_TAG_NAME() + "]采集启动 OK!");
			} catch (Exception e) {
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				String estr = sw.toString();
				DaemonMaster.daemonMaster.workerService.notifyCollectError(msgPO.getMSG_ID(), estr,
						collectExec.getSrcClient());
				LogUtils.error("消息[" + msgPO.getMSG_TAG_NAME() + "]执行" + getCollectName(POConstant.TRI_TYPE_I_ZK) +
						"采集出错!" + e.getMessage(), e);
				collectExec.setStateOrInfo(-1, estr);
			}
		}
	}

	@Override
	public boolean isRunning() {
		return collectExec.getState() != -1;
	}

	@Override
	public void stop() throws Exception {
		if (triggerType != POConstant.TRI_TYPE_I_ZK) {
			collThread.stop();
			collectExec.stopRun();
			collectExec.setStateOrInfo(-1, "正常停止!");
			LogUtils.info("停止轮询消息[" + msgPO.getMSG_TAG_NAME() + "] OK!");
		} else {
			collectExec.stopRun();
			collectExec.setStateOrInfo(-1, "正常停止!");
			LogUtils.info("ZK停止监听消息[" + msgPO.getMSG_TAG_NAME() + "] OK!");
		}
		saveCollectPointer();
	}

	public void saveCollectPointer() {
		String pointer = collectExec.getLastCollectPointer();
		CollectDAO dao = new CollectDAO();
		dao.saveCollectPointer(msgPO.getMSG_ID(), pointer);
		dao.close();
		msgPO.setLAST_COLLECT_POINTER(pointer);
		LogUtils.info("保存消息[" + msgPO.getMSG_TAG_NAME() + "]上次采集位置->" + pointer);
	}

}
