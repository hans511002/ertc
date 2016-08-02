package com.ery.ertc.collect.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.forwards.MsgQueue;
import com.ery.ertc.collect.forwards.MsgQueue.StopThread;
import com.ery.ertc.collect.remote.server.HttpCollectServer;
import com.ery.ertc.collect.remote.server.SocketCollectServer;
import com.ery.ertc.collect.remote.server.WSCollectServer;
import com.ery.ertc.collect.task.TaskAssignTracker;
import com.ery.ertc.collect.watch.CollectMonitor;
import com.ery.ertc.collect.watch.MonitorAttr;
import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.collect.worker.executor.PollTaskExecutor;
import com.ery.ertc.collect.worker.executor.ServiceTaskExecutor;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.StringUtils;
import com.ery.ertc.estorm.util.ToolUtil;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;

public class WorkerService {

	public static Map<Long, MsgPO> msgPOMap = new HashMap<Long, MsgPO>();

	public static MsgPO getMsgPO(long msgId) {
		return msgPOMap.get(msgId);
	}

	public void notifyStopMonitor() {
		try {
			monitor.stopMonitor();
		} catch (Exception e) {
			LogUtils.error("通知停止监控[" + monitor.getClass().getSimpleName() + "]出错!", e);
		}
	}

	public void notifyStopCollect(long msgId) {
		try {
			monitor.stopCollect(msgId);
		} catch (Exception e) {
			LogUtils.error("通知停止采集[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void notifyCollectOK(long msgId, long size, String clientIP) {
		try {
			monitor.collectOK(msgId, size, clientIP);
		} catch (Exception e) {
			LogUtils.error("通知采集OK[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void notifyCollectError(long msgId, String error, String clientIP) {
		try {
			monitor.collectError(msgId, error, clientIP);
		} catch (Exception e) {
			LogUtils.error("通知采集异常[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void notifyFilter(long msgId, long storeId, Object[] tuples) {
		try {
			monitor.filter(msgId, storeId, tuples);
		} catch (Exception e) {
			LogUtils.error("通知过滤[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void notifyDistinct(long msgId, long storeId, Object[] tuples) {
		try {
			monitor.distinct(msgId, storeId, tuples);
		} catch (Exception e) {
			LogUtils.error("通知重复[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void notifySendOK(long msgId, long storeId, int num, long size) {
		try {
			monitor.sendOK(msgId, storeId, num, size);
		} catch (Exception e) {
			LogUtils.error("通知发送OK[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void notifySendError(long msgId, long storeId, String error, Object[] tup) {
		try {
			monitor.sendError(msgId, storeId, error, tup);
		} catch (Exception e) {
			LogUtils.error("通知发送异常[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void notifySendError(long msgId, long storeId, String error, List<Object[]> todoSends) {
		try {
			monitor.sendError(msgId, storeId, error, todoSends);
		} catch (Exception e) {
			LogUtils.error("通知发送异常[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	public void notifyLeaveNoSend(long msgId, long storeId, long size) {
		try {
			monitor.leaveNoSend(msgId, storeId, size);
		} catch (Exception e) {
			LogUtils.error("通知遗留未发送异常[" + monitor.getClass().getSimpleName() + "]出现异常!", e);
		}
	}

	private DaemonMaster daemonMaster;
	private Map<String, AbstractTaskExecutor> taskExecutorMap = new HashMap<String, AbstractTaskExecutor>();
	private HttpCollectServer httpServer;
	private WSCollectServer wsServer;
	private SocketCollectServer socketServer;
	protected ZkClient zkClient;
	String msgRootPath;
	boolean started = false;
	public MsgConfigListener msgListener = new MsgConfigListener();
	public final CollectMonitor monitor;

	public WorkerService(ZkClient zkClient, DaemonMaster daemonMaster) {
		this.daemonMaster = daemonMaster;
		this.zkClient = zkClient;
		httpServer = new HttpCollectServer();
		wsServer = new WSCollectServer();
		socketServer = new SocketCollectServer();
		monitor = new CollectMonitor(daemonMaster.getZkClient());
		msgRootPath = ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE + "/";
	}

	public static String getNodeName(String path) {
		return path.substring(path.lastIndexOf("/") + 1);
	}

	public List<MsgPO> startFailedMsg = new ArrayList<MsgPO>();

	public class MsgConfigListener implements IZkDataListener, IZkChildListener {

		@Override
		public void handleDataDeleted(String dataPath) {
			if (!dataPath.startsWith(msgRootPath)) {// 删除一个MSG
				String msgid = getNodeName(dataPath);
				synchronized (WorkerService.msgPOMap) {
					if (WorkerService.this.daemonMaster.getTaskAssignTracker().assignInfo.contains(msgid))
						WorkerService.this.stopMsgTask(msgid);
					MsgPO msgpo = WorkerService.msgPOMap.remove(Convert.toLong(msgid));
				}
			}
		}

		@Override
		public void handleDataChange(String dataPath, Object data) throws Exception {
			if (!dataPath.startsWith(msgRootPath)) {
				return;
			}
			synchronized (WorkerService.msgPOMap) {
				String msgid = getNodeName(dataPath);
				long msgId = Convert.toLong(msgid);
				MsgPO po = (MsgPO) ToolUtil.deserializeObject(data.toString(), TaskAssignTracker.serialIsGzip,
						TaskAssignTracker.serialIsUrlCode);
				MsgPO oldPoInfo = WorkerService.msgPOMap.get(msgId);
				boolean isNeedUpdate = oldPoInfo == null ? true : oldPoInfo.syncMsgInfo(po);
				MonitorAttr attr = monitor.getMsgMonitorAttr(msgId);
				if (isNeedUpdate || attr == null || !WorkerService.this.monitor.isStared(msgId)) {// 重启服务
					WorkerService.msgPOMap.put(msgId, po);// 判断是否存在于本机执行
					if (WorkerService.this.daemonMaster.getTaskAssignTracker().assignInfo != null &&
							WorkerService.this.daemonMaster.getTaskAssignTracker().assignInfo.contains(msgid))
						WorkerService.this.restartMsgTask(msgid);
				}
			}
		}

		@Override
		public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
			for (String msgId : currentChilds) {
				if (!msgPOMap.containsKey(msgId)) {
					msgListener.handleDataChange(msgRootPath + msgId, zkClient.readData(msgRootPath + msgId));
					zkClient.subscribeDataChanges(msgRootPath + msgId, msgListener);
				}
			}
		}
	}

	public class QueueUtilsReStartSendQueue implements MsgQueue.QueueStartOrStopCall {
		// 停止后准备重启
		final MsgPO msgpo;

		public QueueUtilsReStartSendQueue(MsgPO msgpo) {
			this.msgpo = msgpo;
		}

		@Override
		public void call(MsgPO msgpo, int state) {
			try {
				// final MsgPO msgPo =
				// WorkerService.msgPOMap.get(Convert.toLong(msgpo.getMSG_ID()));
				WorkerService.this.daemonMaster.queueUtils.startSendQueue(this.msgpo, new QueueUtilsStartSendQueue());
			} catch (Exception e) {
				msgTaskStartError(msgpo, e);
			}
		}
	}

	// 转发队列启动后启动任务采集程序
	public class QueueUtilsStartSendQueue implements MsgQueue.QueueStartOrStopCall {
		@Override
		public void call(MsgPO msgPo, int state) {
			try {
				AbstractTaskExecutor taskExecutor = buildTaskExecutor(msgPo);
				taskExecutorMap.put(msgPo.getMSG_ID() + "", taskExecutor);
				taskExecutor.start();
				monitor.setRunStatus(msgPo, "runing");
				LogUtils.info("任务[" + msgPo.getMSG_ID() + "," + msgPo.getMSG_TAG_NAME() + "],重启完成!");
			} catch (Exception e) {
				msgTaskStartError(msgPo, e);
			}
		}
	}

	public void msgTaskStartError(MsgPO msgPo, Exception e) {
		LogUtils.error("启动任务[" + msgPo.getMSG_ID() + "," + msgPo.getMSG_TAG_NAME() + "]出错!", e);
		monitor.setRunStatus(msgPo, "startFail:" + StringUtils.printStackTrace(e).replace("\n", "<br/>"));
	}

	public void restartMsgTask(final String msgid) {
		synchronized (taskExecutorMap) {
			AbstractTaskExecutor taskExecutor = taskExecutorMap.remove(msgid);
			final MsgPO msgPo = WorkerService.msgPOMap.get(Convert.toLong(msgid));
			try {
				monitor.startCollect(msgPo);
				if (taskExecutor != null) {
					taskExecutor.stop(); // 先停止上次的
				}
				LogUtils.info("任务变更[" + msgid + "," + msgPo.getMSG_TAG_NAME() + "]，准备重启!");
				daemonMaster.queueUtils.reStartSendQueue(msgPo, new QueueUtilsReStartSendQueue(msgPo));
			} catch (Exception e) {
				msgTaskStartError(msgPo, e);
			}
		}
	}

	public void stopMsgTask(String msgid) {
		AbstractTaskExecutor taskExecutor = null;
		synchronized (taskExecutorMap) {
			taskExecutor = taskExecutorMap.remove(msgid);
		}
		if (taskExecutor != null) {
			try {
				taskExecutor.stop();
				monitor.setRunStatus(msgPOMap.get(Convert.toLong(msgid)), "stoping");
				daemonMaster.queueUtils.stopSendQueue(Convert.toLong(msgid));
				LogUtils.info("停止任务[" + msgid + "," + taskExecutor.getMsgPO().getMSG_TAG_NAME() + "]");
			} catch (Exception e) {
				LogUtils.info("停止任务[" + msgid + "," + taskExecutor.getMsgPO().getMSG_TAG_NAME() + "]出错!", e);
			}
		}
	}

	//
	// public synchronized void runTask(List<String> tasks) {
	// if (tasks == null) {
	// tasks = new ArrayList<String>();
	// }
	// if (tasks.size() == 0)
	// return;
	// // 删除任务
	// List<String> taskIdS = new ArrayList<String>(taskExecutorMap.keySet());
	// for (String tid : taskIdS) {
	// if (!tasks.contains(tid)) {
	// AbstractTaskExecutor taskExecutor = taskExecutorMap.remove(tid);
	// try {
	// taskExecutor.stop();
	// QueueUtils.stopSendQueue(Convert.toLong(tid));
	// LogUtils.info("停止任务[" + tid + "," +
	// taskExecutor.getMsgPO().getMSG_TAG_NAME() + "]");
	// WorkerService.notifyStopCollect(Convert.toLong(tid));
	// } catch (Exception e) {
	// LogUtils.error("停止任务[" + tid + "," +
	// taskExecutor.getMsgPO().getMSG_TAG_NAME() + "]出错!" + e);
	// }
	// msgPOMap.remove(Convert.toLong(tid));
	// }
	// }
	//
	// // 最新分配任务
	// CollectDAO dao = new CollectDAO();
	// for (final String taskId : tasks) {
	// final MsgPO taskInfo = dao.getCollectMsgInfo(taskId);
	// if (taskInfo != null) {
	// if (taskExecutorMap.containsKey(taskId)) {
	// MsgPO oldTaskInfo = taskExecutorMap.get(taskId).getMsgPO();
	// if (oldTaskInfo.syncMsgInfo(taskInfo)) {
	// // 任务触发类型或者任务消息主题发生变更，需要重新注册相关工作服务
	// try {
	// taskExecutorMap.get(taskId).stop(); // 先停止上次的
	// QueueUtils.reStartSendQueue(taskInfo, new MsgQueue.QueueStartOrStopCall()
	// {
	// // 停止后准备重启
	// @Override
	// public void call(MsgPO msgpo, int state) {
	// QueueUtils.startSendQueue(taskInfo, new MsgQueue.QueueStartOrStopCall() {
	// // 转发队列启动后启动任务采集程序
	// @Override
	// public void call(MsgPO msgpo, int state) {
	// try {
	// AbstractTaskExecutor taskExecutor = buildTaskExecutor(taskInfo);
	// taskExecutorMap.put(taskId, taskExecutor);
	// WorkerService.notifyStartCollect(taskInfo.getMSG_ID());
	// taskExecutor.start();
	// LogUtils.info("任务[" + taskInfo.getMSG_ID() + "," +
	// taskInfo.getMSG_TAG_NAME() + "],重启完成!");
	// } catch (Exception e) {
	// LogUtils.error(
	// "重启任务[" + taskInfo.getMSG_ID() + "," +
	// taskInfo.getMSG_TAG_NAME() + "]出错!", e);
	// }
	// }
	// });
	// }
	// });
	// LogUtils.info("任务变更[" + taskId + "," +
	// taskExecutorMap.get(taskId).getMsgPO().getMSG_TAG_NAME() + "]，准备重启!");
	// } catch (Exception e) {
	// LogUtils.error("变更停止任务[" + taskId + "," +
	// taskExecutorMap.get(taskId).getMsgPO().getMSG_TAG_NAME() + "]出错!" + e);
	// }
	// } else {
	// LogUtils.info("任务[" + taskId + "," + taskInfo.getMSG_TAG_NAME() +
	// "]元数据已同步!");
	// }
	// } else {
	// msgPOMap.put(taskInfo.getMSG_ID(), taskInfo);
	// QueueUtils.startSendQueue(taskInfo, new MsgQueue.QueueStartOrStopCall() {
	// @Override
	// public void call(MsgPO msgpo, int state) {
	// try {
	// AbstractTaskExecutor taskExecutor = buildTaskExecutor(taskInfo);
	// synchronized (taskExecutorMap) {
	// if (!taskExecutorMap.containsKey(taskId)) {
	// taskExecutorMap.put(taskId, taskExecutor);
	// WorkerService.notifyStartCollect(taskInfo.getMSG_ID());
	// taskExecutor.start();
	// LogUtils.info("启动任务[" + taskId + "," + taskInfo.getMSG_TAG_NAME() + "]");
	// }
	// }
	// } catch (Exception e) {
	// LogUtils.error("启动任务[" + taskId + "," + taskInfo.getMSG_TAG_NAME() +
	// "]出错!", e);
	// }
	// }
	// });
	// }
	// } else {
	// LogUtils.warn("未找到任务[" + taskId + "]!");
	// }
	// }
	// dao.close();
	// }

	// 生成一个任务执行者
	private AbstractTaskExecutor buildTaskExecutor(MsgPO taskInfo) {
		int triggerType = taskInfo.getTRIGGER_TYPE();
		if (triggerType < 20) {
			// 主动（轮询线程）
			return new PollTaskExecutor(taskInfo, this);
		} else {
			// 被动（提供服务端接口）
			return new ServiceTaskExecutor(taskInfo, this);
		}
	}

	// 启动服务
	public void startServer() {
		httpServer.start();
		wsServer.start();
		socketServer.start();
		try {// 恢复
			String msgBasePath = ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE;
			List<String> childs = zkClient.getChildren(msgBasePath);
			msgListener.handleChildChange(msgBasePath, childs);
			zkClient.subscribeChildChanges(msgBasePath, msgListener);
		} catch (Exception e) {
			e.printStackTrace();
		}
		started = true;
	}

	public void stopAllTask(boolean wait) {
		List<StopThread> ths = new ArrayList<StopThread>();
		synchronized (taskExecutorMap) {
			for (String msgId : taskExecutorMap.keySet()) {
				AbstractTaskExecutor exc = taskExecutorMap.get(msgId);
				try {
					exc.stop();// 先停止接收和抓取
					StopThread stop = daemonMaster.queueUtils.stopSendQueue(Convert.toLong(msgId));
					if (wait && stop != null) {
						ths.add(stop);
					}
					LogUtils.info("停止任务[" + msgId + "," + exc.getMsgPO().getMSG_TAG_NAME() + "]");
				} catch (Exception e) {
					LogUtils.info("停止任务[" + msgId + "," + exc.getMsgPO().getMSG_TAG_NAME() + "]出错!", e);
				}
			}
			taskExecutorMap.clear();
		}
		if (wait) {
			for (StopThread stopThread : ths) {
				try {
					stopThread.join();
				} catch (InterruptedException e) {
				}
			}
		}
	}

	// 停止服务
	public void stopServer() {
		if (!taskExecutorMap.isEmpty()) {
			stopAllTask(true);
		}
		httpServer.stop();
		wsServer.stop();
		socketServer.stop();
		try {
			monitor.stopMonitor();
		} catch (Exception e) {
			LogUtils.error("通知停止监控[" + monitor.getClass().getSimpleName() + "]出错!", e);
		}
	}

	public DaemonMaster getDaemonMaster() {
		return daemonMaster;
	}

	public HttpCollectServer getHttpServer() {
		return httpServer;
	}

	public WSCollectServer getWsServer() {
		return wsServer;
	}

	public SocketCollectServer getSocketServer() {
		return socketServer;
	}

	public Map<String, AbstractTaskExecutor> getTaskExecutorMap() {
		return taskExecutorMap;
	}
}
