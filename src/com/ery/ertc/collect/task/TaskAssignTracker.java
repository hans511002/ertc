package com.ery.ertc.collect.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.task.assign.TaskAssign;
import com.ery.ertc.collect.task.dao.CollectDAO;
import com.ery.ertc.collect.worker.WorkerService;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.ertc.collect.zk.ZooKeeperNodeTracker;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.ToolUtil;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class TaskAssignTracker extends ZooKeeperNodeTracker {
	public static boolean serialIsGzip = true;
	public static boolean serialIsUrlCode = true;
	public String hostName;
	public String thisNodeTaskAssignPath;
	public List<String> allMsgIds;
	private Thread queryTask;
	long readMsgModtime = 0;// 用于查询更改的任务
	DaemonMaster daemonMaster;
	public WorkerService workerService;
	HostAssignListener hostAssignListener = new HostAssignListener();
	// 存储当前主机上运行的任务状态,启动时间 停止时间 异常信息等
	Map<String, String> taskRunInfos = new HashMap<String, String>();

	/**
	 * 分配的任务列表 msgId
	 */
	public List<String> assignInfo;
	private Map<String, List<String>> allAssignInfo = new HashMap<String, List<String>>();

	public TaskAssignTracker(ZkClient zkClient, DaemonMaster daemonMaster) {
		super(ZkConstant.ASSIGN_NODE, zkClient, daemonMaster);
		this.daemonMaster = daemonMaster;
		hostName = Config.getHostName();
		thisNodeTaskAssignPath = ZkConstant.BASE_NODE + "/" + ZkConstant.ASSIGN_NODE + "/" + hostName;
		enableListener(true, true);// 监听数据
		this.workerService = daemonMaster.workerService;
	}

	class HostAssignListener implements IZkDataListener {
		@Override
		public void handleDataDeleted(String dataPath) {
			if (thisNodeTaskAssignPath.equals(dataPath)) {
				workerService.stopAllTask(false); // 停止 所有任务
			}
		}

		@Override
		public synchronized void handleDataChange(String dataPath, Object data) throws Exception {
			LogUtils.info("HostAssignListener handleDataChange path:" + dataPath + " data:" + data);
			if (!dataPath.startsWith(path) || data == null) {
				return;
			}
			// 分配任务变更
			if (thisNodeTaskAssignPath.equals(dataPath)) {
				assignInfo = (List<String>) JSON.parseObject(data.toString(), List.class);
				LogUtils.info("主机[" + hostName + "]任务:" + assignInfo);
				// 比较变更 停止或启动任务
				for (String msgId : assignInfo) {
					String msgPath = ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE + "/" + msgId;
					workerService.msgListener.handleDataChange(msgPath, zkClient.readData(msgPath));
				}
				allAssignInfo.put(hostName, assignInfo);
			} else {
				String hostName = dataPath.substring(dataPath.lastIndexOf("/") + 1);
				LogUtils.info("other host assign Change path:" + dataPath + " data:" + data);
				List<String> assignInfo = (List<String>) JSON.parseObject(data.toString(), List.class);
				// for (String msgId : assignInfo) {
				// String msgPath = ZkConstant.BASE_NODE + "/" +
				// ZkConstant.MSGINFO_NODE + "/" + msgId;
				// workerService.msgListener.handleDataChange(msgPath,
				// zkClient.readData(msgPath));
				// }
				allAssignInfo.put(hostName, assignInfo);
			}
		}
	}

	// 启动
	public void start() {
		startListener();
		LogUtils.info("任务分派跟踪启动 OK!");
		String time = zkClient.readData(path);
		this.nodeDataChanged(time);
		zkClient.subscribeDataChanges(thisNodeTaskAssignPath, hostAssignListener);
		try {
			nodeChildrenChanged(zkClient.getChildren(path));
			LogUtils.info("恢复集群停止前的分配任务 初始化任务：" + thisNodeTaskAssignPath);
			Object data = zkClient.readData(thisNodeTaskAssignPath);
			hostAssignListener.handleDataChange(thisNodeTaskAssignPath, data);
		} catch (Exception e) {
			LogUtils.warn("读取集群停止前分析的任务异常： " + e.getMessage());
		}
		workerService.startServer();
		started = true;
	}

	public void stop() {
		workerService.stopServer();
		super.stop();
	}

	@Override
	public void nodeChildrenChanged(List<String> childs) {
		synchronized (allAssignInfo) {
			allAssignInfo.clear();
			for (String node : childs) {
				LogUtils.info("获取主机任务列表：" + ZkConstant.BASE_NODE + "/" + ZkConstant.ASSIGN_NODE + "/" + node);
				String data = zkClient.readData(ZkConstant.BASE_NODE + "/" + ZkConstant.ASSIGN_NODE + "/" + node);
				if (data != null && !data.equals("")) {
					List<String> _assignInfo = (List<String>) JSON.parseObject(data.toString(), List.class);
					allAssignInfo.put(node, _assignInfo);
				}
			}
		}
	}

	// 节点数据变化
	public void nodeDataChanged(Object data) {
		if (data != null) {
			readMsgModtime = Convert.toLong(data, 0);
		}
	}

	public void runAssign() {
		if (!DaemonMaster.isMaster)
			return;
		queryTask = new Thread() {
			@Override
			public void run() {
				Utils.sleep(new Random().nextInt(3000));
				synchronized (daemonMaster.getNodeTracker().clusterNodes) {
					LogUtils.debug("查询待分配任务:" + (new Date(readMsgModtime).toLocaleString()));
				}
				while (!daemonMaster.stopped) {
					CollectDAO dao = new CollectDAO();
					try {
						List<Map<String, Object>> list = dao.queryCollectTasks(readMsgModtime);
						if (list != null && list.size() > 0) {
							for (Map<String, Object> map : list) {
								Object mt = map.get("MODIFY_TIME");
								if (mt != null) {
									String MODIFY_TIME = Convert.toString(mt);
									long modDD = CollectDAO.sdf.parse(MODIFY_TIME).getTime();
									if (modDD > readMsgModtime) {
										readMsgModtime = modDD;
									}
								}
							}
							resetModTime(readMsgModtime);
							assignTask(dao, list);
						}
					} catch (Exception e) {
						LogUtils.error("查询任务出错!", e);
					} finally {
						dao.close();
					}
					Utils.sleep(5000);
				}
			}
		};
		queryTask.start();
		LogUtils.info("主节点[" + Config.getHostName() + "]任务分配线程启动 OK!");
	}

	// 分派任务
	public void assignTask(CollectDAO dao, List<Map<String, Object>> taskList) {
		List<String> nodes = daemonMaster.getNodeTracker().getClusterOnlineNodes();
		if (taskList != null && nodes != null) {
			TaskAssign taskAssign = new TaskAssign(this);// 任务分配接口
			Map<String, List<String>> newAssignInfo = null;
			synchronized (allAssignInfo) {
				newAssignInfo = taskAssign.assignChange(taskList, nodes, allAssignInfo);
			}
			// 先更新删除配置
			for (String msgId : taskAssign.delTasks.keySet()) {
				zkClient.delete(ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE + "/" + msgId);
			}
			Map<String, MsgPO> msgMap = dao.getMsgInfos(taskAssign.addTasks.keySet(), taskAssign.modTasks.keySet());
			Map<String, String> delMsgPo = new HashMap<String, String>();// 防止在读取后又删除或修改为无效
			for (String msgId : taskAssign.addTasks.keySet()) {
				try {
					if (!msgMap.containsKey(msgId)) {
						delMsgPo.put(msgId, msgId);
						continue;
					}
					String data = ToolUtil.serialObject(msgMap.get(msgId), serialIsGzip, serialIsUrlCode);
					try {
						zkClient.createPersistent(ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE + "/" + msgId,
								data);
					} catch (Exception e) {
						zkClient.writeData(ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE + "/" + msgId, data);
					}
				} catch (IOException e1) {
					LogUtils.error("序列化对象到字符串异常", e1);
				}
			}
			for (String msgId : taskAssign.modTasks.keySet()) {
				try {
					if (!msgMap.containsKey(msgId)) {
						delMsgPo.put(msgId, msgId);
						continue;
					}
					// 执行节点比较新的配置与ZK上的配置差异
					String data = ToolUtil.serialObject(msgMap.get(msgId), serialIsGzip, serialIsUrlCode);
					zkClient.writeData(ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE + "/" + msgId, data);
				} catch (IOException e1) {
					LogUtils.error("序列化对象到字符串异常", e1);
				}
			}
			// 删除delMsgPo
			for (String msgId : delMsgPo.keySet()) {
				for (String host : newAssignInfo.keySet()) {
					newAssignInfo.get(host).remove(msgId);
				}
				zkClient.delete(ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE + "/" + msgId);
			}
			// 分配完毕，写入zk
			for (String node : newAssignInfo.keySet()) {
				// 判断是否有变化
				List<String> msgs = newAssignInfo.get(node);
				List<String> omsgs = allAssignInfo.get(node);
				boolean isNeedUpdate = false;
				for (String nmsg : msgs) {
					if (omsgs == null || !omsgs.contains(nmsg)) {
						isNeedUpdate = true;
						break;
					}
				}
				if (!isNeedUpdate) {
					for (String omsg : omsgs) {
						if (!msgs.contains(omsg)) {
							isNeedUpdate = true;
							break;
						}
					}
				}
				if (isNeedUpdate) {
					String nodePath = ZkConstant.BASE_NODE + "/" + ZkConstant.ASSIGN_NODE + "/" + node;
					zkClient.writeData(nodePath, JSON.toJSONString(newAssignInfo.get(node)));
				}
			}
			zkClient.writeData(this.path, readMsgModtime + "");
		}
	}

	public void resetModTime(long readMsgModtime) {
		this.readMsgModtime = readMsgModtime;
		zkClient.writeData(this.path, readMsgModtime + "");
	}

	public List<String> getAssignInfo() {
		if (assignInfo == null) {
			try {
				Object data = zkClient.readData(thisNodeTaskAssignPath);
				hostAssignListener.handleDataChange(thisNodeTaskAssignPath, data);
				return assignInfo;
			} catch (Exception e) {
				LogUtils.warn("读取集群停止前分析的任务异常： " + e.getMessage());
				assignInfo = allAssignInfo.get(hostName);
			}
		}
		return assignInfo;
	}

	public Map<String, List<String>> getAllAssignInfo() {
		Map<String, List<String>> _allAssignInfo = new HashMap<String, List<String>>();
		for (String host : allAssignInfo.keySet()) {
			_allAssignInfo.put(host, new ArrayList<String>(allAssignInfo.get(host)));
		}
		return _allAssignInfo;
	}
}
