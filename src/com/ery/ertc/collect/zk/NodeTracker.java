package com.ery.ertc.collect.zk;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.Config;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;

public class NodeTracker extends ZooKeeperNodeTracker {
	public String hostTaskAssignPath;
	private String hostTaskInfoPath;
	private String hostPath;
	private String hostName;
	private String masterHost;
	public final Map<String, Boolean> clusterNodes;
	private String startTime;// 节点进入时间
	private IZkDataListener selfNodeListener;// 节点数据变化监听
	final String masterPath;

	private static Map<String, Object> netServerInfo = new HashMap<String, Object>();

	/**
	 * Constructs a new ZK node tracker.
	 * 
	 * @param zkClient
	 * @param daemonMaster
	 */
	public NodeTracker(ZkClient zkClient, DaemonMaster daemonMaster) {
		super(ZkConstant.HOST_NODE, zkClient, daemonMaster);
		hostName = Config.getHostName();
		clusterNodes = new HashMap<String, Boolean>();
		startTime = Convert.toTimeStr(new Date(), Convert.DATE_DEFAULT_FMT);
		hostPath = this.path + "/" + hostName;
		hostTaskInfoPath = ZkConstant.BASE_NODE + "/" + ZkConstant.TASKINFO_NODE + "/" + hostName;
		hostTaskAssignPath = ZkConstant.BASE_NODE + "/" + ZkConstant.ASSIGN_NODE + "/" + hostName;
		masterPath = ZkConstant.BASE_NODE + "/" + ZkConstant.MASTER_NODE;
		enableListener(true, false);// 监听节点
		LogUtils.info("本机hostName:" + hostName);
	}

	// 通知通信服务尝试启动OK
	public static void noticeTryNetServer(String key, Object o) {
		synchronized (netServerInfo) {
			netServerInfo.put(key, o);
			DaemonMaster.daemonMaster.getNodeTracker().createOrUpdateNodeInfo();
		}
	}

	class MasterListener implements IZkDataListener {
		@Override
		public void handleDataDeleted(String dataPath) {
			if (masterPath.equals(dataPath)) {
				while (true) {
					try {
						DaemonMaster.isMaster = false;
						zkClient.createEphemeral(masterPath, hostName);
						DaemonMaster.isMaster = true;
						// 启动分配线程
						daemonMaster.getTaskAssignTracker().runAssign();
					} catch (Exception e) {
						if (zkClient.exists(masterPath)) {
							masterHost = zkClient.readData(masterPath).toString();
							break;
						}
					}
				}
			}
		}

		@Override
		public void handleDataChange(String dataPath, Object data) throws Exception {
			masterHost = data.toString();
			if (hostName.equals(masterHost)) {
				DaemonMaster.isMaster = true;
			}
		}
	}

	public void start() {
		// 监听master
		startListener();
		registerNodeToZk();
		LogUtils.info("节点跟踪启动 OK!");
	}

	public void registerMaster() {
		MasterListener mastListener = new MasterListener();
		zkClient.subscribeDataChanges(masterPath, mastListener);
		// 注册master
		mastListener.handleDataDeleted(masterPath);
		LogUtils.info("当前Master节点[" + masterHost + "]");

	}

	public void stop() {
		super.stop();
		unRegisterNodeToZk();
	}

	// 取消节点在zk上的注册
	public void unRegisterNodeToZk() {
		try {
			zkClient.delete(hostPath);
			if (masterHost.equals(hostName)) {
				zkClient.delete(masterPath);
			}
			zkClient.delete(hostPath);
			// zkClient.delete(hostTaskPath);
			// zkClient.delete(hostTaskAssignPath);

			LogUtils.info("删除本节点[" + hostName + "]ZK注册!");
		} catch (Exception e) {
			LogUtils.warn("删除节点注册出错!" + e.getMessage(), e);
		}
	}

	void createOrUpdateNodeInfo() {
		Map<String, Object> nodeInfo = new HashMap<String, Object>();
		nodeInfo.put(ZkConstant.NODE_INFO_HOST_NAME, hostName);
		nodeInfo.put(ZkConstant.NODE_START_TIME, startTime);
		nodeInfo.put(ZkConstant.NODE_HTTP_PORT, netServerInfo.get(ZkConstant.NODE_HTTP_PORT));
		nodeInfo.put(ZkConstant.NODE_WS_PORT, netServerInfo.get(ZkConstant.NODE_WS_PORT));
		nodeInfo.put(ZkConstant.NODE_SOCKET_PORT, netServerInfo.get(ZkConstant.NODE_SOCKET_PORT));
		nodeInfo.put(ZkConstant.NODE_UI_PORT, netServerInfo.get(ZkConstant.NODE_UI_PORT));
		if (zkClient.exists(hostPath)) {
			zkClient.writeData(hostPath, JSON.toJSONString(nodeInfo));
		} else {
			zkClient.createEphemeral(hostPath, JSON.toJSONString(nodeInfo));
		}
	}

	/**
	 * 注册节点信息到ZK集群
	 */
	public synchronized void registerNodeToZk() {
		if (started)
			return;
		LogUtils.info("注册主机节点：" + hostPath);
		if (zkClient.exists(this.hostPath)) {
			zkClient.deleteRecursive(hostPath);
		}
		createOrUpdateNodeInfo();
		if (selfNodeListener != null) {
			try {
				zkClient.unsubscribeDataChanges(hostPath, selfNodeListener);
			} catch (Exception e) {
				LogUtils.warn(null, e);
			}
		}
		selfNodeListener = new IZkDataListener() {
			@Override
			public void handleDataChange(String s, Object o) throws Exception {
				// 取出信息，看是否有停止命令，如果有，则让本机程序停止
				Map<String, Object> clusterNodeInfo = JSON.parseObject(o.toString());
				int flag = Convert.toInt(clusterNodeInfo.get(ZkConstant.NODE_STOP_FLAG), 0);
				if (flag == 1) {
					daemonMaster.stop();
				}
			}

			@Override
			public void handleDataDeleted(String s) throws Exception {
				LogUtils.warn("节点[" + s + "]数据被删除!");// 临时节点是否能触发此事件（未知)
				if (!daemonMaster.stopped) {
					NodeTracker.this.registerNodeToZk();
				}
			}
		};
		// 监听节点数据标识，是否有停止命令
		zkClient.subscribeDataChanges(hostPath, selfNodeListener);

		if (!zkClient.exists(hostTaskAssignPath))
			zkClient.createPersistent(hostTaskAssignPath);
		started = true;
	}

	@Override
	public void nodeChildrenChanged(List<String> childs) {
		synchronized (clusterNodes) {
			if (childs.size() > 0) { // 重新分析平衡
				for (String string : childs) {
					if (!clusterNodes.containsKey(string)) {// 新增
						clusterNodes.put(string, true);//
						this.daemonMaster.getTaskAssignTracker().resetModTime(0);// 全部重新读取分配
						continue;
					}
				}
				for (String node : clusterNodes.keySet()) {
					if (!childs.contains(node)) {// 删除
						clusterNodes.put(node, false);// 置宕机状态
						this.daemonMaster.getTaskAssignTracker().resetModTime(0);
						continue;
					}
				}
			}
		}
	}

	public String getHostName() {
		return hostName;
	}

	public String getMasterName() {
		return masterHost;
	}

	public void getNodes() {
		List<String> children = zkClient.getChildren(path);
		nodeChildrenChanged(children);
	}

	public List<String> getClusterOnlineNodes() {
		getNodes();
		List<String> _clusterNodes = new ArrayList<String>();
		if (clusterNodes != null) {
			synchronized (clusterNodes) {
				for (String node : clusterNodes.keySet()) {
					if (clusterNodes.get(node))
						_clusterNodes.add(node);
				}
			}
		}
		return _clusterNodes;
	}

	public Map<String, Boolean> getClusterNodes() {
		getNodes();
		Map<String, Boolean> _clusterNodes = new HashMap<String, Boolean>();
		if (clusterNodes != null) {
			synchronized (clusterNodes) {
				for (String node : clusterNodes.keySet()) {
					_clusterNodes.put(node, clusterNodes.get(node));
				}
			}
		}
		return _clusterNodes;
	}

}
