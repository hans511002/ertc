package com.ery.ertc.collect;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;

import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.exception.CollectException;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.log.CollectLog;
import com.ery.ertc.collect.task.TaskAssignTracker;
import com.ery.ertc.collect.ui.UIServer;
import com.ery.ertc.collect.worker.WorkerService;
import com.ery.ertc.collect.zk.NodeTracker;
import com.ery.ertc.collect.zk.StringZkSerializer;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.DataSourceManager;
import com.ery.base.support.utils.Utils;

public class DaemonMaster {
	public static boolean isMaster = false;// 是否主节点
	public boolean stopped;// 是否停止
	private ZkClient zkClient;// zk客户端
	private NodeTracker nodeTracker;// 集群节点跟踪
	private TaskAssignTracker taskAssignTracker;// 任务信息监控
	private UIServer uiServer;// Http服务

	public final QueueUtils queueUtils;
	public final WorkerService workerService;

	public static DaemonMaster daemonMaster;

	public DaemonMaster() {
		stopped = false;
		this.zkClient = new ZkClient(Config.getZkUrl(), Config.getZkSessionTOMS(), Config.getZkConnectTOMS(),
				new StringZkSerializer());
		nodeTracker = new NodeTracker(zkClient, this);
		if (Config.getUiEnable()) {
			uiServer = new UIServer(this);
		} else {
			NodeTracker.noticeTryNetServer(ZkConstant.NODE_UI_PORT, "关闭");
		}
		workerService = new WorkerService(zkClient, this);
		taskAssignTracker = new TaskAssignTracker(zkClient, this);
		queueUtils = new QueueUtils(this);
		daemonMaster = this;
	}

	/**
	 * 初始 zk各种目录
	 */
	public void initZkRoot() {
		// 创建Zk根目录
		if (!zkClient.exists(ZkConstant.BASE_NODE)) {
			zkClient.createPersistent(ZkConstant.BASE_NODE);
		}
		// 创建节点目录
		if (!zkClient.exists(ZkConstant.BASE_NODE + "/" + ZkConstant.HOST_NODE)) {
			zkClient.createPersistent(ZkConstant.BASE_NODE + "/" + ZkConstant.HOST_NODE);
		}
		// 任务节点目录
		if (!zkClient.exists(ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE)) {
			zkClient.createPersistent(ZkConstant.BASE_NODE + "/" + ZkConstant.MSGINFO_NODE);
		}
		// 创建工作分配目录
		if (!zkClient.exists(ZkConstant.BASE_NODE + "/" + ZkConstant.ASSIGN_NODE)) {
			zkClient.createPersistent(ZkConstant.BASE_NODE + "/" + ZkConstant.ASSIGN_NODE);
		}
		// 创建任务运行数据监控节点
		if (!zkClient.exists(ZkConstant.BASE_NODE + "/" + ZkConstant.TASKINFO_NODE)) {
			zkClient.createPersistent(ZkConstant.BASE_NODE + "/" + ZkConstant.TASKINFO_NODE);
		}
	}

	/**
	 * 监听连接
	 */
	public void listenerState() {
		zkClient.subscribeStateChanges(new IZkStateListener() {
			@Override
			public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
				LogUtils.debug("连接状态改变:" + keeperState.name());
			}

			@Override
			public void handleNewSession() throws Exception {
				LogUtils.debug("节点连接新开了会话");
				nodeTracker.registerNodeToZk();
			}

			@Override
			public void handleSessionEstablishmentError(Throwable throwable) throws Exception {
				LogUtils.info("建立会话出错!" + throwable.getMessage(), throwable);
			}
		});
	}

	/**
	 * 启动
	 */
	public void start() {
		LogUtils.info("节点[" + Config.getHostName() + "]开始启动");
		try {
			LogUtils.info("初始化数据源连接");
			DataSourceManager.dataSourceInit();
		} catch (Exception e) {
			LogUtils.error("初始数据源错误!", e);
			throw new CollectException(e);
		}
		LogUtils.info("初始化检查ZK节点");
		// 初始ZK节点
		initZkRoot();
		LogUtils.info("ZK连接监听");
		// ZK连接监听
		listenerState();
		if (uiServer != null) {
			LogUtils.info("启动UI");
			uiServer.start();// ui监控
		}
		LogUtils.info("启动节点任务监听");
		taskAssignTracker.start();// 节点任务跟踪
		LogUtils.info("启动节点监听");
		nodeTracker.start();// 节点跟踪,master注册
		nodeTracker.registerMaster();
		LogUtils.info("主线程轮询守护中……");

		loop();// 循环守护进程
	}

	boolean stopSuccess = false;

	private void loop() {
		// 未停止或者还有活动队列

		// // 创建master目录
		// if (!zkClient.exists(ZkConstant.BASE_NODE + "/" +
		// ZkConstant.MASTER_NODE)) {
		// zkClient.createPersistent(ZkConstant.BASE_NODE + "/" +
		// ZkConstant.MASTER_NODE);
		// }

		while (!stopSuccess || this.queueUtils.hasActiveCacheQueue(stopped)) {
			Utils.sleep(500);
		}
		// 停止各线程，关闭连接，数据持久化磁盘等
		LogUtils.info("本节点[" + nodeTracker.getHostName() + "]主线程停止,程序退出！");
		Utils.sleep(500);
		System.exit(1);
	}

	/**
	 * 停止一些线程，关闭zk连接
	 */
	public void stop() {
		stopped = true;
		taskAssignTracker.stop();
		nodeTracker.stop();
		if (uiServer != null) {
			uiServer.stop();
		}
		CollectLog.destory();
		zkClient.unsubscribeAll();
		zkClient.close();
		Utils.sleep(5000);
		stopSuccess = true;
	}

	public NodeTracker getNodeTracker() {
		return nodeTracker;
	}

	public TaskAssignTracker getTaskAssignTracker() {
		return taskAssignTracker;
	}

	public UIServer getUiServer() {
		return uiServer;
	}

	public ZkClient getZkClient() {
		return zkClient;
	}
}
