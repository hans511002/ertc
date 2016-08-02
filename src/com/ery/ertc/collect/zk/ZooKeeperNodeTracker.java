package com.ery.ertc.collect.zk;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.StringUtils;

public abstract class ZooKeeperNodeTracker {

	/** Path of node being tracked */
	protected String path;
	protected ZkClient zkClient;
	protected DaemonMaster daemonMaster;
	protected boolean listenerChild = true;// 是否监听数据
	private IZkChildListener zkChildListener;
	protected boolean listenerData = true;// 是否监听数据
	private IZkDataListener zkDataListener;
	protected boolean started = false;// 是否已经启动监听

	/**
	 * Constructs a new ZK node tracker.
	 * 
	 * @param node
	 * @param zkClient
	 * @param daemonMaster
	 */
	public ZooKeeperNodeTracker(String node, ZkClient zkClient, DaemonMaster daemonMaster) {
		this.zkClient = zkClient;
		this.path = ZkConstant.BASE_NODE + "/" + node;
		this.daemonMaster = daemonMaster;
	}

	/**
	 * 确定哪些需要监听
	 * 
	 * @param child
	 *            是否监听子节点变更
	 * @param data
	 *            是否监听数据
	 */
	public void enableListener(boolean child, boolean data) {
		listenerChild = child;
		listenerData = data;
	}

	/**
	 * Starts the tracking of the node in ZooKeeper.
	 */
	public synchronized void startListener() {
		if (!started) {
			if (listenerChild) {
				zkChildListener = new IZkChildListener() {
					String str = null;

					@Override
					public void handleChildChange(String s, List<String> strings) throws Exception {
						if (!path.equals(s)) {
							return;
						}
						if (strings != null && strings.size() > 0) {
							String _s = StringUtils.join(strings, ",");
							if (!_s.equals(str)) {
								str = _s;
								LogUtils.debug("子节点变更:" + s + "-->" + str);
							}
							nodeChildrenChanged(strings);
						} else {
							LogUtils.debug("节点删除:" + s);
							daemonMaster.initZkRoot();
							nodeDeleted();
						}
					}
				};
				zkClient.subscribeChildChanges(path, zkChildListener);
			}

			if (listenerData) {
				zkDataListener = new IZkDataListener() {
					String str = null;

					@Override
					public void handleDataChange(String s, Object o) throws Exception {
						if (!path.equals(s)) {
							return;
						}
						String _s = "" + o;
						if (!_s.equals(str)) {
							str = _s;
							LogUtils.debug("[" + s + "]数据发生变化:--->" + str);
							nodeDataChanged(o);
						}
					}

					@Override
					public void handleDataDeleted(String s) throws Exception {
						if (!path.equals(s)) {
							return;
						}
						LogUtils.debug("[" + s + "]数据删除:");
						daemonMaster.initZkRoot();
						nodeDataDeleted();
					}
				};
				zkClient.subscribeDataChanges(path, zkDataListener);
			}
		}
	}

	public synchronized void stopListener() {
		if (listenerChild) {
			zkClient.unsubscribeChildChanges(path, zkChildListener);
		}
		if (listenerData) {
			zkClient.unsubscribeDataChanges(path, zkDataListener);
		}
		started = false;
	}

	public abstract void start();

	public void stop() {
		stopListener();
		started = false;
		LogUtils.info(this.getClass().getSimpleName() + "停止 OK!");
	}

	public boolean isStarted() {
		return started;
	}

	// 子节点变更
	public void nodeChildrenChanged(List<String> childs) {
	}

	// 节点删除
	public void nodeDeleted() {
	}

	// 节点数据变更
	public void nodeDataChanged(Object data) {
	}

	// 节点数据删除
	public void nodeDataDeleted() {
	}

}
