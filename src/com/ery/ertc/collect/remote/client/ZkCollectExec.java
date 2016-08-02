package com.ery.ertc.collect.remote.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.collect.zk.StringZkSerializer;
import com.ery.ertc.estorm.po.MsgPO;

public class ZkCollectExec extends CollectExec {

	private ZkClient zkClient;
	private String zkData;

	public ZkCollectExec(AbstractTaskExecutor executor) {
		super(executor);
	}

	@Override
	public void execute() {
		final MsgPO msgPO = executor.getMsgPO();
		String zkurl = msgPO.getURL();
		srcClient = zkurl;
		if (reqZkRoot != null && !"".equals(reqZkRoot)) {
			if (!reqZkRoot.startsWith("/")) {
				reqZkRoot = "/" + reqZkRoot;
			}
			zkClient = new ZkClient(zkurl, reqZkSessionOMS, reqZkConnectOMS, new StringZkSerializer(reqCharset));
			zkClient.subscribeDataChanges(reqZkRoot, new ZKDataListner());
		} else {
			throw new RuntimeException("zkRoot未配置，找不到监控目录");
		}
	}

	class ZKDataListner implements IZkDataListener, IZkChildListener {
		Map<String, Long> childs = new HashMap<String, Long>();

		@Override
		public void handleDataChange(String s, Object o) throws Exception {
			String data = o.toString();
			if (!data.equals(zkData)) {
				zkData = data;
				if (isBatch) {
					String[] contents = QueueUtils.convertDataToContents(zkData, allType, itemSplitCh);
					for (String content : contents) {
						sendMsg(executor.getMsgPO(), content);
					}
				} else {
					sendMsg(executor.getMsgPO(), zkData);
				}
			}
		}

		@Override
		public void handleDataDeleted(String s) throws Exception {
			setStateOrInfo(0, "zk目录[" + s + "]被删除!");
		}

		@Override
		public void handleChildChange(String path, List<String> childs) throws Exception {
			for (String n : childs) {
				if (!this.childs.containsKey(n))
					handleDataChange(path + "/" + n, zkClient.readData(path + "/" + n));
			}
		}
	}

	@Override
	public void stopRun() {
		if (zkClient != null) {
			zkClient.unsubscribeAll();
		}
	}
}
