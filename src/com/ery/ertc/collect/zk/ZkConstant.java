package com.ery.ertc.collect.zk;

import com.ery.ertc.collect.conf.Config;

public class ZkConstant {

	// 节点目录名
	public static final String BASE_NODE = "/" + Config.getZkBaseZone();// 跟节点
	public static final String MASTER_NODE = "master";// 临时节点，master节点信息
	// 子节点:主机ID ，集群主机所有活动节点ID 临时
	public static final String HOST_NODE = "hosts";
	// 任务分配信息 子节点：主机 数据任务msgId，任务分派情况（每个节点包含的任务）
	public static final String ASSIGN_NODE = "assign";
	// 子节点：任务msgId/主机节点 任务执行情况（统计信息）
	public static final String TASKINFO_NODE = "taskInfo";
	// 子节点：任务msgId
	public static final String MSGINFO_NODE = "msgs";// 任务配置数据

	// zk信息json，key
	public static final String NODE_INFO_COLLECT_ID = "collectId";
	public static final String NODE_INFO_HOST_NAME = "hostName";
	public static final String NODE_START_TIME = "startTime";
	public static final String NODE_HTTP_PORT = "httpPort";// 如果为0，则表示未启动成功
	public static final String NODE_WS_PORT = "wsPort";
	public static final String NODE_SOCKET_PORT = "socketPort";
	public static final String NODE_UI_PORT = "uiPort";
	public static final String NODE_STOP_FLAG = "stop";

}
