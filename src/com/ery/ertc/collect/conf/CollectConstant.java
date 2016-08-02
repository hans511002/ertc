package com.ery.ertc.collect.conf;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.codehaus.jackson.map.ObjectMapper;

public class CollectConstant {

	// 必选配置项
	public static final String HOST_ID_KEY = "collect.id";// 集群唯一ID
	public static final String ZK_CONNECT_KEY = "zk.connect";// zk连接
	public static final String ZK_BASE_ZONE_KEY = "zk.base.node";
	public static final String KAFKA_BROKER_ADDR_KEY = "kafka.broker.addr";

	// 可选配置项
	public static final String HOST_NAME_KEY = "host.name";// 集群当前机器IP
	public static final String UI_ENABLE_KEY = "ui.enable";// 是否提供ui服务
	public static final String UI_PORT_KEY = "ui.port";// ui端口http://host_name:port
	public static final String SERVER_HTTP_PORT_KEY = "collect.server.http.port"; // http服务端口
	public static final String SERVER_WS_PORT_KEY = "collect.server.ws.port";// ws服务端口
	public static final String SERVER_SOCKET_PORT_KEY = "collect.server.socket.port";// socket服务端口

	public static final String KAFKA_ZK_ROOT_KEY = "kafka.zk.root";// kafka在zk上的根节点

	public static final String ZK_CONNECT_TIMEOUT_MS_KEY = "zk.connect.timeout.ms";
	public static final String ZK_SESSION_TIMEOUT_MS_KEY = "zk.session.timeout.ms";

	public static final String SEND_THREADS_KEY = "collect.send.threads";
	public static final String SEND_QUEUE_SIZE_KEY = "collect.send.queue.size";
	public static final String CACHE_MAX_SIZE_KEY = "collect.cache.max.size";
	public static final String CACHE_FILE_SIZE_KEY = "collect.cache.file.size";
	public static final String CACHE_INTERVAL_MS_KEY = "collect.cache.interval.ms";
	public static final String CACHE_DIR_KEY = "collect.cache.dir";

	public static final String DETAIL_LOG_WRITE_INTERVAL_MS_KEY = "detail.log.write.interval.ms";// 明细统计写入间隔
	public static final String ERROR_LOG_WRITE_INTERVAL_MS_KEY = "error.log.write.interval.ms";// 采集异常写入间隔
	public static final String SEND_LOG_WRITE_INTERVAL_MS_KEY = "send.log.write.interval.ms";// 发送异常写入间隔
	public static final String TOTAL_LOG_WRITE_INTERVAL_MS_KEY = "total.log.write.interval.ms";// 总统计写入间隔

	public static final String COLLECT_DETAIL_LOG_SAVED_KEY = "collect.detail.log.saved";// 明细统计写入开关

	public static final String DISTINCT_DIR_KEY = "collect.distinct.dir";// 排重计算，缓存文件目录
	public static final String COLLECT_LOG_PATH_KEY = "collect.log.path";// 采集明细日志目录

	public static ObjectMapper objectMapper = new ObjectMapper();
	public static java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static long utsTiime = new Date(70, 1, 1).getTime();
	public static String utsTiimeString = sdf.format(new Date(70, 0, 1));

	public static Date getNow() {
		return new Date(System.currentTimeMillis());
	}

	public static String getNowString() {
		return sdf.format(new Date(System.currentTimeMillis()));
	}
}
