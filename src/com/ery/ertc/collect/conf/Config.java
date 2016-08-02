package com.ery.ertc.collect.conf;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.ery.ertc.collect.forwards.MsgQueue;
import com.ery.ertc.collect.handler.Distinct;
import com.ery.ertc.collect.log.CollectLog;
import com.ery.base.support.sys.SystemVariable;
import com.ery.base.support.utils.Convert;

public class Config extends SystemVariable {

	public static String getHostName() {
		String hostName = conf.getProperty(CollectConstant.HOST_NAME_KEY);
		if (hostName == null || "".equals(hostName)) {
			hostName = "0.0.0.0";
			// RemotingUtils.getLocalAddress();
			try {
				InetAddress a = java.net.InetAddress.getByName(hostName);
				hostName = a.getCanonicalHostName();
			} catch (UnknownHostException e) {
			}
		}
		return hostName;
	}

	public static String getZkUrl() {
		return conf.getProperty(CollectConstant.ZK_CONNECT_KEY);
	}

	public static String getZkBaseZone() {
		return conf.getProperty(CollectConstant.ZK_BASE_ZONE_KEY);
	}

	public static String getKafkaBrokersUrl() {
		return conf.getProperty(CollectConstant.KAFKA_BROKER_ADDR_KEY);
	}

	public static int getZkConnectTOMS() {
		return Convert.toInt(conf.getProperty(CollectConstant.ZK_CONNECT_TIMEOUT_MS_KEY), 30000);
	}

	public static int getZkSessionTOMS() {
		return Convert.toInt(conf.getProperty(CollectConstant.ZK_SESSION_TIMEOUT_MS_KEY), 7000);
	}

	public static boolean getUiEnable() {
		return Convert.toBool(conf.getProperty(CollectConstant.UI_ENABLE_KEY), true);
	}

	public static int getUiPort() {
		return Convert.toInt(conf.getProperty(CollectConstant.UI_PORT_KEY), 7891);
	}

	public static int getServerHttpPort() {
		return Convert.toInt(conf.getProperty(CollectConstant.SERVER_HTTP_PORT_KEY), 7892);
	}

	public static int getServerWSPort() {
		return Convert.toInt(conf.getProperty(CollectConstant.SERVER_WS_PORT_KEY), 7893);
	}

	public static String getKafkaZkRoot() {
		return conf.getProperty(CollectConstant.KAFKA_ZK_ROOT_KEY, "/kafka");
	}

	public static int getServerSocketPort() {
		return Convert.toInt(conf.getProperty(CollectConstant.SERVER_SOCKET_PORT_KEY), 7894);
	}

	public static String getCacheDir() {
		return Convert.toString(conf.getProperty(CollectConstant.CACHE_DIR_KEY), MsgQueue.CACHE_DIR);
	}

	public static int getSendThreadNum() {
		return Convert.toInt(conf.getProperty(CollectConstant.SEND_THREADS_KEY), MsgQueue.DEFAULT_SEND_POOL_SIZE);
	}

	public static int getSendQueueSize() {
		return Convert.toInt(conf.getProperty(CollectConstant.SEND_QUEUE_SIZE_KEY), MsgQueue.DEFAULT_SEND_QUEUE_SIZE);
	}

	public static int getCacheMaxSize() {
		return Convert.toInt(conf.getProperty(CollectConstant.CACHE_MAX_SIZE_KEY), MsgQueue.DEFAULT_CACHE_MAX_SIZE);
	}

	public static int getCacheFileSize() {
		return Convert.toInt(conf.getProperty(CollectConstant.CACHE_FILE_SIZE_KEY), MsgQueue.DEFAULT_CACHE_MAX_SIZE);
	}

	public static int getCacheIntervalMs() {
		return Convert.toInt(conf.getProperty(CollectConstant.CACHE_INTERVAL_MS_KEY),
				MsgQueue.DEFAULT_CACHE_INTERVAL_MS);
	}

	public static int getDetailLogWriteIntervalMs() {
		return Convert.toInt(conf.getProperty(CollectConstant.DETAIL_LOG_WRITE_INTERVAL_MS_KEY), 6000);
	}

	public static int getErrorLogWriteIntervalMs() {
		return Convert.toInt(conf.getProperty(CollectConstant.ERROR_LOG_WRITE_INTERVAL_MS_KEY), 6000);
	}

	public static int getSendLogWriteIntervalMs() {
		return Convert.toInt(conf.getProperty(CollectConstant.SEND_LOG_WRITE_INTERVAL_MS_KEY), 6000);
	}

	public static int getTotalLogWriteIntervalMs() {
		return Convert.toInt(conf.getProperty(CollectConstant.TOTAL_LOG_WRITE_INTERVAL_MS_KEY), 6000);
	}

	public static boolean getCollectDetailLogSaveFlag() {
		return Convert.toBool(conf.getProperty(CollectConstant.COLLECT_DETAIL_LOG_SAVED_KEY), false);
	}

	public static String getDistinctDir() {
		return conf.getProperty(CollectConstant.DISTINCT_DIR_KEY, Distinct.DISTINCT_DIR);
	}

	public static String getCollectLogDir() {
		return conf.getProperty(CollectConstant.COLLECT_LOG_PATH_KEY, CollectLog.LOG_PATH);
	}
}
