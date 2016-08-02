package com.ery.ertc.estorm.po;

import java.io.Serializable;

public class POConstant implements Serializable {

	/**
	 * 数据源类型
	 */
	public final static int DS_TYPE_ORACLE = 1;
	public final static int DS_TYPE_MYSQL = 2;
	public final static int DS_TYPE_HBASE = 3;
	public final static int DS_TYPE_FTP = 4;
	public final static int DS_TYPE_HDFS = 5;

	/**
	 * 消息触发类型 主动类，即是本系统作为客户端访问外部服务端 客户端，本系统作为服务端给外部提供服务接口
	 */
	public final static int TRI_TYPE_I_WS = 10;// 主动WS
	public final static int TRI_TYPE_I_SOCKET = 11;// 主动SOCKET
	public final static int TRI_TYPE_I_HTTP = 12;// 主动HTTP
	public final static int TRI_TYPE_I_ZK = 13;// 主动ZK
	public final static int TRI_TYPE_I_SQL = 14;// 主动SQL
	public final static int TRI_TYPE_I_CLASS = 15;// 主动实现
	public final static int TRI_TYPE_I_FILE = 16;// 主动文件
	public final static int TRI_TYPE_I_HBASE = 17;// 主动Hbase

	public final static int TRI_TYPE_P_WS = 20;// 被动WS
	public final static int TRI_TYPE_P_SOCKET = 21;// 被动SOCKET
	public final static int TRI_TYPE_P_HTTP = 22;// 被动HTTP

	/**
	 * 消息发送类型
	 */
	public final static int SEND_TARGET_KAFKA = 0;// 发送到kafka
	public final static int SEND_TARGET_HBASE = 1;// 发送到hbase
	public final static int SEND_TARGET_HDFS = 2;// 发送到hdfs
	public final static int SEND_TARGET_RDBMS = 3;// 发送到rdbms

	/**
	 * 节点输入类型
	 */
	public final static int INPUT_TYPE_QUEUE = 0;// 消息队列（生成spout节点）
	public final static int INPUT_TYPE_NODE = 1;// 节点（生成bolt节点）

	/**
	 * 订阅类型
	 */
	public final static int PUSH_TYPE_WS = 0;
	public final static int PUSH_TYPE_SOCKET = 1;
	public final static int PUSH_TYPE_HTTP = 2;
	public final static int PUSH_TYPE_ZK = 3;

	/**
	 * 关联表数据刷新类型
	 */
	public final static int FLASH_TYPE_CYCLE = 1;// 周期
	public final static int FLASH_TYPE_EVENT = 2;// 事件

	/**
	 * 节点打印日志级别
	 */
	public final static int NODE_LOG_LEVEL_NONE = 0;// 不记录
	public final static int NODE_LOG_LEVEL_ERROR = 1;// 记录异常
	public final static int NODE_LOG_LEVEL_DETAIL = 2;// 记录明细

	/*** ===下面是一些PO内置JSON对象map的key=== ***/

	// 请求参数定义
	public final static String Msg_REQ_PARAM_charset = "charset";// 编码
	public final static String Msg_REQ_PARAM_period = "period";// 频率
	public final static String Msg_REQ_PARAM_timeout = "timeout";// 频率
	public final static String Msg_REQ_PARAM_sendContent = "sendContent";// 内容

	public final static String Msg_REQ_PARAM_httpContentKey = "contentKey";// 内容key
	public final static String Msg_REQ_PARAM_wsSendXMLTemp = "sendXMLTemp";// xml模板
	public final static String Msg_REQ_PARAM_fileFileName = "fileName";//
	public final static String Msg_REQ_PARAM_fileFtpType = "ftpType";//
	public final static String Msg_REQ_PARAM_hbaseConnJSON = "connJSON";//
	public final static String Msg_REQ_PARAM_hbaseConnXML = "connXML";//
	public final static String Msg_REQ_PARAM_hbaseTableName = "tableName";//
	public final static String Msg_REQ_PARAM_hbaseColFamily = "colFamily";//
	public final static String Msg_REQ_PARAM_hbaseStartRowKey = "startRowKey";//
	public final static String Msg_REQ_PARAM_classJarFile = "jarFile";//
	public final static String Msg_REQ_PARAM_classJavaCode = "javaCode";//
	public final static String Msg_REQ_PARAM_classClassName = "className";//
	public final static String Msg_REQ_PARAM_classMethodName = "methodName";// 方法名
	public final static String Msg_REQ_PARAM_classArgs = "args";// 参数
	public final static String Msg_REQ_PARAM_classIsNotPoll = "isNotPoll";// 是否不轮询
	public final static String Msg_REQ_PARAM_zkRoot = "zkRoot";
	public final static String Msg_REQ_PARAM_zkConnectOMS = "zkConnectOMS";
	public final static String Msg_REQ_PARAM_zkSessionOMS = "zkSessionOMS";
	public final static String Msg_REQ_PARAM_socketMaxConnNum = "socketMaxConnNum";// socket服务最大连接数

	// 返回数据定义
	public final static String Msg_DATA_SCHEME_dataType = "dataType";// 数据类型text,jsonArray，jsonMap
	public final static String Msg_DATA_SCHEME_splitCh = "splitCh";// 数据拆分符
	public final static String Msg_DATA_SCHEME_charset = "charset";// 编码
	public final static String Msg_DATA_SCHEME_isBatch = "isBatch";// 是否批次
	public final static String Msg_DATA_SCHEME_allType = "allType";// 如果为批次，整体数据类型
	public final static String Msg_DATA_SCHEME_itemSplitCh = "itemSplitCh";// 整理数据类型为text时拆分符
	public final static String Msg_DATA_SCHEME_newItemStartCh = "newItemStartCh";// 新行识别符,可带日期宏
	public final static String Msg_DATA_SCHEME_xmlDataNodePath = "dataNodePath";// ws或html请求返回xml，数据节点path
	public final static String Msg_DATA_SCHEME_contentType = "contentType";// 返回数据内容类型，纯文本或xml代码,或二进制
	public final static String Msg_DATA_SCHEME_headerSize = "headerSize";// socket流式数据时，每个消息头标示消息长度的字节大小

}
