package com.ery.ertc.estorm.po;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

public class MsgPO implements Serializable {
	private static final long serialVersionUID = -602758985824774409L;
	private long MSG_ID;// 消息ID
	private String MSG_NAME;// 消息名

	private int TRIGGER_TYPE;// 消息触发类型。对应 【POConstant.TRI_TYPE_?】

	/**
	 * <pre>
	 * 主动才配置
	 * 	由ID、端口、域名、目录或某路径组成
	 *  	http ——url【http://www.baidu.com 或 http://x.x.x.x:y/z】
	 * 		socket ——ip:port【x.x.x.x:y】 
	 * 		ws ——wsdl地址【{http_url}?wsdl】
	 *  	sql ——无 
	 *  	file  ——目标机器ip和ftp端口【x.x.x.x:y】 
	 * 		hbase ——无
	 * 		class ——无
	 * 		zk ——zk地址端口，多个用逗号分割【x.x.x.x:y,x.x.x.x:y,……】
	 * </pre>
	 */
	private String URL;
	private String USER;// 用户[暂时设计为只有SQL、FILE用到]
	private String PASS;// 密码
	/**
	 * <pre>
	 * 请求参数 JSON 
	 * 通用: period:频率 timeout:60 charset:utf-8 
	 * HTTP: 
	 * 	contentKey:content——根据此参数值获取内容，多个用逗号分割 
	 * 	sendContent:内容,json数组，数组大小必须与key个数对应 
	 * SOCKET：
	 * 		sendContent:发送内容 
	 * 		socketMaxConnNum:被动socket（最大连接数）
	 *  WS: 
	 *  	sendXMLTemp:发送xml模板
	 * SQL： 
	 * FILE： 
	 * 		ftpType:传输类型 
	 * 		fileName：文件名路径(文件名可带时间宏变量【${YYYY},${YY},${MM},${DD},${h},${m},${s}】,目录分割符请用'/',可带*通配符)
	 * 
	 * HBASE： 
	 * 		connJSON：连接信息json格式
	 * 		connXML：连接信息XML格式 
	 * 		tableName：表名 
	 * 		colFamily：列簇
	 * 		startRowKey：rowkey规则 
	 * CLASS： 
	 * 		jarFile:jar包路径 
	 * 		javaCode:java代码，与jarFile 2选1
	 * 		className:类名,可实现指定接口(com.ery.ertc.collect.remote.client.CollectPlugin)
	 * 		methodName:方法名,未实现接口时指定 args:参数，多个用逗号分割 isNotPoll：是否不是轮询（0轮询，1不轮询） 
	 * ZK：
	 * 		zkRoot:zook根目录 
	 * 		zkConnectOMS:连接超时时间 
	 * 		zkSessionOMS:会话超时时间
	 * </pre>
	 */
	private String REQ_PARAM;

	/**
	 * <pre>
	 * 获取到的数据格式，根据triggerType不同而不同 
	 * 通用：json串(sql除外) 
	 * 	dataType:text，array，map
	 * 	splitCh：拆分符(text特有) 
	 * 	charset:utf-8 
	 * 	contentType:text,xml,binary 【字符串，xml串，二进制数据】
	 * 	WS： 
	 * 		isBatch：是否批次(0否，一个节点代表一条记录；1是一个节点代表多条记录）
	 * 		dataNodePath:值path（ws请求返回的数据为xml串，需要从中解析需要的数据）
	 * 		allType：text，array。批次数据时，数据结构是jsonArray或字符join
	 * 		itemSplitCh：记录拆分符(isBatch为0且allType为text特有，需要先将多条记录拆分出来)
	 * 		newItemStartCh：新行识别符（与拆分符2选1），可以是日期格式串 SOCKET:
	 * 		isBatch:是否批次(0流式；1接收到的消息是一个整体共包含N个消息)
	 * 		headerSize:头大小（标示接下来的消息长度）——连续的消息字节流，头-消息-头-消息-头-消息…… allType:text,array
	 * 		itemSplitCh：记录拆分符(批次且allType为text特有)
	 * 		newItemStartCh：新行识别符（与拆分符2选1），可以是日期格式串 HTTP:
	 * 		contentType：内容类型【text,html,binary】纯文本，http代码，二进制流
	 * 		headerSize:二进制流时头大小（标示接下来的消息长度）——连续的消息字节流，头-消息-头-消息-头-消息……
	 * 		dataNodePath；值path isBatch：是否批次 allType：text，array
	 * 		itemSplitCh：记录拆分符(allType为text特有) newItemStartCh：新行识别符（与拆分符2选1），可以是日期格式串
	 * 
	 * 	SQL：sql语句 
	 * 	ZK： 
	 * 		isBatch：是否批次 
	 * 		allType:text,array
	 * 		itemSplitCh：记录拆分符(批次且allType为text特有)
	 * 		newItemStartCh：新行识别符（与拆分符2选1），可以是日期格式串 【${YYYY},${YY},${MM},${DD},${h},${m},${s},${sss},${mmm},${eee},${z}】
	 * 		FILE：allType默认就是text了，省略 
	 * 		itemSplitCh：记录拆分符号（\n时直接按行读取,否则按字节读取，读出之后再解析）.
	 * 		newItemStartCh：新行识别符（与拆分符2选1），可以是日期格式串 HBASE：json串
	 * 
	 * CLASS:自己实现，无需关心数据
	 * 
	 * 被动服务类(数据格式相对固定)： 
	 * 	JSON串 
	 * 		dataType:text,array,map ——传入数据格式，数组或map
	 * 		splitCh：拆分符(text特有)
	 * </pre>
	 */
	private String DATA_SCHEME;

	private long DATA_SOURCE_ID;// 数s据源
	private String MSG_TAG_NAME;// 消息标签(一般按业务分,唯一，不可重复)
	private String MSG_DESC;//
	private int MSG_ORDER_FLAG;// 消息是否有序，0无序，1有序 (采集用不到。因为本身客户端消息接入的无序性)
	private String CREATE_DATE;
	private String CREATE_USER;
	private String STATE;//
	private String LAST_COLLECT_POINTER;// 上次采集的点
	private long CACHE_DS;// 队列缓存数据源ID:-1标示本地文件
	private int TODO_SEND_QUEUE_SIZE;// 待发送区域大小

	private List<MsgExtPO> extPOs = new ArrayList<MsgExtPO>();// 扩展参数
	// private MsgQueueParamPO queueParam;// 队列参数
	private List<MsgFieldPO> msgFields;// 消息字段（定义消息字段）

	public long getMSG_ID() {
		return MSG_ID;
	}

	public void setMSG_ID(long MSG_ID) {
		this.MSG_ID = MSG_ID;
	}

	public String getMSG_NAME() {
		return MSG_NAME;
	}

	public void setMSG_NAME(String MSG_NAME) {
		this.MSG_NAME = MSG_NAME;
	}

	public int getTRIGGER_TYPE() {
		return TRIGGER_TYPE;
	}

	public String getTriggerName() {
		switch (TRIGGER_TYPE) {
		case POConstant.TRI_TYPE_I_WS:
			return "主动WS";
		case POConstant.TRI_TYPE_I_SOCKET:
			return "主动SOCKET";
		case POConstant.TRI_TYPE_I_HTTP:
			return "主动HTTP";
		case POConstant.TRI_TYPE_I_ZK:
			return "主动ZK";
		case POConstant.TRI_TYPE_I_SQL:
			return "主动SQL";
		case POConstant.TRI_TYPE_I_CLASS:
			return "主动外部实现";
		case POConstant.TRI_TYPE_I_FILE:
			return "主动文件";
		case POConstant.TRI_TYPE_I_HBASE:
			return "主动Hbase";
		case POConstant.TRI_TYPE_P_WS:
			return "被动WS";
		case POConstant.TRI_TYPE_P_SOCKET:
			return "被动SOCKET";
		case POConstant.TRI_TYPE_P_HTTP:
			return "被动HTTP";

		}
		return "none";
	}

	public void setTRIGGER_TYPE(int TRIGGER_TYPE) {
		this.TRIGGER_TYPE = TRIGGER_TYPE;
	}

	public String getURL() {
		return URL;
	}

	public void setURL(String URL) {
		this.URL = URL;
	}

	public String getUSER() {
		return USER;
	}

	public void setUSER(String USER) {
		this.USER = USER;
	}

	public String getPASS() {
		return PASS;
	}

	public void setPASS(String PASS) {
		this.PASS = PASS;
	}

	public String getREQ_PARAM() {
		return REQ_PARAM;
	}

	public void setREQ_PARAM(String REQ_PARAM) {
		this.REQ_PARAM = REQ_PARAM;
	}

	public String getDATA_SCHEME() {
		return DATA_SCHEME;
	}

	public void setDATA_SCHEME(String DATA_SCHEME) {
		this.DATA_SCHEME = DATA_SCHEME;
	}

	public long getDATA_SOURCE_ID() {
		return DATA_SOURCE_ID;
	}

	public void setDATA_SOURCE_ID(long DATA_SOURCE_ID) {
		this.DATA_SOURCE_ID = DATA_SOURCE_ID;
	}

	public String getMSG_TAG_NAME() {
		return MSG_TAG_NAME;
	}

	public void setMSG_TAG_NAME(String MSG_TAG_NAME) {
		this.MSG_TAG_NAME = MSG_TAG_NAME;
	}

	public String getMSG_DESC() {
		return MSG_DESC;
	}

	public void setMSG_DESC(String MSG_DESC) {
		this.MSG_DESC = MSG_DESC;
	}

	public int getMSG_ORDER_FLAG() {
		return MSG_ORDER_FLAG;
	}

	public void setMSG_ORDER_FLAG(int MSG_ORDER_FLAG) {
		this.MSG_ORDER_FLAG = MSG_ORDER_FLAG;
	}

	public String getCREATE_DATE() {
		return CREATE_DATE;
	}

	public void setCREATE_DATE(String CREATE_DATE) {
		this.CREATE_DATE = CREATE_DATE;
	}

	public String getCREATE_USER() {
		return CREATE_USER;
	}

	public void setCREATE_USER(String CREATE_USER) {
		this.CREATE_USER = CREATE_USER;
	}

	public String getSTATE() {
		return STATE;
	}

	public void setSTATE(String STATE) {
		this.STATE = STATE;
	}

	public String getLAST_COLLECT_POINTER() {
		return LAST_COLLECT_POINTER;
	}

	public void setLAST_COLLECT_POINTER(String LAST_COLLECT_POINTER) {
		this.LAST_COLLECT_POINTER = LAST_COLLECT_POINTER;
	}

	public List<MsgExtPO> getExtPO() {
		return extPOs;
	}

	public void addExtPO(MsgExtPO extPO) {
		if (extPOs == null)
			extPOs = new ArrayList<MsgExtPO>();
		this.extPOs.add(extPO);
	}

	// public MsgQueueParamPO getQueueParam() {
	// return queueParam;
	// }

	// public void setQueueParam(MsgQueueParamPO queueParam) {
	// this.queueParam = queueParam;
	// }

	public List<MsgFieldPO> getMsgFields() {
		if (msgFields == null) {
			msgFields = new ArrayList<MsgFieldPO>();
		}
		return msgFields;
	}

	public void setMsgFields(List<MsgFieldPO> msgFields) {
		this.msgFields = msgFields;
	}

	public long getCACHE_DS() {
		return CACHE_DS;
	}

	public void setCACHE_DS(long CACHE_DS) {
		this.CACHE_DS = CACHE_DS;
	}

	public int getTODO_SEND_QUEUE_SIZE() {
		return TODO_SEND_QUEUE_SIZE;
	}

	public void setTODO_SEND_QUEUE_SIZE(int TODO_SEND_QUEUE_SIZE) {
		this.TODO_SEND_QUEUE_SIZE = TODO_SEND_QUEUE_SIZE;
	}

	/**
	 * 下面的属性，是方便业务计算时的一些预处理
	 */
	private Map<String, Object> paramMap;// 参数map,如果需要
	private Map<String, Object> dataSchemeMap;// 数据格式map，如果需要
	private Map<String, Integer> fieldIdxMapping;// 字段索引映射

	public Map<String, Object> getParamMap() {
		if (paramMap == null && REQ_PARAM != null && !"".equals(REQ_PARAM)) {
			paramMap = JSON.parseObject(REQ_PARAM);
		}
		return paramMap;
	}

	public Map<String, Object> getDataSchemeMap() {
		if (dataSchemeMap == null && DATA_SCHEME != null && !"".equals(DATA_SCHEME)) {
			dataSchemeMap = JSON.parseObject(DATA_SCHEME);
		}
		return dataSchemeMap;
	}

	public Map<String, Integer> getFieldIdxMapping() {
		if (fieldIdxMapping == null && msgFields != null) {
			int i = 0;
			Map<String, Integer> map = new HashMap<String, Integer>();
			map.put("COLLECT_HOSTNAME", i++);
			map.put("COLLECT_LOG_ID", i++);
			for (MsgFieldPO fieldPO : msgFields) {
				map.put(fieldPO.getFIELD_NAME(), i);
				i++;
			}
			fieldIdxMapping = map;
		}
		return fieldIdxMapping;
	}

	/**
	 * 同步消息变更，发现有重要信息发生变更，需要重启则返回true
	 * 
	 * @param oldPo
	 *            原po
	 * @param po
	 *            新po
	 * @return 是否需要重启，true需,false不需
	 */
	public boolean syncMsgInfo(MsgPO newPo) {
		if (this.TRIGGER_TYPE != newPo.TRIGGER_TYPE) {
			return true;
		}
		if (this.msgFields.size() != newPo.msgFields.size()) {
			return true;
		}
		if (this.MSG_TAG_NAME != null && !this.MSG_TAG_NAME.equals(newPo.MSG_TAG_NAME)) {
			return true;
		}
		if (this.REQ_PARAM != null && !this.REQ_PARAM.equals(newPo.REQ_PARAM)) {
			return true;
		}
		if (this.DATA_SCHEME != null && !this.REQ_PARAM.equals(newPo.REQ_PARAM)) {
			return true;
		}
		if (this.URL != null && !this.URL.equals(newPo.URL)) {
			return true;
		}
		if (this.USER != null && !this.USER.equals(newPo.USER)) {
			return true;
		}
		if (this.PASS != null && !this.PASS.equals(newPo.PASS)) {
			return true;
		}
		if (this.MSG_ORDER_FLAG != newPo.MSG_ORDER_FLAG) {
			return true;
		}
		if (this.CACHE_DS != newPo.CACHE_DS)
			return true;

		if (this.extPOs.size() != newPo.extPOs.size())
			return true;
		for (int i = 0; i < this.extPOs.size(); i++) {
			if (this.extPOs.get(i).syncMsgInfo(newPo.extPOs.get(i)))
				return true;
		}
		// 这些信息发生变更，必须重启：采集触发类型，主题，请求参数，解析参数,url,user,pass,有序否,缓存类型,发现线程池大小，发送目标
		List<MsgFieldPO> oldFields = this.getMsgFields();
		List<MsgFieldPO> fields = newPo.getMsgFields();

		for (int i = 0; i < oldFields.size(); i++) {
			MsgFieldPO oldFieldPo = oldFields.get(i);
			MsgFieldPO fieldPO = fields.get(i);
			if (!oldFieldPo.getFIELD_DATA_TYPE().equals(fieldPO.getFIELD_DATA_TYPE()) ||
					!oldFieldPo.getFIELD_NAME().equals(fieldPO.getFIELD_NAME()) ||
					!oldFieldPo.getSRC_FIELD().equals(fieldPO.getSRC_FIELD())) {
				return true;
			} else {
				oldFieldPo.setFIELD_CN_NAME(fieldPO.getFIELD_CN_NAME());
				oldFieldPo.setFIELD_DESC(fieldPO.getFIELD_DESC());
				oldFieldPo.setORDER_ID(fieldPO.getORDER_ID());
			}
		}
		this.setMSG_DESC(newPo.getMSG_DESC());
		this.setMSG_NAME(newPo.getMSG_NAME());
		return false;
	}
}
