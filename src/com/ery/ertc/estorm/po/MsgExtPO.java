package com.ery.ertc.estorm.po;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;

public class MsgExtPO implements Serializable {

	long STORE_ID;
	private long MSG_ID;// 消息ID
	private int SEND_THD_POOL_SIZE;// 发送线程池大小
	private int SEND_TARGET;// 发送目标，0,kafak 1,hbase 2,hdfs
	/**
	 * 过滤规则 expr
	 */
	private String FILTER_RULE;// 过滤规则
	/**
	 * 去重规则.设计多种规则类型（多选1），暂实现一种，按字段去重 fields ——直接根据字段值去重。多个字段用逗号分割 otherkey
	 * ——其他规则
	 * 
	 * 其他去重计算资源相关设置（注意，规则类型不可与这些设置key重复）。主要采用“布隆过滤器”算法 timeRange
	 * ——排重时间范围，只针对最近这个时间范围内的数据进行排重。过了这个范围的数据将会从过滤器中删除
	 * 格式:m:180标示180分钟，d:1标示一天，h:3标示3个小时 predictNum ——预期范围内消息数量。此值关系到排重算法分配到的资源。
	 * partFieldIdx ——分区索引，标示被过滤的业务数据需要直接用某个字段的值进行分区 timeFieldIdx
	 * ——时间字段索引，标示被过滤的业务数据包含时间属性
	 * 
	 */
	private String DISTINCT_RULE;// 去重规则
	private int LOG_LEVEL;// 日志级别，0不记录，1打印到log文件，2记录到mysql
	String MSG_STORE_CFG;
	public String STORE_PLUGIN;

	public String getSTORE_PLUGIN() {
		return STORE_PLUGIN;
	}

	public void setSTORE_PLUGIN(String sTORE_PLUGIN) {
		STORE_PLUGIN = sTORE_PLUGIN;
	}

	public String getMsgStoreParam(String key) {
		if (storeConfig != null && storeConfig.containsKey(key))
			return storeConfig.get(key);
		return null;
	}

	public Map<String, String> getStoreConfigParams() {
		if (storeConfig == null) {
			storeConfig = new HashMap<String, String>();
		}
		return storeConfig;
	}

	private String partFieldStrs = null;// 分区字段
	private boolean partKeyIsStr;// 分区类型key是否是字符串

	public boolean getPartKeyIsStr() {
		return partKeyIsStr;
	}

	public String getPartFieldStrs() {
		if (partFieldStrs == null) {
			partFieldStrs = "";
			String PART_RULE = this.getMsgStoreParam("PART_RULE");
			if (PART_RULE != null && !"".equals(PART_RULE)) {
				if (PART_RULE.startsWith("1:")) {
					partFieldStrs = PART_RULE.substring(2);
				} else if (PART_RULE.startsWith("2:")) {
					partFieldStrs = "," + PART_RULE.substring(2) + ",";
					partKeyIsStr = true;
				}
			}
		}
		return partFieldStrs;
	}

	public void setMSG_STORE_CFG(String mSG_STORE_CFG) {
		MSG_STORE_CFG = mSG_STORE_CFG;
		storeConfig = null;
		if (MSG_STORE_CFG != null && !MSG_STORE_CFG.equals("")) {
			storeConfig = new HashMap<String, String>();
			String[] params = MSG_STORE_CFG.replaceAll("\r", "").split("\n");
			for (String par : params) {
				if (par.trim().startsWith("#"))
					continue;
				if (par.indexOf("=") > 0) {
					storeConfig.put(par.substring(0, par.indexOf("=")).trim(), par.substring(par.indexOf("=") + 1)
							.trim().replaceAll("\\n", "\n").replaceAll("\\\\n", "\n"));
				}
			}
		}
	}

	private Map<String, Object> distinctRule;
	private Map<String, String> storeConfig;

	public int getLOG_LEVEL() {
		return LOG_LEVEL;
	}

	public void setLOG_LEVEL(int LOG_LEVEL) {
		this.LOG_LEVEL = LOG_LEVEL;
	}

	public long getSTORE_ID() {
		return STORE_ID;
	}

	public void setSTORE_ID(long STORE_ID) {
		this.STORE_ID = STORE_ID;
	}

	public long getMSG_ID() {
		return MSG_ID;
	}

	public void setMSG_ID(long MSG_ID) {
		this.MSG_ID = MSG_ID;
	}

	public int getSEND_THD_POOL_SIZE() {
		return SEND_THD_POOL_SIZE;
	}

	public void setSEND_THD_POOL_SIZE(int SEND_THD_POOL_SIZE) {
		this.SEND_THD_POOL_SIZE = SEND_THD_POOL_SIZE;
	}

	public int getSEND_TARGET() {
		return SEND_TARGET;
	}

	public void setSEND_TARGET(int SEND_TARGET) {
		this.SEND_TARGET = SEND_TARGET;
	}

	public String getFILTER_RULE() {
		return FILTER_RULE;
	}

	public void setFILTER_RULE(String FILTER_RULE) {
		this.FILTER_RULE = FILTER_RULE;
	}

	public String getDISTINCT_RULE() {
		return DISTINCT_RULE;
	}

	public void setDISTINCT_RULE(String DISTINCT_RULE) {
		if (this.DISTINCT_RULE != null) {
			this.distinctRule = null;
		}
		this.DISTINCT_RULE = DISTINCT_RULE;
	}

	public String getFilterRule() {
		return FILTER_RULE;
	}

	public Map<String, Object> getDistinctRule() {
		if (distinctRule == null && DISTINCT_RULE != null && !"".equals(DISTINCT_RULE)) {
			distinctRule = JSON.parseObject(DISTINCT_RULE);
		}
		return distinctRule;
	}

	public boolean syncMsgInfo(MsgExtPO extPO) {
		if (this.SEND_TARGET != extPO.SEND_TARGET)
			return true;
		if (this.SEND_THD_POOL_SIZE != extPO.SEND_THD_POOL_SIZE)
			return true;
		if ((this.DISTINCT_RULE != null && !DISTINCT_RULE.equals(extPO.DISTINCT_RULE)) ||
				(extPO.DISTINCT_RULE != null && !extPO.DISTINCT_RULE.equals(DISTINCT_RULE)))
			return true;
		if ((this.MSG_STORE_CFG != null && !MSG_STORE_CFG.equals(extPO.MSG_STORE_CFG)) ||
				(extPO.MSG_STORE_CFG != null && !extPO.MSG_STORE_CFG.equals(MSG_STORE_CFG)))
			return true;
		return false;
	}
}
