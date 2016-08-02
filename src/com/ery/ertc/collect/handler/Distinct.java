package com.ery.ertc.collect.handler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.exception.ConfigException;
import com.ery.ertc.collect.handler.distinct.Conf;
import com.ery.ertc.collect.handler.distinct.FilterCacheHash;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.utils.Convert;

public class Distinct {

	public static final String DISTINCT_DIR = "../collect_distinct";
	public static final String FILTER_FILE_NAME = "filter";// filter2标示二级过滤器
	public static final String DISTINCT_CACHE_FILE_NAME = "distinct";// 最终存储如：distinct.0312-050639.{毫秒数}
	public static final int DEFAULT_NUM = 10000;// 1w记录，最终容器占用大概0.04 mb

	private MsgPO msgPO;
	MsgExtPO extPo;
	private FilterCacheHash filterHash;
	// private MulHashTable hashTable;
	private int[] distinctFieldIdxRel;

	private int predictNum;// 预估数据量
	private long timeRange;// 时间范围（数据库存的是整合单位，实际使用需要用毫秒数）。
	private int partFieldIdx = -1;
	private int timeFieldIdx = -1;

	public Distinct(MsgPO msgPO, MsgExtPO extPo) {
		this.msgPO = msgPO;
		this.extPo = extPo;
		ready();// 初始准备一些预处理数据
	}

	/**
	 * 准备数据
	 * 
	 * @return 返回true标示可以执行去重过滤，返回false标示定义不全无需执行去重
	 */
	public boolean ready() {
		if (distinctFieldIdxRel == null) {
			Map<String, Object> distinctRule = extPo.getDistinctRule();
			if (distinctRule == null)
				return false;
			Map<String, Integer> fieldIdxMapping = msgPO.getFieldIdxMapping();
			String fields = Convert.toString(distinctRule.get("fields"), null);
			if (fields == null || "".equals(fields)) {
				return false;
			}
			String[] arr = fields.split(",");
			distinctFieldIdxRel = new int[arr.length];
			for (int i = 0; i < arr.length; i++) {
				distinctFieldIdxRel[i] = fieldIdxMapping.get(arr[i]);
				if (!fieldIdxMapping.containsKey(arr[i])) {
					throw new ConfigException("过滤规则配置错误，字段" + arr[i] + "不存在");
				}
			}
			predictNum = Convert.toInt(distinctRule.get("predictNum"), DEFAULT_NUM);
			String tr = Convert.toString(distinctRule.get("timeRange"));
			int idx_ = -1;
			if ((idx_ = tr.indexOf(":")) == -1) {
				throw new IllegalArgumentException("未定义时间范围!");
			}
			timeRange = Convert.toLong(tr.substring(idx_ + 1));
			tr = tr.substring(0, idx_).toLowerCase();
			if ("d".equals(tr)) {// 天
				timeRange *= 86400000;
			} else if ("h".equals(tr)) {// 时
				timeRange *= 3600000;
			} else if ("m".equals(tr)) {// 分钟
				timeRange *= 60000;
			} else {
				throw new IllegalArgumentException("时间范围[" + distinctRule.get("timeRange") + "]定义有误!");
			}
			partFieldIdx = -1;
			if (distinctRule.containsKey("partField")) {
				partFieldIdx = fieldIdxMapping.get(distinctRule.get("partField"));
			}
			timeFieldIdx = -1;
			if (distinctRule.containsKey("timeField")) {
				timeFieldIdx = fieldIdxMapping.get(distinctRule.get("timeField"));
			}
			Conf conf = new Conf();
			if (Config.getDistinctDir().startsWith("/")) {
				conf.setFilePath(Config.getDistinctDir() + "/" + msgPO.getMSG_TAG_NAME() + "/" +
						this.extPo.getSTORE_ID() + "/" + DISTINCT_CACHE_FILE_NAME);
			} else {
				conf.setFilePath(Config.getCurrentWorkDir() + "/" + Config.getDistinctDir() + "/" +
						msgPO.getMSG_TAG_NAME() + "/" + this.extPo.getSTORE_ID() + "/" + DISTINCT_CACHE_FILE_NAME);
			}
			conf.setFileCycle(timeRange);
			conf.setBusKey(msgPO.getMSG_TAG_NAME());
			conf.setNumLevel(predictNum);
			filterHash = new FilterCacheHash(conf);
			filterHash.shutup();
		}
		return distinctFieldIdxRel != null;
	}

	/**
	 * 去重
	 * 
	 * @param tuples
	 *            数据元组
	 * @return 返回true。标示重复
	 */
	public boolean distinct(Object[] tuples) {
		if (distinctFieldIdxRel != null) {
			byte[] part = null;
			long time = 0;
			if (partFieldIdx != -1) {
				Object val = tuples[partFieldIdx];
				if (val instanceof Double) {
					part = Convert.toBytes((Double) val);
				} else if (val instanceof Number) {
					part = Convert.toBytes((Long) val);
				} else if (val instanceof byte[]) {
					part = (byte[]) val;
				} else if (val != null) {
					part = Convert.toString(val).getBytes();
				}
			}
			if (timeFieldIdx != -1) {
				time = Convert.toLong(tuples[timeFieldIdx]);
			}
			byte[] data = getDistinctFieldsValueKey(tuples, distinctFieldIdxRel, partFieldIdx, part);
			return filterHash.containsFilter(data, part, time);
		}
		return false;
	}

	// 停止
	public void shutdown() {
		if (filterHash != null) {
			filterHash.shutdown();
		}
	}

	/**
	 * 获取排重字段值对应的二进制key
	 * 
	 * @param tuples
	 *            消息数据
	 * @param distinctFieldIdxMapping
	 *            重复字段对应的数据索引
	 * @param partIdx
	 *            分区字段索引（如果没有，则是-1）
	 * @param part
	 *            分区字段对应的byte[],如果没有则对应null。。。传入分区主要是为了避免二次计算
	 * @return
	 */
	public static byte[] getDistinctFieldsValueKey(Object[] tuples, int[] distinctFieldIdxRel, int partIdx, byte[] part) {
		List<byte[]> bytesList = new ArrayList<byte[]>();
		int len = 0;
		for (int findex : distinctFieldIdxRel) {
			if (findex == partIdx) {
				bytesList.add(part);
				len += part.length;
			} else {
				Object val = tuples[findex];
				if (val instanceof Double) {
					bytesList.add(Convert.toBytes((Double) val));
					len += 8;
				} else if (val instanceof Number) {
					bytesList.add(Convert.toBytes((Long) val));
					len += 8;
				} else if (val instanceof byte[]) {
					byte[] b = (byte[]) val;
					bytesList.add(b);
					len += b.length;
				} else {
					byte[] b = Convert.toString(val).getBytes();
					bytesList.add(b);
					len += b.length;
				}
			}
		}
		byte[] data = new byte[len];// 需要排重字段数据的二进制编码
		int of = 0;
		for (byte[] b : bytesList) {
			System.arraycopy(b, 0, data, of, b.length);
			of += b.length;
		}
		return data;
	}

	// 获取发送数据对应的distinct key
	public static byte[] getDistinctFieldsValueKey(MsgPO msgPO, byte[] sendValue,
			TreeMap<String, Integer> distinctFieldIdxMapping) {
		List<byte[]> keys = new ArrayList<byte[]>();
		int len = 0;
		Iterator<Map.Entry<String, Integer>> iterator = distinctFieldIdxMapping.entrySet().iterator();
		for (int idx = 0, i = 0; i < msgPO.getMsgFields().size() && iterator.hasNext(); i++) {
			Map.Entry<String, Integer> entry = iterator.next();
			String fname = entry.getKey();
			int fidx = entry.getValue();
			MsgFieldPO fieldPO = msgPO.getMsgFields().get(i);
			byte[] b = null;
			switch (fieldPO.getType()) {
			case 2:// 数字
			case 4:// 小数
			case 8:// 日期数字
				if (fidx == i && fname.equals(fieldPO.getFIELD_NAME())) {
					b = new byte[8];
					System.arraycopy(sendValue, idx, b, 0, 8);
					keys.add(b);
					len += 8;
				}
				idx += 8;
				break;
			case 16:
				b = new byte[4];
				System.arraycopy(sendValue, idx, b, 0, 4);
				idx += 4;
				b = new byte[Convert.toInt(b)];
				if (fidx == i && fname.equals(fieldPO.getFIELD_NAME())) {
					System.arraycopy(sendValue, idx, b, 0, b.length);
					keys.add(b);
					len += b.length;
				}
				idx += b.length;
			default:
				b = new byte[4];
				System.arraycopy(sendValue, idx, b, 0, 4);
				idx += 4;
				b = new byte[Convert.toInt(b)];
				if (fidx == i && fname.equals(fieldPO.getFIELD_NAME())) {
					System.arraycopy(sendValue, idx, b, 0, b.length);
					keys.add(b);
					len += b.length;
				}
				idx += b.length;
			}
		}

		byte[] data = new byte[len];// 需要排重字段数据的二进制编码
		int of = 0;
		for (byte[] b : keys) {
			System.arraycopy(b, 0, data, of, b.length);
			of += b.length;
		}
		return data;
	}

}
