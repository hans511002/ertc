package com.ery.ertc.collect.forwards.send;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.exception.ConfigException;
import com.ery.ertc.collect.forwards.MsgCache;
import com.ery.ertc.collect.forwards.Send;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.ToolUtil;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class SendRDBMS extends Send {
	public HTable table = null;
	public Configuration conf;
	public final Map<String, String> storeParams;
	public String tableName;
	public String rowkeyRule;
	public int rowKeyType = 0;// //1 字段替换 0 表达式计算
	public int beachSize = 1000;
	public boolean autoFlush = false;
	public Expression expr = null;
	public String[] macNames = null;
	public int[] fieldIndexRel = null;
	public static Pattern macPattern = Pattern.compile("\\{\\w+\\}");
	public final byte[] CF;
	public SimpleDateFormat sdf;
	public MsgFieldPO[] msgFields;
	public byte[][] qualifierNames;
	public boolean isMergeFields;
	public String mergeFieldSplitChar;

	public SendRDBMS(MsgPO msgPO, MsgExtPO extPo, MsgCache cache) throws IOException {
		super(msgPO, extPo, cache);
		storeParams = extPo.getStoreConfigParams();
		conf = HBaseConfiguration.create();
		for (String key : storeParams.keySet()) {
			conf.set(key, storeParams.get(key));
		}
		tableName = Convert.toString(storeParams.get("hbase.tableName"), this.msgPO.getMSG_TAG_NAME());
		table = new HTable(conf, tableName);
		rowKeyType = Convert.toInt(storeParams.get("hbase.rowkey.type"), 0);
		rowkeyRule = Convert.toString(storeParams.get("hbase.rowkey.rule"));
		beachSize = Convert.toInt(storeParams.get("hbase.beachSize"), beachSize);
		autoFlush = Convert.toBool(storeParams.get("hbase.isautoflush"), autoFlush);
		CF = Convert.toString(storeParams.get("hbase.family.name"), "f").getBytes();
		isMergeFields = Convert.toBool(storeParams.get("hbase.isMergeFields"), true);
		isMergeFields = Convert.toBool(storeParams.get("hbase.isMergeFields"), true);
		sdf = new SimpleDateFormat(Convert.toString(storeParams.get("hbase.date.format"), "yyyy-MM-dd HH:mm:ss"));
		mergeFieldSplitChar = Convert.toString(storeParams.get("hbase.mergeField.SplitChar"), ",");
		msgFields = this.msgPO.getMsgFields().toArray(new MsgFieldPO[0]);
		if (isMergeFields) {
			qualifierNames = new byte[1][];
			qualifierNames[0] = Convert.toString(storeParams.get("hbase.qualifier.name"), "c").getBytes();
		} else {
			qualifierNames = new byte[msgFields.length + 1][];
			for (int i = 0; i < msgFields.length; i++) {
				qualifierNames[i] = msgFields[i].getFIELD_NAME().getBytes();
			}
			qualifierNames[msgFields.length] = "log_msg_id".getBytes();
		}

		if (autoFlush) {
			table.setAutoFlush(true, true);
		} else {
			table.setAutoFlush(false, false);
		}
		if (rowKeyType == 0) {
			expr = AviatorEvaluator.compile(rowkeyRule);
			macNames = expr.getVariableNames().toArray(new String[0]);
			fieldIndexRel = new int[macNames.length];
			for (int i = 0; i < fieldIndexRel.length; i++) {
				String macName = macNames[i].toUpperCase();
				if (!msgPO.getFieldIdxMapping().containsKey(macName)) {
					throw new ConfigException("消息[" + msgPO.getMSG_ID() + ":" + msgPO.getMSG_TAG_NAME() + "] 存储ID:" +
							extPo.getSTORE_ID() + " 规则解析异常,宏变量:" + macName + "在字段中不存在");
				}
				fieldIndexRel[i] = msgPO.getFieldIdxMapping().get(macName);
			}
		} else {
			Matcher m = macPattern.matcher(rowkeyRule);
			StringBuffer sb = new StringBuffer();
			List<String> macNames = new ArrayList<String>();
			while (m.find()) {
				String macName = m.group(1);
				if (!msgPO.getFieldIdxMapping().containsKey(macName)) {
					throw new ConfigException("消息[" + msgPO.getMSG_ID() + ":" + msgPO.getMSG_TAG_NAME() + "] 存储ID:" +
							extPo.getSTORE_ID() + " 规则解析异常,宏变量:" + macName + "在字段中不存在");
				}
				m.appendReplacement(sb, "{" + macNames.size() + "}");
				macNames.add(macName);
			}
			m.appendTail(sb);
			rowkeyRule = sb.toString();
			this.macNames = macNames.toArray(new String[0]);
			fieldIndexRel = new int[this.macNames.length];
			for (int i = 0; i < fieldIndexRel.length; i++) {
				String macName = this.macNames[i].toUpperCase();
				fieldIndexRel[i] = msgPO.getFieldIdxMapping().get(macName);
			}
		}
		checkTable();
	}

	// 检查表是否存在并创建
	public void checkTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		boolean isExistTable = admin.tableExists(tableName);
		if (!isExistTable) { // 创建表
			createTable(admin);
		}
		admin.close();
	}

	public boolean createTable(HBaseAdmin admin) throws IOException {
		int blocksize = Convert.toInt(storeParams.get("hbase.table.blocksize"), 1 << 16);
		Algorithm compressionType = Algorithm.valueOf(Convert.toString(storeParams.get("hbase.table.CompressionType"),
				Algorithm.SNAPPY.getName()));
		int maxversion = Convert.toInt(storeParams.get("hbase.table.maxversion"), 1);
		int minversion = Convert.toInt(storeParams.get("hbase.table.minversion"), 0);
		BloomType bloomType = BloomType.valueOf(Convert.toString(storeParams.get("hbase.table.bloomType"),
				BloomType.ROW.name()));
		boolean inMemory = Convert.toBool(storeParams.get("hbase.table.inMemory"), false);

		long hfileMaxfilesize = Convert.toLong(storeParams.get("hbase.table.Maxfilesize"), 1 << 30l);
		long memstoreFlushSize = Convert.toLong(storeParams.get("hbase.table.memstore.FlushSize"), 1 << 30l);
		Durability logType = Durability.valueOf(Convert.toString(storeParams.get("hbase.table.wallog.type"),
				Durability.ASYNC_WAL.name()));
		int ttl = Convert.toInt(storeParams.get("hbase.table.ttl"), 1 << 31l);

		try {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.setMaxFileSize(hfileMaxfilesize);
			tableDesc.setMemStoreFlushSize(memstoreFlushSize);
			tableDesc.setDurability(logType);

			// 添加列族
			HColumnDescriptor hcolumn = new HColumnDescriptor(CF);
			hcolumn.setBlocksize(blocksize);
			hcolumn.setCompactionCompressionType(compressionType);
			hcolumn.setCompressionType(compressionType);
			hcolumn.setMinVersions(minversion);
			hcolumn.setMaxVersions(maxversion);
			hcolumn.setBloomFilterType(bloomType);
			hcolumn.setInMemory(inMemory);
			hcolumn.setTimeToLive(ttl);

			tableDesc.addFamily(hcolumn);
			admin.createTable(tableDesc);
			LogUtils.info("Create hbase table success!");
			return true;
		} catch (IOException e) {
			String errorInfo = "Create hbase table error!";
			LogUtils.error(errorInfo, e);
			throw e;
		}
	}

	@Override
	public int send() {
		List<Object[]> todoSends = getMore(beachSize);
		if (todoSends == null || todoSends.size() == 0)
			return 0;
		try {
			List<Put> puts = new ArrayList<Put>(todoSends.size());
			StringBuffer sb = new StringBuffer(1024);
			for (Object[] val : todoSends) {
				Map<String, Object> nav = new HashMap<String, Object>();
				if (fieldIndexRel != null) {
					for (int i = 0; i < fieldIndexRel.length; i++) {// 转计算对象
						if (val[fieldIndexRel[i]] != null) {
							nav.put(this.macNames[i], ToolUtil.convertToExecEnvObj(val[fieldIndexRel[i]].toString()));
						} else {
							nav.put(this.macNames[i], null);
						}
					}
				}
				String rowkey = this.expr.execute(nav).toString();
				Put put = new Put(rowkey.getBytes());
				if (isMergeFields) {
					sb.setLength(0);
					for (int i = 0; i < this.msgFields.length; i++) {
						if (this.msgFields[i].getType() == 8) {
							try {
								sb.append(sdf.format(new Date(Convert.toLong(val[i]))));
							} catch (Exception e) {
								sb.append(val[i].toString());
							}
						} else {
							sb.append(val[i].toString());
						}
						sb.append(mergeFieldSplitChar);
					}
					sb.append(val[this.msgFields.length].toString());
					put.add(CF, this.qualifierNames[0], sb.toString().getBytes());
				} else {
					for (int i = 0; i < this.msgFields.length; i++) {
						byte[] va = null;
						if (this.msgFields[i].getType() == 8) {
							try {
								va = sdf.format(new Date(Convert.toLong(val[i]))).getBytes();
							} catch (Exception e) {
								va = val[i].toString().getBytes();
							}
						} else {
							va = val[i].toString().getBytes();
						}
						put.add(CF, this.qualifierNames[i], va);
					}
					put.add(CF, this.qualifierNames[msgFields.length], val[msgFields.length].toString().getBytes());
				}
				if (sendPlugin != null) {
					ISendPlugin.beforeSend(sendPlugin, put, val);
				}
				puts.add(put);
			}
			table.put(puts);
			return todoSends.size();
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			DaemonMaster.daemonMaster.workerService.notifySendError(msgPO.getMSG_ID(), this.extPo.getSTORE_ID(),
					sw.toString(), todoSends);
			LogUtils.error("发送向Hbase出错!" + e.getMessage(), e);
			if (autoFlush)
				sendFail(todoSends);
			Utils.sleep(1000);
		}
		return 0;

	}

	public void stop() {
		try {
			super.stop();
			table.flushCommits();
			table.close();
		} catch (Exception e) {

		}
	}
}
