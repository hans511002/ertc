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

public class SendHbase extends Send {
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
	// yyyyMMddHHmmss yyyyMMddHHmmss.sss yyyyMMddHHmmsssss
	public SimpleDateFormat keyDF;
	public MsgFieldPO[] msgFields;
	public byte[][] qualifierNames;
	public boolean isMergeFields;
	public String mergeFieldSplitChar;

	public SendHbase(MsgPO msgPO, MsgExtPO extPo, MsgCache cache) throws IOException {
		super(msgPO, extPo, cache);
		storeParams = extPo.getStoreConfigParams();
		conf = HBaseConfiguration.create();
		for (String key : storeParams.keySet()) {
			conf.set(key, storeParams.get(key));
		}
		tableName = Convert.toString(storeParams.get("hbase.tableName"), this.msgPO.getMSG_TAG_NAME());
		rowKeyType = Convert.toInt(storeParams.get("hbase.rowkey.type"), 0);
		rowkeyRule = Convert.toString(storeParams.get("hbase.rowkey.rule"));
		beachSize = Convert.toInt(storeParams.get("hbase.beachSize"), beachSize);
		autoFlush = Convert.toBool(storeParams.get("hbase.isautoflush"), autoFlush);
		CF = Convert.toString(storeParams.get("hbase.family.name"), "f").getBytes();
		isMergeFields = Convert.toBool(storeParams.get("hbase.isMergeFields"), true);
		isMergeFields = Convert.toBool(storeParams.get("hbase.isMergeFields"), true);
		keyDF = new SimpleDateFormat(Convert.toString(storeParams.get("hbase.date.format"), "yyyyMMddHHmmss"));
		mergeFieldSplitChar = Convert.toString(storeParams.get("hbase.mergeField.SplitChar"), ",");
		msgFields = this.msgPO.getMsgFields().toArray(new MsgFieldPO[0]);
		if (isMergeFields) {
			qualifierNames = new byte[1][];
			qualifierNames[0] = Convert.toString(storeParams.get("hbase.qualifier.name"), "c").getBytes();
		} else {
			qualifierNames = new byte[msgFields.length + 2][];
			qualifierNames[0] = "COLLECT_HOSTNAME".getBytes();
			qualifierNames[1] = "COLLECT_LOG_ID".getBytes();
			for (int i = 0; i < msgFields.length; i++) {
				qualifierNames[i + 2] = msgFields[i].getFIELD_NAME().getBytes();
			}
		}
		checkTable();
		// Connection con =
		// org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(conf);
		// table =con.getTable(TableName.valueOf( tableName));//
		table = new HTable(conf, tableName);
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
					throw new ConfigException("消息[" + msgId + ":" + msgPO.getMSG_TAG_NAME() + "] 存储ID:" +
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
					throw new ConfigException("消息[" + msgId + ":" + msgPO.getMSG_TAG_NAME() + "] 存储ID:" +
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
		String comType = Convert.toString(storeParams.get("hbase.table.CompressionType"), Algorithm.SNAPPY.getName());
		Algorithm compressionType = org.apache.hadoop.hbase.io.compress.Compression
				.getCompressionAlgorithmByName(comType);
		int maxversion = Convert.toInt(storeParams.get("hbase.table.maxversion"), 1);
		int minversion = Convert.toInt(storeParams.get("hbase.table.minversion"), 0);
		BloomType bloomType = BloomType.valueOf(Convert.toString(storeParams.get("hbase.table.bloomType"),
				BloomType.ROW.name()));
		boolean inMemory = Convert.toBool(storeParams.get("hbase.table.inMemory"), false);

		long hfileMaxfilesize = Convert.toLong(storeParams.get("hbase.table.Maxfilesize"), 1 << 30l);
		long memstoreFlushSize = Convert.toLong(storeParams.get("hbase.table.memstore.FlushSize"), 1 << 30l);
		Durability logType = Durability.valueOf(Convert.toString(storeParams.get("hbase.table.wallog.type"),
				Durability.ASYNC_WAL.name()));
		int ttl = Convert.toInt(storeParams.get("hbase.table.ttl"), Integer.MAX_VALUE);

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
	public int send() throws IOException {
		List<Object[]> todoSends = getMore(beachSize);
		if (todoSends == null || todoSends.size() == 0)
			return 0;
		try {
			List<Put> puts = new ArrayList<Put>(todoSends.size());
			StringBuffer sb = new StringBuffer(1024);
			long byteSize = 0;
			for (Object[] val : todoSends) {
				String rowkey;
				if (rowKeyType == 0) {
					Map<String, Object> nav = new HashMap<String, Object>();
					if (fieldIndexRel != null) {
						for (int i = 0; i < fieldIndexRel.length; i++) {// 转计算对象
							if (val[fieldIndexRel[i]] != null) {
								if (fieldIndexRel[i] - 2 > 0 && this.msgFields[fieldIndexRel[i] - 2].getType() == 8) {
									nav.put(this.macNames[i],
											keyDF.format(new Date(Convert.toLong(val[fieldIndexRel[i]]))));
								} else {
									nav.put(this.macNames[i],
											ToolUtil.convertToExecEnvObj(val[fieldIndexRel[i]].toString()));
								}
							} else {
								nav.put(this.macNames[i], null);
							}
						}
					}
					rowkey = this.expr.execute(nav).toString();
				} else {
					rowkey = rowkeyRule;
					if (fieldIndexRel != null) {
						for (int i = 0; i < fieldIndexRel.length; i++) {// 转计算对象
							String v = val[fieldIndexRel[i]].toString();
							if (fieldIndexRel[i] - 2 > 0 && this.msgFields[fieldIndexRel[i] - 2].getType() == 8) {
								v = keyDF.format(new Date(Convert.toLong(val[fieldIndexRel[i]])));
							}
							rowkey = rowkey.replaceAll("\\{" + i + "\\}", v);
						}
					}
				}
				byte tmpBts[] = rowkey.getBytes();
				Put put = new Put(tmpBts);
				byteSize += tmpBts.length;
				if (isMergeFields) {
					sb.setLength(0);
					sb.append(val[0].toString());
					sb.append(mergeFieldSplitChar);
					sb.append(val[1].toString());
					for (int i = 0; i < this.msgFields.length; i++) {
						sb.append(mergeFieldSplitChar);
						Object o = val[i + 2];
						if (this.msgFields[i].getType() == 8) {
							try {
								sb.append(this.msgFields[i].getSimpleDateFormat().format(new Date(Convert.toLong(o))));
							} catch (Exception e) {
								sb.append(o.toString());
							}
						} else {
							sb.append(o.toString());
						}
					}
					tmpBts = sb.toString().getBytes();
					byteSize += tmpBts.length;
					put.addColumn(CF, this.qualifierNames[0], tmpBts);
				} else {
					tmpBts = val[0].toString().getBytes();
					put.addColumn(CF, this.qualifierNames[0], tmpBts);
					byteSize += tmpBts.length;
					tmpBts = val[1].toString().getBytes();
					put.addColumn(CF, this.qualifierNames[1], tmpBts);
					byteSize += tmpBts.length;
					for (int i = 0; i < this.msgFields.length; i++) {
						Object o = val[i + 2];
						byte[] va = null;
						if (this.msgFields[i].getType() == 8) {
							try {
								va = this.msgFields[i].getSimpleDateFormat().format(new Date(Convert.toLong(o)))
										.getBytes();
							} catch (Exception e) {
								va = o.toString().getBytes();
							}
						} else {
							va = o.toString().getBytes();
						}
						put.addColumn(CF, this.qualifierNames[i + 2], va);
						byteSize += va.length;
					}
				}
				if (sendPlugin != null) {
					ISendPlugin.beforeSend(sendPlugin, put, val);
				}
				puts.add(put);
				// byteSize += put.heapSize(); // 对象大小，不只数据
			}
			table.put(puts);
			table.flushCommits();
			DaemonMaster.daemonMaster.workerService.notifySendOK(msgId, stroeId, puts.size(), byteSize);
			return todoSends.size();
		} catch (Throwable e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			DaemonMaster.daemonMaster.workerService.notifySendError(msgId, stroeId, sw.toString(), todoSends);
			LogUtils.error("发送向Hbase出错!" + e.getMessage(), e);
			sendFail(todoSends);
			Utils.sleep(1000);
			isExped = true;
			throw new IOException(e);
		}
	}

	boolean isExped = false;

	public void stop() {
		try {
			super.stop();
			if (!isExped)
				table.flushCommits();
			table.close();
		} catch (Exception e) {

		}
	}
}
