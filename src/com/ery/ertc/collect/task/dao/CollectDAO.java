package com.ery.ertc.collect.task.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.forwards.send.SendKafka;
import com.ery.ertc.estorm.po.CollectErrorLogPO;
import com.ery.ertc.estorm.po.CollectLogPO;
import com.ery.ertc.estorm.po.CollectSendErrorLogPO;
import com.ery.ertc.estorm.po.CollectTotalLogPO;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.StringUtils;
import com.ery.base.support.jdbc.DBUtils;
import com.ery.base.support.jdbc.IParamsSetter;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.podo.BaseDAO;
import com.ery.base.support.utils.Convert;

public class CollectDAO extends BaseDAO {
	public static java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * 查询需要采集的任务
	 * 
	 * @return
	 */
	public List<Map<String, Object>> queryCollectTasks(long time) {
		String sql = "SELECT MSG_ID,TRIGGER_TYPE,STATE,date_format(MODIFY_TIME,'%Y-%m-%d %H:%i:%s') MODIFY_TIME FROM ST_MSG_STREAM WHERE " +
				" MODIFY_TIME > STR_TO_DATE('" +
				sdf.format(new Date(time)) +
				"','%Y-%m-%d %H:%i:%s')  ORDER BY TRIGGER_TYPE";
		return getDataAccess().queryForList(sql);
	}

	public MsgPO getCollectMsgInfo(String msgId) {
		Map<String, MsgPO> map = getMsgInfos(msgId);
		if (map.size() > 0)
			return map.remove(msgId);
		return null;
	}

	public Map<String, MsgPO> getMsgInfos(Set<String> addMsgId, Set<String> modMsgId) {
		StringBuffer sb = new StringBuffer();
		for (String string : addMsgId) {
			sb.append(string);
			sb.append(",");
		}
		for (String string : modMsgId) {
			sb.append(string);
			sb.append(",");
		}
		String msgs = sb.substring(0, sb.length() - 1);
		return getMsgInfos(msgs);
	}

	public Map<String, MsgPO> getMsgInfos(String... msgs) {
		String msgIds = StringUtils.join(",", msgs);
		LogUtils.info("query msg config:" + msgIds);
		String sql = "SELECT  A.MSG_ID,MSG_NAME,TRIGGER_TYPE,URL,USER," +
				"PASS,REQ_PARAM,DATA_SCHEME,DATA_SOURCE_ID,MSG_TAG_NAME," +
				"MSG_DESC,MSG_ORDER_FLAG,LAST_COLLECT_POINTER,CACHE_DS " +
				",DATE_FORMAT(A.MODIFY_TIME,'%Y-%m-%d %H:%i:%s') MODIFY_TIME" +
				" FROM ST_MSG_STREAM A  WHERE A.MSG_ID in (" + msgIds + ")";
		Map<String, MsgPO> msgPos = new HashMap<String, MsgPO>();
		List<Map<String, Object>> mapList = getDataAccess().queryForList(sql);
		for (Map<String, Object> map : mapList) {
			MsgPO po = new MsgPO();
			try {
				po.setMSG_ID(Convert.toLong(map.get("MSG_ID")));
				po.setMSG_NAME(Convert.toString(map.get("MSG_NAME")));
				po.setTRIGGER_TYPE(Convert.toInt(map.get("TRIGGER_TYPE")));
				po.setURL(Convert.toString(map.get("URL")));
				po.setUSER(Convert.toString(map.get("USER")));
				po.setPASS(Convert.toString(map.get("PASS")));
				po.setREQ_PARAM(Convert.toString(map.get("REQ_PARAM")));
				po.setDATA_SCHEME(Convert.toString(map.get("DATA_SCHEME")));
				po.setDATA_SOURCE_ID(Convert.toLong(map.get("DATA_SOURCE_ID"), 0));
				po.setMSG_TAG_NAME(Convert.toString(map.get("MSG_TAG_NAME")));
				po.setMSG_DESC(Convert.toString(map.get("MSG_DESC")));
				po.setMSG_ORDER_FLAG(Convert.toInt(map.get("MSG_ORDER_FLAG"), 0));
				po.setLAST_COLLECT_POINTER(Convert.toString(map.get("LAST_COLLECT_POINTER")));
				po.setCACHE_DS(Convert.toLong(map.get("CACHE_DS"), -1));
				po.setTODO_SEND_QUEUE_SIZE(Convert.toInt(map.get("TODO_SEND_QUEUE_SIZE"), Config.getSendQueueSize()));
				// 读取存储扩展配置,支持多目标分发
				sql = "select STORE_ID,b.DATA_SOURCE_ID,SEND_THD_POOL_SIZE,B.TODO_SEND_QUEUE_SIZE,B.LOG_LEVEL" +
						",B.FILTER_RULE ,B.DISTINCT_RULE,MSG_STORE_CFG,STORE_PLUGIN" +
						",d.DATA_SOURCE_TYPE,d.DATA_SOURCE_URL,d.DATA_SOURCE_USER,d.DATA_SOURCE_PASS,d.DATA_SOURCE_CFG " +
						" from st_msg_store_cfg b left join st_data_source d " +
						" on b.DATA_SOURCE_ID=d.DATA_SOURCE_ID and d.STATE=1 " + " where b.state=1 and MSG_ID=" +
						po.getMSG_ID();
				List<Map<String, Object>> extMapList = getDataAccess().queryForList(sql);
				if (extMapList != null && extMapList.size() > 0) {
					for (Map<String, Object> extMap : extMapList) {
						MsgExtPO extPO = new MsgExtPO();
						extPO.setMSG_ID(po.getMSG_ID());
						extPO.setSTORE_ID(Convert.toLong(extMap.get("STORE_ID")));
						extPO.setSEND_THD_POOL_SIZE(Convert.toInt(extMap.get("SEND_THD_POOL_SIZE"),
								Config.getSendThreadNum()));

						// 10-19 rdb 11,ORACLE 12,MYSQL 23,HBASE 34,FTP
						// 45,HDFS,50：kafka
						int dsType = Convert.toInt(extMap.get("DATA_SOURCE_TYPE"), 50);
						if (dsType > 0 && dsType < 20) {
							extPO.setSEND_TARGET(3);
						} else if (dsType == 23) {
							extPO.setSEND_TARGET(1);
						} else if (dsType == 45) {
							extPO.setSEND_TARGET(2);
						} else if (dsType == 50) {
							extPO.setSEND_TARGET(0);
						}

						extPO.setFILTER_RULE(Convert.toString(extMap.get("FILTER_RULE"), null));
						extPO.setDISTINCT_RULE(Convert.toString(extMap.get("DISTINCT_RULE"), null));
						extPO.setLOG_LEVEL(Convert.toInt(extMap.get("LOG_LEVEL"), 0));
						extPO.setSTORE_PLUGIN(Convert.toString(extMap.get("STORE_PLUGIN"), null));

						String dsCfg = Convert.toString(extMap.get("DATA_SOURCE_CFG"), "").trim();
						if (dsCfg.length() > 0 && !dsCfg.endsWith("\n"))
							dsCfg += "\n";
						dsCfg += "ds.DATA_SOURCE_TYPE=" + dsType + "\n";
						dsCfg += "ds.DATA_SOURCE_URL=" + Convert.toString(extMap.get("DATA_SOURCE_URL"), "") + "\n";
						dsCfg += "ds.DATA_SOURCE_USER=" + Convert.toString(extMap.get("DATA_SOURCE_USER"), "") + "\n";
						dsCfg += "ds.DATA_SOURCE_PASS=" + Convert.toString(extMap.get("DATA_SOURCE_PASS"), "") + "\n";

						String storeCfg = Convert.toString(extMap.get("MSG_STORE_CFG"), null);
						if (extPO.getSEND_TARGET() == 0) {// kafka 专用配置
							if (storeCfg == null || storeCfg.equals("")) {
								storeCfg = "PARTITION_NUM=" + SendKafka.DEFAULT_PART_NUM;
								storeCfg += "\nPARTITION_REPLICA_NUM=" + SendKafka.DEFAULT_PART_REPLICA_NUM;
								storeCfg += "\nbatch.num.messages=" + SendKafka.DEFAULT_ASYNC_BATCH_NUM;
								storeCfg += "\nproducer.type=sync";
								storeCfg += "\nrequest.required.acks=0";
								storeCfg += "\ncompression.codec=0";
							}
						}
						extPO.setMSG_STORE_CFG(dsCfg + storeCfg);
						po.addExtPO(extPO);
					}
				} else {
					MsgExtPO extPO = new MsgExtPO();
					extPO.setMSG_ID(po.getMSG_ID());
					extPO.setSTORE_ID(Convert.toLong(0));
					extPO.setSEND_THD_POOL_SIZE(Config.getSendThreadNum());
					extPO.setSEND_TARGET(0);
					extPO.setFILTER_RULE(null);
					extPO.setDISTINCT_RULE(null);
					extPO.setLOG_LEVEL(0);
					// kafka 专用配置
					String storeCfg = "PARTITION_NUM=" + SendKafka.DEFAULT_PART_NUM;
					storeCfg += "\nPARTITION_REPLICA_NUM=" + SendKafka.DEFAULT_PART_REPLICA_NUM;
					storeCfg += "\nbatch.num.messages=" + SendKafka.DEFAULT_ASYNC_BATCH_NUM;
					storeCfg += "\nproducer.type=sync";
					storeCfg += "\nrequest.required.acks=0";
					storeCfg += "\ncompression.codec=0";
					extPO.setMSG_STORE_CFG(storeCfg);
					po.addExtPO(extPO);
					// // kafka专用配置
					// MsgQueueParamPO paramPO = new MsgQueueParamPO();
					// paramPO.setPARAM_ID(Convert.toLong(map.get("PARAM_ID")));
					// paramPO.setMSG_ID(Convert.toLong(map.get("MSG_ID")));
					// paramPO.setPART_NUM(Convert.toInt(map.get("PART_NUM"),
					// SendKafka.DEFAULT_PART_NUM));
					// paramPO.setPART_RULE(Convert.toString(map.get("PART_RULE")));
					// paramPO.setPART_REPLICA_NUM(Convert.toInt(map.get("PART_REPLICA_NUM"),
					// SendKafka.DEFAULT_PART_REPLICA_NUM));
					// paramPO.setSEND_TYPE(Convert.toInt(map.get("SEND_TYPE"),
					// 0));
					// paramPO.setSEND_ACK_LEVEL(Convert.toInt(map.get("SEND_ACK_LEVEL"),
					// 0));
					// paramPO.setSEND_BATCH_NUM(Convert.toInt(map.get("SEND_BATCH_NUM"),
					// SendKafka.DEFAULT_ASYNC_BATCH_NUM));
					// paramPO.setCOMPRESSION_TYPE(Convert.toInt(map.get("COMPRESSION_TYPE"),
					// 0));
					// po.setQueueParam(paramPO);
				}

				// 字段信息
				sql = "SELECT FIELD_ID,MSG_ID,FIELD_NAME,FIELD_CN_NAME,FIELD_DATA_TYPE"
						+ ",SRC_FIELD,ORDER_ID,FIELD_DESC " + "FROM ST_MSG_FIELDS WHERE MSG_ID=? ORDER BY ORDER_ID ASC";
				List<Map<String, Object>> fieldMaps = getDataAccess().queryForList(sql, po.getMSG_ID());
				List<MsgFieldPO> fpos = po.getMsgFields();
				// MsgFieldPO fpo = new MsgFieldPO();
				// fpo.setFIELD_ID(-1);
				// fpo.setMSG_ID(Convert.toLong(po.getMSG_ID()));
				// fpo.setFIELD_NAME("COLLECT_HOSTNAME");
				// fpo.setFIELD_CN_NAME("采集主机名称");
				// fpo.setFIELD_DATA_TYPE("VARCHAR");
				// fpo.setSRC_FIELD("-1");
				// fpo.setORDER_ID(0);
				// fpo.setFIELD_DESC("");
				// fpos.add(fpo);
				// fpo = new MsgFieldPO();
				// fpo.setFIELD_ID(-1);
				// fpo.setMSG_ID(Convert.toLong(po.getMSG_ID()));
				// fpo.setFIELD_NAME("COLLECT_LOG_ID");
				// fpo.setFIELD_CN_NAME("消息日志ID");
				// fpo.setFIELD_DATA_TYPE("NUMBER");
				// fpo.setSRC_FIELD("-1");
				// fpo.setORDER_ID(0);
				// fpo.setFIELD_DESC("由采集主机编号，加上采集主机唯一");
				// fpos.add(fpo);
				for (Map<String, Object> fieldMap : fieldMaps) {
					MsgFieldPO fpo = new MsgFieldPO();
					fpo.setFIELD_ID(Convert.toLong(fieldMap.get("FIELD_ID")));
					fpo.setMSG_ID(Convert.toLong(fieldMap.get("MSG_ID")));
					fpo.setFIELD_NAME(Convert.toString(fieldMap.get("FIELD_NAME")).toUpperCase());
					fpo.setFIELD_CN_NAME(Convert.toString(fieldMap.get("FIELD_CN_NAME")));
					fpo.setFIELD_DATA_TYPE(Convert.toString(fieldMap.get("FIELD_DATA_TYPE")).toUpperCase());
					fpo.setSRC_FIELD(Convert.toString(fieldMap.get("SRC_FIELD")));
					fpo.setORDER_ID(Convert.toInt(fieldMap.get("ORDER_ID")));
					fpo.setFIELD_DESC(Convert.toString(fieldMap.get("FIELD_DESC")));
					fpos.add(fpo);
				}
				msgPos.put(po.getMSG_ID() + "", po);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return msgPos;
	}

	// 保存采集点
	public void saveCollectPointer(long msgId, String collectPointer) {
		String sql = "UPDATE ST_MSG_STREAM SET LAST_COLLECT_POINTER=? WHERE MSG_ID=?";
		getDataAccess().execUpdate(sql, collectPointer, msgId);
	}

	private static boolean primaryKeyInited = false;

	public synchronized static void initPrimaryKey() {
		if (primaryKeyInited)
			return;

		CollectDAO dao = new CollectDAO();
		String keySql = "SELECT MAX(LOG_ID) FROM ST_MSG_COLLECT_LOG";
		long id = 0;
		try {
			id = dao.getDataAccess().queryForLongByNvl(keySql, 0);
			if (id > 0) {
				id = id % 10000000000000l;
				if (id < System.currentTimeMillis()) {
					id = System.currentTimeMillis();
				}
				DBUtils.initKeyForIP("ST_MSG_COLLECT_LOG", id);
				DBUtils.initKeyForIP("ST_MSG_DETAIL_LOG", id);
			}
		} catch (Exception e) {
			id = System.currentTimeMillis();
			DBUtils.initKeyForIP("ST_MSG_COLLECT_LOG", id);
		}

		keySql = "SELECT MAX(LOG_ID%10000000000000) FROM ST_MSG_COLLECT_ERROR_LOG";
		id = dao.getDataAccess().queryForLongByNvl(keySql, 0);
		if (id > 0) {
			DBUtils.initKeyForIP("ST_MSG_COLLECT_ERROR_LOG", id);
		}

		keySql = "SELECT MAX(LOG_ID%10000000000000) FROM ST_MSG_SEND_ERROR_LOG";
		id = dao.getDataAccess().queryForLongByNvl(keySql, 0);
		if (id > 0) {
			DBUtils.initKeyForIP("ST_MSG_SEND_ERROR_LOG", id);
		}

		keySql = "SELECT MAX(LOG_ID%10000000000000) FROM ST_MSG_COLLECT_TOTAL_LOG";
		id = dao.getDataAccess().queryForLongByNvl(keySql, 0);
		if (id > 0) {
			DBUtils.initKeyForIP("ST_MSG_COLLECT_TOTAL_LOG", id);
		}

		dao.close();
		primaryKeyInited = true;
	}

	// 采集日志
	public void writeCollectLog(CollectLogPO logPO) {
		String sql = "INSERT INTO ST_MSG_COLLECT_LOG"
				+ "(LOG_ID,MSG_ID,START_TIME,END_TIME,BYTE_SIZE,MSG_NUM,SERVER_HOST) " + "VALUES(?,?,?,?,?,?,?)";
		logPO.setEND_TIME(Convert.toTimeStr(new Date(), null));
		getDataAccess().execUpdate(sql, DBUtils.getPrimaryKeyID("ST_MSG_COLLECT_LOG"), logPO.getMSG_ID(),
				logPO.getSTART_TIME(), logPO.getEND_TIME(), logPO.getBYTE_SIZE().get(), logPO.getMSG_NUM().get(),
				logPO.getSERVER_HOST());
	}

	// 错误日志
	public void writeCollectErrorLogs(final List<CollectErrorLogPO> errorLogs) {
		String sql = "INSERT INTO ST_MSG_COLLECT_ERROR_LOG"
				+ "(LOG_ID,MSG_ID,ERROR_TIME,ERROR_INFO,SERVER_HOST,SRC_CLIENT) " + "VALUES(?,?,?,?,?,?)";
		getDataAccess().execUpdateBatch(sql, new IParamsSetter() {
			@Override
			public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
				CollectErrorLogPO errorLogPO = errorLogs.get(i);
				preparedStatement.setLong(1, DBUtils.getPrimaryKeyID("ST_MSG_COLLECT_ERROR_LOG"));
				preparedStatement.setLong(2, errorLogPO.getMSG_ID());
				preparedStatement.setString(3, errorLogPO.getERROR_TIME());
				preparedStatement.setString(4, errorLogPO.getERROR_INFO());
				preparedStatement.setString(5, errorLogPO.getSERVER_HOST());
				preparedStatement.setString(6, errorLogPO.getSRC_CLIENT());
			}

			@Override
			public int batchSize() {
				return errorLogs.size();
			}
		});
	}

	// 发送日志
	public void writeCollectSendLogs(final List<CollectSendErrorLogPO> sendLogs) {
		String sql = "INSERT INTO ST_MSG_SEND_ERROR_LOG"
				+ "(LOG_ID,MSG_ID,STORE_ID,MSG_CONTENT,SEND_TIME,ERROR_DATA,SERVER_HOST) " + "VALUES(?,?,?,?,?,?,?)";
		getDataAccess().execUpdateBatch(sql, new IParamsSetter() {
			@Override
			public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
				CollectSendErrorLogPO kafkaLogPO = sendLogs.get(i);
				preparedStatement.setLong(1, DBUtils.getPrimaryKeyID("ST_MSG_SEND_ERROR_LOG"));
				preparedStatement.setLong(2, kafkaLogPO.getMSG_ID());
				preparedStatement.setLong(3, kafkaLogPO.getSTORE_ID());
				preparedStatement.setString(4, kafkaLogPO.getMSG_CONTENT());
				preparedStatement.setString(5, kafkaLogPO.getSEND_TIME());
				preparedStatement.setString(6, kafkaLogPO.getERROR_DATA());
				preparedStatement.setString(7, kafkaLogPO.getSERVER_HOST());
			}

			@Override
			public int batchSize() {
				return sendLogs.size();
			}
		});
	}

	// 一个统计日志
	public void reCordCollectTotalLog(CollectTotalLogPO totalLogPO, String hostName) {
		String sql = "SELECT LOG_ID,TOTAL_NUM,ERROR_NUM,SEND_NUM,SEND_ERROR_NUM FROM ST_MSG_COLLECT_TOTAL_LOG "
				+ "WHERE MSG_ID=? AND START_TIME=? AND SERVER_HOST=?";
		Object[][] arr = getDataAccess().queryForArray(sql, false,
				new Object[] { totalLogPO.getMSG_ID(), totalLogPO.getSTART_TIME(), hostName });
		AtomicLong totalNum = totalLogPO.getTOTAL_NUM();
		AtomicLong errorNum = totalLogPO.getERROR_NUM();
		AtomicLong sendNum = totalLogPO.getSEND_NUM();
		AtomicLong sendErrorNum = totalLogPO.getSEND_ERROR_NUM();
		long logId = 0;
		if (arr != null && arr.length == 1) {
			logId = Convert.toLong(arr[0][0]);
			totalNum.addAndGet(Convert.toLong(arr[0][1], 0));
			errorNum.addAndGet(Convert.toLong(arr[0][2], 0));
			sendNum.addAndGet(Convert.toLong(arr[0][3], 0));
			sendErrorNum.addAndGet(Convert.toLong(arr[0][4], 0));
			sql = "UPDATE ST_MSG_COLLECT_TOTAL_LOG SET TOTAL_NUM=?,ERROR_NUM=?,SEND_NUM=?,SEND_ERROR_NUM=?,STOP_TIME=?,STOP_FLAG=? "
					+ "WHERE LOG_ID=?";
			getDataAccess().execUpdate(sql, totalNum.get(), errorNum.get(), sendNum.get(), sendErrorNum.get(),
					totalLogPO.getSTOP_TIME(), totalLogPO.getSTOP_FLAG(), logId);
		} else {
			logId = DBUtils.getPrimaryKeyID("ST_MSG_COLLECT_TOTAL_LOG");
			sql = "INSERT INTO ST_MSG_COLLECT_TOTAL_LOG"
					+ "(LOG_ID,MSG_ID,START_TIME,STOP_TIME,TOTAL_NUM,ERROR_NUM,SEND_NUM,SEND_ERROR_NUM,SERVER_HOST,STOP_FLAG) "
					+ "VALUES(?,?,?,?,?,?,?,?,?,?)";
			getDataAccess().execUpdate(sql, logId, totalLogPO.getMSG_ID(), totalLogPO.getSTART_TIME(),
					totalLogPO.getSTOP_TIME(), totalNum.get(), errorNum.get(), sendNum.get(), sendErrorNum.get(),
					hostName, totalLogPO.getSTOP_FLAG());
		}
		totalNum.set(0);
		errorNum.set(0);
		sendNum.set(0);
		sendErrorNum.set(0);
	}

	// 停止之前的采集统计
	public void stopBeforeCollectTotalLog(String startTime, long msgId, String hostName) {
		String sql = "UPDATE ST_MSG_COLLECT_TOTAL_LOG SET STOP_FLAG=1," +
				"STOP_TIME=CASE WHEN STOP_TIME IS NULL THEN ? ELSE STOP_TIME END " +
				"WHERE MSG_ID=? AND SERVER_HOST=? AND UNIX_TIMESTAMP(START_TIME)<UNIX_TIMESTAMP('" + startTime + "')";
		getDataAccess().execUpdate(sql, startTime, msgId, hostName);
	}

	/**
	 * 查询节点统计信息
	 * 
	 * @param serverHost
	 *            hostName
	 * @param startTime
	 *            开始时间
	 * @return
	 */
	public Map<String, Map<String, Object>> queryNodeCollectTotalByTime(String serverHost, String startTime) {
		String sql = "SELECT MSG_ID," +
				"SUM(TOTAL_NUM) TOTAL_NUM,SUM(ERROR_NUM) ERROR_NUM,SUM(SEND_NUM) SEND_NUM,SUM(SEND_ERROR_NUM) SEND_ERROR_NUM " +
				"FROM ST_MSG_COLLECT_TOTAL_LOG " +
				"WHERE SERVER_HOST=? AND UNIX_TIMESTAMP(START_TIME)>=UNIX_TIMESTAMP('" + startTime + "') " +
				"GROUP BY MSG_ID";
		return getDataAccess().queryForMapMap(sql, "MSG_ID", serverHost);
	}

}
