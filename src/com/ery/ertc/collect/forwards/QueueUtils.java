package com.ery.ertc.collect.forwards;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;

import scala.collection.Seq;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.CollectConstant;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.exception.CollectException;
import com.ery.ertc.collect.exception.ConfigException;
import com.ery.ertc.collect.forwards.MsgQueue.StopThread;
import com.ery.ertc.collect.watch.CollectMonitor;
import com.ery.ertc.collect.worker.WorkerService;
import com.ery.ertc.collect.zk.StringZkSerializer;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.StringUtils;
import com.ery.base.support.utils.Utils;

public class QueueUtils {

	public final DaemonMaster master;
	public final WorkerService workerService;
	/**
	 * 缓冲队列 key=消息ID value=每个主题消息维护一个缓存队列
	 * 规则：服务器从各数据源采集到消息后，加入队列末尾，异步线程读取队列头部向kafka消息队列服务器发送
	 */
	private static final Map<Long, MsgQueue> bufferQueue = new HashMap<Long, MsgQueue>();

	public QueueUtils(DaemonMaster master) {
		this.master = master;
		this.workerService = master.workerService;
	}

	// 停止回调，因为队列不是立即停止
	private MsgQueue.QueueStartOrStopCall stopCall = new MsgQueue.QueueStartOrStopCall() {
		@Override
		public void call(MsgPO msgpo, int state) {
			synchronized (bufferQueue) {
				bufferQueue.remove(msgpo.getMSG_ID());
			}
		}
	};

	// 保证在停止时所有队列未发送收都被完全持久化
	public boolean hasActiveCacheQueue(boolean stopped) {
		if (stopped && bufferQueue.size() > 0) {
			List<Long> ids = new ArrayList<Long>();
			synchronized (bufferQueue) {
				for (Long id : bufferQueue.keySet()) {
					ids.add(id);
				}
			}
			for (long id : ids) {
				stopSendQueue(id);
			}
		}
		return bufferQueue.size() > 0;
	}

	public synchronized void startSendQueue(final MsgPO msgPO, final MsgQueue.QueueStartOrStopCall call) {
		// 初始转发，缓存队列
		MsgQueue cacheQueue = bufferQueue.get(msgPO.getMSG_ID());
		if (cacheQueue == null) {
			synchronized (bufferQueue) {
				cacheQueue = new MsgQueue(this.master, msgPO);
				bufferQueue.put(msgPO.getMSG_ID(), cacheQueue);
				cacheQueue.start(call);
			}
		} else if (cacheQueue.getState() != 1) {
			// 属于正在停止或完全停止状态
			LogUtils.debug("[" + msgPO.getMSG_TAG_NAME() + "]上次正在停止，等待中……，完全停止后再启动!");
			final MsgQueue cacheQueue_ = cacheQueue;
			new Thread() {
				public void run() {
					// 说明正在停止中
					while (cacheQueue_.getState() != 0) {
						Utils.sleep(50);
					}
					synchronized (bufferQueue) {
						MsgQueue cacheQueue = new MsgQueue(QueueUtils.this.master, msgPO);
						bufferQueue.put(msgPO.getMSG_ID(), cacheQueue);
						cacheQueue.start(call);
					}
				}
			}.start();
		}
	}

	// 停止一个消息队列
	public StopThread stopSendQueue(long msgId) {
		MsgQueue cacheQueue = bufferQueue.get(msgId);
		if (cacheQueue != null) {
			return cacheQueue.stop(stopCall);// MsgPO msgPo
		} else {
			MsgPO msgPo = WorkerService.msgPOMap.get(msgId);
			if (msgPo != null) {
				this.workerService.monitor.setRunStatus(msgPo, "stoped:" + CollectConstant.getNowString());
			} else {
				this.workerService.monitor.setRunStatus(msgId, "stoped:" + CollectConstant.getNowString());
			}
			return null;
		}
	}

	// public void stopSendQueue(MsgPO msgPo) {
	// MsgQueue cacheQueue = bufferQueue.get(msgPo.getMSG_ID());
	// if (cacheQueue != null) {
	// cacheQueue.stop(stopCall);// MsgPO msgPo
	// } else {
	// this.workerService.monitor.setRunStatus(msgPo, "stoped:" +
	// CollectConstant.getNowString());
	// }
	// }

	// 重启一个消息队列
	public void reStartSendQueue(MsgPO msgPo, MsgQueue.QueueStartOrStopCall call) {
		MsgQueue cacheQueue = bufferQueue.remove(msgPo.getMSG_ID());
		if (cacheQueue != null) {
			cacheQueue.stop(call);// 先停止原队列，再启动
		} else {
			call.call(msgPo, 1);// 直接启动
		}
	}

	/**
	 * 追加一条消息 执行过滤，去重，添加队列
	 * 
	 * @param msgPO
	 * @param tuples
	 * @return
	 */
	public boolean appendMsg(MsgPO msgPO, List<Object> tuples) {
		try {
			if (tuples == null || tuples.isEmpty())
				return false;
			// 加入队列
			MsgQueue cacheQueue = bufferQueue.get(msgPO.getMSG_ID());
			// 获取待发送的信息的key val
			cacheQueue.put(tuples);
			return true;
		} catch (Throwable ew) {
			// 写失败日志
			return false;
		}
		// //先过滤
		// Filter filter = filterMap.get(msgPO.getMSG_ID());
		// if(filter!=null){
		// if(filter.filter(tuples)){
		// //被过滤
		// WorkerService.notifyFilter(msgPO.getMSG_ID(),tuples);
		// CollectLog.filterLog(msgPO,tuples,logIdSn);
		// return true;
		// }
		// }
		// //再去重
		// Distinct distinct = distinctMap.get(msgPO.getMSG_ID());
		// if(distinct!=null){
		// if(distinct.distinct(tuples)){
		// //重复记录
		// WorkerService.notifyDistinct(msgPO.getMSG_ID(),tuples);
		// CollectLog.distinctLog(msgPO,tuples,logIdSn);
		// return true;
		// }
		// }

	}

	public static List<Object> getMsgCollentInfo(MsgPO msgPO) {
		List<Object> arr = new ArrayList<Object>(msgPO.getMsgFields().size() + 2);
		Long logIdSn = CollectMonitor.getNewLogId(msgPO.getMSG_ID());
		arr.add(logIdSn);
		arr.add(Config.getHostName());
		return arr;
	}

	/**
	 * 将消息字节数组转换成消息元组
	 * 
	 * @param po
	 * @param bytes
	 * @return
	 */
	public static Object[] getStoreJSONByTuples(MsgPO po, byte[] bytes) {
		Object[] tuples = new Object[po.getMsgFields().size()];
		int idx = 0;
		int i = 0;
		for (MsgFieldPO fieldPO : po.getMsgFields()) {
			byte[] b = null;
			switch (fieldPO.getType()) {
			case 2:
			case 8:
				b = new byte[8];
				System.arraycopy(bytes, idx, b, 0, 8);
				idx += 8;
				tuples[i++] = Convert.toLong(b);
				break;
			case 4:
				b = new byte[8];
				System.arraycopy(bytes, idx, b, 0, 8);
				idx += 8;
				tuples[i++] = Convert.toDouble(b);
				break;
			case 16:
				b = new byte[4];
				System.arraycopy(bytes, idx, b, 0, 4);
				idx += 4;
				b = new byte[Convert.toInt(b)];
				System.arraycopy(bytes, idx, b, 0, b.length);
				idx += b.length;
				tuples[i++] = Convert.toHexString(b);// 转换成16进制字符存储
			default:
				b = new byte[4];
				System.arraycopy(bytes, idx, b, 0, 4);
				idx += 4;
				b = new byte[Convert.toInt(b)];
				System.arraycopy(bytes, idx, b, 0, b.length);
				idx += b.length;
				tuples[i++] = new String(b);
			}
		}
		return tuples;
	}

	// 将json字符串转换成消息
	public static Object[] getTuplesByStoreJSON(MsgPO po, String str) {
		List<Object> list = JSON.parseArray(str);
		Object[] arr = new Object[po.getMsgFields().size()];
		int i = 0;
		for (MsgFieldPO fieldPO : po.getMsgFields()) {
			if (i < list.size()) {
				switch (fieldPO.getType()) {
				case 2:
				case 8:
					arr[i] = Convert.toLong(list.get(i));
					break;
				case 4:
					arr[i] = Convert.toDouble(list.get(i));
					break;
				case 16:
					arr[i] = Convert.toBytesForHexString(Convert.toString(list.get(i)));
					break;
				default:
					arr[i] = Convert.toString(list.get(i));
				}
				i++;
			}
		}
		return arr;
	}

	// 获取发送配置
	public static ProducerConfig getConfig(MsgPO po, MsgExtPO extPo) {
		Properties prop = new Properties();
		// prop.put("serializer.class", StringEncoder.class.getName());//值类型
		prop.put("serializer.class", BytesEncoder.class.getName());
		prop.put("key.serializer.class", StringEncoder.class.getName());
		prop.put("partitioner.class", FieldNumberPartitioner.class.getName());
		HostPort[] hosts = getAllBorkers(extPo);
		if (hosts == null) {
			throw new ConfigException("[" + po.getMSG_ID() + ":" + po.getMSG_NAME() + "] 存储Id【" + extPo.getSTORE_ID() +
					"】配置错误 需要输出到Kafka,ZK上未获取到kafka地址列表");
		}
		prop.put("metadata.broker.list", StringUtils.join(hosts, ","));

		// prop.put("metadata.broker.list", Config.getKafkaBrokersUrl());

		Map<String, String> paramPO = extPo.getStoreConfigParams();
		if (paramPO != null) {
			if (paramPO.containsKey("producer.type")) {// async sync
				prop.put("producer.type", paramPO.get("producer.type"));
			} else {
				prop.put("producer.type", "sync");
			}
			// 批次大小
			prop.put("batch.num.messages", Convert.toInt(paramPO.get("batch.num.messages"), Config.getSendQueueSize()) +
					""); // 默认
			// 压缩方式
			if (paramPO.containsKey("compression.codec")) {// gzip snappy
				prop.put("compression.codec", paramPO.get("compression.codec"));
			} else {
				prop.put("compression.codec", "none");
			}
			/**
			 * ##此值控制可用性级别，不同级别发送性能不同，可配三种级别（0,1，-1）。系统默认为0 ##
			 * 0：服务端领导者接受到数据后立即返回客户端标示（此时未完全持久化到磁盘，节点死了会造成部分数据丢失） ##
			 * 1：服务领导者完全持久化到磁盘后返回客户端标示（备份者未完全备份，尚未备份的数据可能丢失） ##
			 * -1：服务端等待所有备份完全同步后返回客户端标示（完全备份，只要保证服务端任意一个isr存活，则数据不会丢失）
			 * ##三种级别性能梯度：10->5>1 ##————————如果ISR列表全挂，客户端将收到异常，客户端设立失败重发机制
			 */
			if (paramPO.containsKey("request.required.acks")) {
				prop.put("request.required.acks", paramPO.get("request.required.acks"));
			} else {
				prop.put("request.required.acks", "0");
			}

			/**
			 * 分区规则,格式：【typeid:规则】 【0】 ——0或空标示使用默认分区
			 * 【1:fileName】——使用字段的值来分区取模,（fieldName只能是一个字段）
			 * 【2:fileName】——使用字段的值的hashCode来分区取模
			 * ,（fieldName可以是多个字段用逗号分割，取值时先将值相加再hashCode）
			 */
			if (paramPO.containsKey("PARTITION_RULE") && !"".equals(paramPO.get("PARTITION_RULE"))) {
				if (paramPO.get("PARTITION_RULE").startsWith("2:")) {
					prop.put("partitioner.class", FieldStringPartitioner.class.getName());
				}
			}
		} else {
			prop.put("producer.type", "sync");
			prop.put("request.required.acks", "0");
		}
		LogUtils.info("new ProducerConfig=" + prop.toString());
		return new ProducerConfig(prop);
	}

	public static String converToString(HostPort[] hosts) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < hosts.length; i++) {
			sb.append(hosts[i].host + ":" + hosts[i].port + ",");
		}
		return sb.substring(0, sb.length() - 1);
	}

	public static HostPort[] getAllBorkers(MsgExtPO extPo) {
		try {
			Map<String, String> storeParams = extPo.getStoreConfigParams();
			String zkurl = Convert.toString(storeParams.get("kafka.zk.url"), Config.getZkUrl());
			String kafkaRoot = Convert.toString(storeParams.get("kafka.zk.root"), Config.getKafkaZkRoot());
			int sessionTms = Convert.toInt(storeParams.get("kafka.zk.session.timeout"), Config.getZkSessionTOMS());
			int connectTms = Convert.toInt(storeParams.get("kafka.zk.connect.timeout"), Config.getZkConnectTOMS());
			LogUtils.info("KafkaZkRoot=" + zkurl + kafkaRoot + " ZkSessionTOMS=" + sessionTms + " ZkConnectTOMS=" +
					connectTms);
			ZkClient zkClient = new ZkClient(zkurl, sessionTms, connectTms, new StringZkSerializer());
			String brokerInfoPath = kafkaRoot + "/brokers/ids";
			Seq<String> children = ZkUtils.getChildren(zkClient, brokerInfoPath);
			// .getChildren().forPath(brokerInfoPath);
			if (children == null || children.size() == 0)
				return null;
			HostPort[] hosts = new HostPort[children.size()];
			int len = 0;
			for (int i = 0; i < children.size(); i++) {
				String id = children.apply(i);
				String path = brokerInfoPath + "/" + id;
				String hostPortData = zkClient.readData(path);
				// _curator.getData().forPath(path);
				hosts[len++] = getBrokerHost(hostPortData);
			}
			return hosts;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private static HostPort getBrokerHost(String contents) {
		// ObjectMapper map = new ObjectMapper();
		Map<Object, Object> value = null;
		try {
			value = (Map<Object, Object>) CollectConstant.objectMapper.readValue(contents, Map.class);
			// value = (Map<Object, Object>) map.readValue(new String(contents,
			// "UTF-8"), Map.class);
			String host = Convert.toString(value.get("host"));
			Integer port = Convert.toInt(value.get("port"));
			return new HostPort(host, port);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// 获取发送器
	public static Producer getProducer(MsgPO po, MsgExtPO extPo) {
		return new Producer<String, byte[]>(getConfig(po, extPo));
	}

	/**
	 * 将数批量数据转换为消息数组
	 * 
	 * @param data
	 *            批次数据
	 * @param type
	 * @param splitCh
	 * @return
	 */
	public static String[] convertDataToContents(String data, String type, String splitCh) {
		if (type.contains("array")) {
			List<String> d = JSON.parseArray(data, String.class);
			String[] arr = new String[d.size()];
			d.toArray(arr);
			return arr;
		} else {
			return StringUtils.split(data, splitCh);
		}
	}

	/**
	 * 将一串消息数据转换成消息元组。根据字段定义规则
	 * 
	 * @param msgPO
	 * @param type
	 *            类型
	 * @param content
	 *            字符数据
	 * @param splitCh
	 *            二次拆分符 (如果需要)
	 * @return
	 */
	public static List<Object> convertMsgToTuple(MsgPO msgPO, String type, String content, String splitCh) {
		if (content == null && "".equals(content))
			return null;
		List<Object> arr = QueueUtils.getMsgCollentInfo(msgPO);
		if (type.contains("array")) {
			List<Object> data = JSON.parseArray(content);
			int i = 0;
			for (MsgFieldPO fieldPO : msgPO.getMsgFields()) {
				// SRC_FIELD标示数组索引
				Object tmp = data.get(Convert.toInt(fieldPO.getSRC_FIELD(), i));
				arr.add(fmtData(fieldPO, tmp));
				i++;
			}
		} else if (type.contains("map")) {
			Map<String, Object> data = JSON.parseObject(content);
			int i = 0;
			for (MsgFieldPO fieldPO : msgPO.getMsgFields()) {
				// SRC_FIELD标示map键
				Object tmp = data.get(fieldPO.getSRC_FIELD());
				arr.add(fmtData(fieldPO, tmp));
				i++;
			}
		} else {
			// 复杂匹配，首先会在数据解析内定义一个二次拆分符，虽然未必会用到
			String[] conts = StringUtils.split(content, splitCh);
			int idx = -1;
			for (int i = 0; i < msgPO.getMsgFields().size(); i++) {
				MsgFieldPO fieldPO = msgPO.getMsgFields().get(i);
				String srcf = fieldPO.getSRC_FIELD();
				if (srcf.startsWith("mac:")) {
					srcf = srcf.substring(4).toUpperCase();
					if (srcf.equals(MsgFieldPO.MAC_MSG_ID)) {
						arr.add(msgPO.getMSG_ID());
					} else if (srcf.equals(MsgFieldPO.MAC_HOST)) {
						arr.add(Config.getHostName());
					} else if (srcf.equals(MsgFieldPO.MAC_COLLECT_TIME)) {
						arr.add(new Date().getTime());
					}
					continue;
				} else if (srcf.startsWith("reg:")) {
					srcf = srcf.substring(4);
					idx = srcf.indexOf("~");
					String regex = srcf.substring(idx + 1);// 匹配串
					Matcher matcher = Pattern.compile(regex).matcher(content);
					if (matcher.find()) {
						arr.add(matcher.replaceAll(srcf.substring(0, idx)));
						// idx = Convert.toInt(srcf.substring(0, idx));
						// arr[i] = Convert.toString(matcher.group(idx));
					}
					continue;
				} else if (srcf.startsWith("idx:")) {
					srcf = srcf.substring(4);
				}
				if ((idx = srcf.indexOf(",")) != -1) {// 需要二次处理
					String ra = srcf.substring(0, idx);// 可能是 idx or idx1-idx3
														// or idx-?
					int idx_ = ra.indexOf("-");
					String v;
					if (idx_ != -1) {
						int s = Convert.toInt(ra.substring(0, idx_));
						int e = Convert.toInt(ra.substring(idx_ + 1), conts.length - 1);
						String str = conts[s];
						for (int x = s + 1; x <= e; x++) {
							str += splitCh + conts[x];
						}
						v = str;
					} else {
						v = conts[Convert.toInt(srcf.substring(0, idx))];// 取一次拆分后的值
																			// idx,{rule}
					}
					srcf = srcf.substring(idx + 1);// type:{rule}
					if (srcf.startsWith("split:")) {// 二次拆分
						srcf = srcf.substring(6);// idx:spch
						idx = srcf.indexOf(":");
						int idx2 = Convert.toInt(srcf.substring(0, idx));// 二次拆分后的索引
						srcf = srcf.substring(idx + 1);// 二次拆分符
						String[] aaa = v.split(srcf);
						arr.add(aaa[idx2]);
					} else if (srcf.startsWith("substr_s:")) {// 二次提取前半截
						srcf = srcf.substring(9);// 提取符
						if ((idx = v.indexOf(srcf)) != -1) {
							arr.add(v.substring(0, idx));
						} else {
							arr.add("");
						}
					} else if (srcf.startsWith("substr_e:")) {// 二次提取后半截
						srcf = srcf.substring(9);// 提取符
						if ((idx = v.indexOf(srcf)) != -1) {
							arr.add(v.substring(idx + 1));
						} else {
							arr.add("");
						}
					}
				} else if ((idx = srcf.indexOf("-")) != -1) {// 范围
					int s = Convert.toInt(srcf.substring(0, idx));
					int e = Convert.toInt(srcf.substring(idx + 1), conts.length - 1);
					String str = conts[s];
					for (int x = s + 1; x <= e; x++) {
						str += splitCh + conts[x];
					}
					arr.add(str);
				} else {
					arr.add(conts[Convert.toInt(srcf)]);
				}
			}
		}
		return arr;
	}

	// 格式化数据
	static Object fmtData(MsgFieldPO fieldPO, Object data) {
		try {
			switch (fieldPO.getType()) {
			case 2:
				return Convert.toLong(data);
			case 4:
				return Convert.toDouble(data);
			case 8:
				if (data instanceof java.util.Date || data instanceof java.sql.Date ||
						data instanceof java.sql.Timestamp) {
					data = ((java.util.Date) data).getTime();
				} else if (data instanceof Long) {
					data = Convert.toLong(data);
				} else {
					try {
						return fieldPO.getSimpleDateFormat().parse(data.toString()).getTime();
					} catch (Exception e) {
						return Convert.toLong(data, 0);
					}
				}
				break;
			case 16:
				if (data instanceof String) {
					String hexStr = Convert.toString(data);
					return Convert.toBytesForHexString(hexStr);
				} else if (data instanceof byte[]) {
					return data;
				}
				break;
			default:
				return Convert.toString(data);
			}
		} catch (Exception ex) {
			throw new CollectException("消息[" + fieldPO.getMSG_ID() + "]字段[" + fieldPO.getFIELD_NAME() + "]数据[" + data +
					"]格式与定义[" + fieldPO.getFIELD_DATA_TYPE() + "]不符!", ex);
		}
		throw new CollectException("消息[" + fieldPO.getMSG_ID() + "]字段[" + fieldPO.getFIELD_NAME() + "]数据[" + data +
				"]格式与定义[" + fieldPO.getFIELD_DATA_TYPE() + "]不符!");
	}

}
