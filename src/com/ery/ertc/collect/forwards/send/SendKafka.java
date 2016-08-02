package com.ery.ertc.collect.forwards.send;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.I0Itec.zkclient.ZkClient;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.forwards.MsgCache;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.forwards.Send;
import com.ery.ertc.collect.zk.StringZkSerializer;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class SendKafka extends Send {

	private static Random random = new Random();
	public static final int DEFAULT_PART_NUM = 3;
	public static final int DEFAULT_PART_REPLICA_NUM = 2;
	public static final int DEFAULT_ASYNC_BATCH_NUM = 200;

	public Producer producer;
	public boolean isBeachSend = false;
	public int beachSize = 1000;

	public SendKafka(MsgPO msgPO, MsgExtPO extPo, MsgCache cache) {
		super(msgPO, extPo, cache);
		producer = QueueUtils.getProducer(msgPO, extPo);
		createOrModifyKafkaTopic();// 同步kafka主题元数据
		String sendType = extPo.getMsgStoreParam("producer.type");
		beachSize = Convert.toInt(extPo.getMsgStoreParam("batch.num.messages"), 1000);
		if (sendType != null && sendType.equals("async"))
			isBeachSend = true;
		if (beachSize > 0)
			isBeachSend = true;
	}

	// 创建或修改Kafka主题分区
	private void createOrModifyKafkaTopic() {
		try {
			Map<String, String> storeParams = extPo.getStoreConfigParams();
			int numPartition = Convert.toInt(extPo.getMsgStoreParam("PARTITION_NUM"), DEFAULT_PART_NUM);
			int numReplica = Convert.toInt(extPo.getMsgStoreParam("PARTITION_REPLICA_NUM"), DEFAULT_PART_NUM);
			String zkurl = Convert.toString(storeParams.get("kafka.zk.url"), Config.getZkUrl());
			String kafkaRoot = Convert.toString(storeParams.get("kafka.zk.root"), Config.getKafkaZkRoot());
			int sessionTms = Convert.toInt(storeParams.get("kafka.zk.session.timeout"), Config.getZkSessionTOMS());
			int connectTms = Convert.toInt(storeParams.get("kafka.zk.connect.timeout"), Config.getZkConnectTOMS());
			if (kafkaRoot.startsWith("/")) {
				zkurl += kafkaRoot;
			} else {
				zkurl += "/" + kafkaRoot;
			}
			LogUtils.info("KafkaZkRoot=" + zkurl + " ZkSessionTOMS=" + sessionTms + " ZkConnectTOMS=" + connectTms);
			ZkClient zkClient = new ZkClient(zkurl, sessionTms, connectTms, new StringZkSerializer());
			if (zkClient.exists("/brokers/topics/" + msgPO.getMSG_TAG_NAME())) {
				// 存在主题,读取主题分区信息，看是否涉及添加分区（不可删除分区，不可添加备份数）
				List<String> strings = zkClient.getChildren("/brokers/topics/" + msgPO.getMSG_TAG_NAME() +
						"/partitions");

				if (strings.size() < numPartition) {// 需要添加分区
					String[] options = new String[] { "--alter", "--zookeeper", zkurl, "--partitions",
							numPartition + "", "--topic", msgPO.getMSG_TAG_NAME(), "--replication-factor",
							numReplica + "" };
					// TopicCommand.main(options);
					TopicCommand.TopicCommandOptions ops = new TopicCommandOptions(options);
					TopicCommand.alterTopic(zkClient, ops);
					// AddPartitionsCommand.addPartitions(zkClient,
					// msgPO.getMSG_TAG_NAME(),
					// numPartition - strings.size(), "");
					LogUtils.info("消息主题[" + msgPO.getMSG_TAG_NAME() + "]添加" + (numPartition - strings.size()) +
							"个分区 OK!");
				} else if (strings.size() > numPartition) {
					LogUtils.warn("消息主题[" + msgPO.getMSG_TAG_NAME() + "]已建立分区[" + strings.size() + "],不可减少为[" +
							numPartition + "]");
				}
			} else {
				// 不存在，执行命令新建
				// "--alter", "--describe", "--list", "--config", "x=y"
				// "--deleteConfig","x"
				String[] options = new String[] { "--create", "--zookeeper", zkurl, "--partitions", numPartition + "",
						"--topic", msgPO.getMSG_TAG_NAME(), "--replication-factor", numReplica + "" };
				// TopicCommand.main(options);
				TopicCommand.TopicCommandOptions ops = new TopicCommandOptions(options);
				TopicCommand.createTopic(zkClient, ops);
			}

		} catch (Exception ex) {
			LogUtils.error("同步Kafka主题[" + msgPO.getMSG_TAG_NAME() + "]信息出错!", ex);
		}
	}

	/**
	 * 发送至kafka实现逻辑，自己决定单发还是批量发
	 */
	@Override
	public int send() throws IOException {
		if (isBeachSend) {// 异步批次
			return sendMore();
		} else {
			return sendOne();
		}
	}

	static byte[] checkLength(byte[] b, int len, int size) {
		if (b.length - len < size) { // 剩余位置不足以装下本次的值
			b = Utils.expandVolume(b, size);// 扩大容量
		}
		return b;
	}

	/**
	 * 获取消息分区key和内容字符
	 * 
	 * @param po
	 * @param tuples
	 * @param logIdSn
	 * @return 包含两个元素的数组，arr0=分区key,arr1=发送内容
	 */
	public static byte[][] getKeyOrContentForTuples(MsgPO po, MsgExtPO extPo, Object[] tuples) {
		String partationKeyfileds = extPo.getPartFieldStrs();
		String key = "";
		byte[] b = new byte[4096];// 先用4kb大小存储，不够再倍扩
		byte[] bs_ = null;
		int i = 2;
		int len = 0;
		String sv = Convert.toString(tuples[0]);
		byte[] bs = sv.getBytes();
		System.arraycopy(Convert.toBytes(bs.length), 0, b, len, 4);
		len += 4;
		System.arraycopy(bs, 0, b, len, bs.length);
		len += bs.length;
		long lid = Convert.toLong(tuples[1]);
		bs = Convert.toBytes(lid);
		System.arraycopy(bs, 0, b, len, bs.length);
		len += bs.length;
		for (MsgFieldPO fieldPO : po.getMsgFields()) {
			bs = null;
			switch (fieldPO.getType()) {
			case 2:
				long v = Convert.toLong(tuples[i]);
				bs = Convert.toBytes(v);
				if (partationKeyfileds.equals(fieldPO.getFIELD_NAME())) {
					key = v + "";
				} else if (partationKeyfileds.contains("," + fieldPO.getFIELD_NAME() + ",")) {
					key += v;
				}
				b = checkLength(bs, len, bs.length);
				System.arraycopy(bs, 0, b, len, bs.length);
				len += bs.length;
				break;
			case 4:
				double dv = Convert.toDouble(tuples[i]);
				bs = Convert.toBytes(dv);
				if (partationKeyfileds.equals(fieldPO.getFIELD_NAME())) {
					key = dv + "";
				} else if (partationKeyfileds.contains("," + fieldPO.getFIELD_NAME() + ",")) {
					key += dv;
				}
				b = checkLength(bs, len, bs.length);
				System.arraycopy(bs, 0, b, len, bs.length);
				len += bs.length;
				break;
			case 8: // 时间，必须保证调用此方法时，数据已被转换成数字
				long tv = Convert.toLong(tuples[i]);
				bs = Convert.toBytes(tv);
				if (partationKeyfileds.equals(fieldPO.getFIELD_NAME())) {
					key = tv + "";
				} else if (partationKeyfileds.contains("," + fieldPO.getFIELD_NAME() + ",")) {
					key += tv;
				}
				b = checkLength(bs, len, bs.length);
				System.arraycopy(bs, 0, b, len, bs.length);

				len += bs.length;
				break;
			case 16:
				bs_ = (byte[]) tuples[i];
				bs = Convert.toBytes(bs_.length);
				b = checkLength(bs, len, bs.length);
				System.arraycopy(bs, 0, b, len, bs.length);
				len += bs.length;
				b = checkLength(bs_, len, bs_.length);
				System.arraycopy(bs_, 0, b, len, bs_.length);
				len += bs_.length;
				break;
			default:
				sv = Convert.toString(tuples[i]);
				bs_ = sv.getBytes();
				bs = Convert.toBytes(bs_.length);
				b = checkLength(bs, len, bs.length);
				System.arraycopy(bs, 0, b, len, bs.length);
				len += bs.length;
				b = checkLength(bs_, len, bs_.length);
				System.arraycopy(bs_, 0, b, len, bs_.length);
				len += bs_.length;
				if (partationKeyfileds.equals(fieldPO.getFIELD_NAME())) {
					key = sv;
				} else if (partationKeyfileds.contains("," + fieldPO.getFIELD_NAME() + ",")) {
					key += sv;
				}
			}
			i++;
		}
		if (len < b.length) {
			byte[] b1 = new byte[len];
			System.arraycopy(b, 0, b1, 0, len);
			b = b1;
		}

		if ("".equals(key)) {
			key = Convert.toString(random.nextInt(Integer.MAX_VALUE));
		}

		byte[] ks = null;
		if (extPo.getPartKeyIsStr()) {
			ks = key.getBytes();
		} else {
			ks = Convert.toBytes(Convert.toLong(key));
		}
		return new byte[][] { ks, b };
	}

	// 获取发送对象
	public static KeyedMessage buildMessage(MsgPO po, MsgExtPO extPo, byte[][] kvs) {
		byte[] key = kvs[0];
		byte[] content = kvs[1];
		if (content == null)
			return null;
		if (extPo.getPartKeyIsStr()) {
			return new KeyedMessage<String, byte[]>(po.getMSG_TAG_NAME(), new String(key), content);
		} else {
			return new KeyedMessage<String, byte[]>(po.getMSG_TAG_NAME(), Convert.toLong(key) + "", content);
		}
	}

	// 发送一个
	private int sendOne() throws IOException {
		Object[] kvs = getOne();
		if (kvs == null)
			return 0;
		try {
			// 转节烈序列
			byte[][] kv = getKeyOrContentForTuples(msgPO, extPo, kvs);
			KeyedMessage km = buildMessage(msgPO, extPo, kv);
			if (sendPlugin != null) {
				ISendPlugin.beforeSend(sendPlugin, kv, kvs);
			}
			producer.send(km);
			DaemonMaster.daemonMaster.workerService.notifySendOK(msgId, stroeId, 1, kv[0].length + kv[1].length);
			sendOK(kvs);
			return 1;
		} catch (Exception e) {
			createOrModifyKafkaTopic();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			DaemonMaster.daemonMaster.workerService.notifySendError(msgId, stroeId, sw.toString(), kvs);
			LogUtils.error("发送向Kafka出错!" + e.getMessage(), e);
			sendFail(kvs);
			throw new IOException(e);
		}
	}

	// 发送多个
	private int sendMore() throws IOException {
		List<Object[]> todoSends = getMore(beachSize);
		if (todoSends == null || todoSends.size() == 0) {
			return 0;
		}
		try {
			List<KeyedMessage> kms = new ArrayList<KeyedMessage>();
			long byteSize = 0;
			for (Object[] kv_ : todoSends) {
				byte[][] kv = getKeyOrContentForTuples(msgPO, extPo, kv_);
				if (sendPlugin != null) {
					ISendPlugin.beforeSend(sendPlugin, kv, kv_);
				}
				KeyedMessage km = buildMessage(msgPO, extPo, kv);
				if (km != null) {
					kms.add(km);
					// todoSends.add(kv_);
					byteSize += kv[0].length + kv[1].length;
				}
			}
			producer.send(kms);
			DaemonMaster.daemonMaster.workerService.notifySendOK(msgId, stroeId, kms.size(), byteSize);
			sendOK(todoSends);
			return kms.size();
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			DaemonMaster.daemonMaster.workerService.notifySendError(msgId, stroeId, sw.toString(), todoSends);
			LogUtils.error("发送向Kafka出错!" + e.getMessage(), e);
			sendFail(todoSends);
			// Utils.sleep(1000);
			throw new IOException(e);
		}
	}

	public void stop() {
		try {
			super.stop();
			producer.close();
		} catch (Exception e) {
		}
	}
}
