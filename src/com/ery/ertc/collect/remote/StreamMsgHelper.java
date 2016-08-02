package com.ery.ertc.collect.remote;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.utils.Convert;

public class StreamMsgHelper {

	public static void parseStream(MsgPO msgPO, InputStream in, boolean isBatch, String srcClient) throws IOException {
		int batchSize = -1;
		byte[] b = null;
		if (isBatch) { // 批次
			b = new byte[4];
			in.read(b);
			batchSize = Convert.toInt(b);
		}
		int i = 0;
		while (batchSize == -1 || i < batchSize) {
			b = new byte[4];
			in.read(b);
			int len = Convert.toInt(b);
			if (len == 0)
				break;
			b = new byte[len];
			int x = in.read(b);
			if (x == b.length) {
				sendMsgForBinary(msgPO, b, srcClient);
			} else if (x != -1 && x < b.length) {
				throw new RuntimeException("数据不全");
			}
			i++;
		}
		in.close();
	}

	/**
	 * 发送消息.接收到的二进制数据
	 * 
	 * @param msgPO
	 * @param msg
	 * @param srcClient
	 */
	public static void sendMsgForBinary(MsgPO msgPO, byte[] msg, String srcClient) {
		if (msg == null || msg.length == 0) {
			return;
		}
		List<Object> arr = QueueUtils.getMsgCollentInfo(msgPO);
		byte[] b = null;
		int l = 0;
		for (MsgFieldPO fieldPO : msgPO.getMsgFields()) {
			switch (fieldPO.getType()) {
			case 2:// 数字
			case 8:// 时间毫秒数
				b = new byte[8];
				System.arraycopy(msg, l, b, 0, b.length);
				l += b.length;
				arr.add(Convert.toLong(b));
				break;
			case 4:// 小数
				b = new byte[8];
				System.arraycopy(msg, l, b, 0, b.length);
				l += b.length;
				arr.add(Convert.toDouble(b));
				break;
			case 16:// 二进制数据
				b = new byte[4];
				System.arraycopy(msg, l, b, 0, b.length);
				l += b.length;

				b = new byte[Convert.toInt(b)];
				System.arraycopy(msg, l, b, 0, b.length);
				l += b.length;
				arr.add(b);
			default:// 字符
				b = new byte[4];
				System.arraycopy(msg, l, b, 0, b.length);
				l += b.length;

				b = new byte[Convert.toInt(b)];
				System.arraycopy(msg, l, b, 0, b.length);
				l += b.length;
				arr.add(new String(b));
			}
		}
		if (DaemonMaster.daemonMaster.queueUtils.appendMsg(msgPO, arr)) {
			DaemonMaster.daemonMaster.workerService.notifyCollectOK(msgPO.getMSG_ID(), msg.length, srcClient);
		}
	}

}
