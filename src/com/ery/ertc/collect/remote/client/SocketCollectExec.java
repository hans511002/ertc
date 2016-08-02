package com.ery.ertc.collect.remote.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.ery.ertc.collect.remote.StreamMsgHelper;
import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.TcpChannel;

public class SocketCollectExec extends CollectExec {

	private String host;
	private int port;

	public SocketCollectExec(AbstractTaskExecutor executor) {
		super(executor);
	}

	@Override
	public void execute() throws IOException {
		MsgPO msgPO = executor.getMsgPO();
		if (srcClient == null) {
			String[] arr = msgPO.getURL().split(":");
			host = arr[0];
			port = Convert.toInt(arr[1]);
			srcClient = host;
		}
		SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
		TcpChannel tcpChannel = new TcpChannel(socketChannel, reqTimeout, SelectionKey.OP_WRITE);
		try {
			byte[] bs = reqSendContent.getBytes(reqCharset);
			tcpChannel.send(ByteBuffer.wrap(bs));
			int batchSize = -1;
			if (isBatch) { // 批次
				bs = new byte[4];
				tcpChannel.recv(ByteBuffer.wrap(bs));
				batchSize = Convert.toInt(bs);// 批次大小
			}
			int i = 0;
			while (batchSize == -1 || i < batchSize) {
				bs = new byte[4];
				tcpChannel.recv(ByteBuffer.wrap(bs));
				int len = Convert.toInt(bs);// 1条消息的长度
				if (len == 0) {
					break;
				}
				bs = new byte[len];
				tcpChannel.recv(ByteBuffer.wrap(bs));
				if (!"binary".equals(contentType)) {
					String content = new String(bs, dataCharset);// 文本
					sendMsg(msgPO, content);
				} else {
					// 二进制数据
					StreamMsgHelper.sendMsgForBinary(msgPO, bs, srcClient);
				}
				i++;
			}

		} finally {
			tcpChannel.cleanup();
		}
	}
}
