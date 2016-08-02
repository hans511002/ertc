package com.ery.ertc.collect.remote.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.exception.CollectException;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.zk.NodeTracker;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.po.POConstant;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.RemotingUtils;
import com.ery.base.support.utils.TcpChannel;

public class SocketCollectServer extends CollectServer {

	private ServerSocketChannel ssc;// 套接字通道
	private Selector selector;// 事件选择器记录通道事件
	private Thread socketThread;// socket守护线程
	private boolean started;

	public SocketCollectServer() {
		socketThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// 通过此循环来遍例事件
					while (true) {
						// 查询事件如果一个事件都没有就阻塞
						selector.select();

						Iterator<SelectionKey> it = selector.selectedKeys().iterator();
						// 遍历事件
						while (it.hasNext()) {
							try {
								SelectionKey selKey = it.next();
								it.remove();// 将事件从当前通道删除

								// 连接事件(客户端连接服务器时产生)
								if (selKey.isAcceptable()) {
									ServerSocketChannel server = (ServerSocketChannel) selKey.channel();
									SocketChannel channel = server.accept(); // 实例化一个套接字通道，用于客户端通信

									if (channel != null) {
										ClientTh clientTh = new ClientTh(channel);
										clientTh.start();
									}
								}
							} catch (Exception e) {
								LogUtils.error("处理客户端请求出错!" + e.getMessage(), e);
							}
						}

					}
				} catch (Exception e) {
					LogUtils.error("Socket服务崩溃!" + e.getMessage(), e);
					started = false;
				}
			}
		});
	}

	@Override
	public void start() {
		if (started) {
			return;
		}
		try {
			selector = RemotingUtils.openSelector();
			ssc = ServerSocketChannel.open();
			// 将此socket对象设置为异步
			ssc.configureBlocking(false);
			// 定义存放监听端口的对象
			InetSocketAddress address = new InetSocketAddress(Config.getServerSocketPort());
			// 将服务器socket与这个端口绑定
			ssc.socket().bind(address);
			// 将异步的服务器socket对象的接受客户端连接事件注册到selector对象内
			ssc.register(selector, SelectionKey.OP_ACCEPT);

			socketThread.start();// 启动socket服务端守护线程
			started = true;
			NodeTracker.noticeTryNetServer(ZkConstant.NODE_SOCKET_PORT, Config.getServerSocketPort());
			LogUtils.info("消息接收Socket服务[" + Config.getHostName() + ":" + Config.getServerSocketPort() + "]启动 OK!");
		} catch (Exception e) {
			started = false;
			NodeTracker.noticeTryNetServer(ZkConstant.NODE_SOCKET_PORT, e.getMessage());
			LogUtils.error("启动Socket服务[" + Config.getServerSocketPort() + "]出错!" + e.getMessage(), e);
		}
	}

	@Override
	public void stop() {
		try {
			socketThread.stop();
			ssc.close();
			selector.close();
			started = false;
			LogUtils.info("消息接收Socket服务[" + Config.getHostName() + ":" + Config.getServerSocketPort() + "]停止 OK!");
		} catch (Exception e) {
			LogUtils.error("停止Socket服务出错!" + e.getMessage(), e);
		}
	}

	public boolean isStarted() {
		return started;
	}

	// 多客户端处理
	class ClientTh extends Thread {

		private static final int timeout = 20000;// 20秒读不出数据或未写入数据，则连接断开
		private SocketChannel sc;
		private final TcpChannel channel;

		ClientTh(SocketChannel sc) throws IOException {
			this.sc = sc;
			this.channel = new TcpChannel(sc, timeout, SelectionKey.OP_READ);
		}

		private ClientInfo getClientInfo() {
			ClientInfo clientInfo = new ClientInfo();
			Socket skt = sc.socket();
			clientInfo.setClientIp(skt.getInetAddress().getHostAddress());
			clientInfo.setClientHostName(skt.getInetAddress().getHostName());
			clientInfo.setSocketPort(skt.getPort());
			return clientInfo;
		}

		@Override
		public void run() {
			Map<String, Boolean> reg = new HashMap<String, Boolean>();
			ClientInfo ci = getClientInfo();
			try {
				long okNum = 0;
				try {// //////////////////////传输协议格式需要重新定义///////////////////////////////
					byte[] cache = new byte[8];// 取出8个字节，标示本次传输涉及的消息记录数
					channel.recv(ByteBuffer.wrap(cache));
					long total = Convert.toLong(cache);// 如果=-1，则表示无限字节流
					assert total > 0;// 如果不大于0，则出异常
					while (total == -1 || okNum < total) {
						MsgPO msgPO = null;
						cache = new byte[4];// 先读取4字节。标示消息长度
						channel.recv(ByteBuffer.wrap(cache));
						int len = Convert.toInt(cache);

						cache = new byte[Math.abs(len)];// 再读取真正的消息
						channel.recv(ByteBuffer.wrap(cache));
						try {
							// 如果len>=0，标示用字符json格式标示数据，如果<0标示用字节标示数据
							if (len >= 0) {
								String data = new String(cache);
								// 解析验证格式
								int idx = data.indexOf(",");// 第一个逗号之前是主题
								String topic = data.substring(0, idx);
								String content = data.substring(idx + 1);
								if (!reqHandlerMap.containsKey(topic)) {
									throw new CollectException("未找到相关主题[" + topic + "]处理规则,数据不能处理");
								}
								ServiceReqHandler srh = reqHandlerMap.get(topic);
								msgPO = srh.msgPO;
								if (!Convert.toBool(reg.get(topic), false)) {
									Map<String, AtomicInteger> numMap = ClientNumMap.get(topic);
									int maxConnNum = Convert.toInt(
											msgPO.getParamMap().get(POConstant.Msg_REQ_PARAM_socketMaxConnNum), -1);
									AtomicInteger num = numMap.get(ci.getClientIp());
									if (num == null) {
										synchronized (numMap) {
											numMap.put(ci.getClientIp(), new AtomicInteger(0));
										}
										num = numMap.get(ci.getClientIp());
									}
									if (maxConnNum != -1 && num.get() >= maxConnNum) {
										throw new CollectException("主题[" + topic + "]连接数超出服务器限制,连接被拒绝,请稍候再试!");
									}
									num.incrementAndGet();
									reg.put(topic, true);
								}
								try {
									String o = srh.doSocket(topic, content, ci);
									if ("ok".equals(o)) {
										okNum++;
									} else {
										writeToClient(okNum, o);
										return;
									}
									if (total == -1 && Convert.toBool(msgPO.getParamMap().get(""), false)) {
										// 持续的流，每成功接收一条回写一条记录
										writeToClient(okNum, "ok");
									}
								} catch (Exception e) {
									throw new CollectException("处理主题[" + topic + "]数据出错:" + e.getMessage(), e);
								}
							} else {
								byte[] b = new byte[2];
								int l = 0;
								System.arraycopy(cache, l, b, 0, b.length);
								l += b.length;

								short topiclen = Convert.toShort(b);
								b = new byte[topiclen];
								System.arraycopy(cache, l, b, 0, b.length);
								l += b.length;

								String topic = new String(b);
								if (reqHandlerMap.containsKey(topic)) {
									ServiceReqHandler srh = reqHandlerMap.get(topic);
									msgPO = srh.msgPO;
									if (!Convert.toBool(reg.get(topic), false)) {
										Map<String, AtomicInteger> numMap = ClientNumMap.get(topic);
										int maxConnNum = Convert.toInt(
												msgPO.getParamMap().get(POConstant.Msg_REQ_PARAM_socketMaxConnNum), -1);
										AtomicInteger num = numMap.get(ci.getClientIp());
										if (num == null) {
											synchronized (numMap) {
												numMap.put(ci.getClientIp(), new AtomicInteger(0));
											}
											num = numMap.get(ci.getClientIp());
										}
										if (maxConnNum != -1 && num.get() >= maxConnNum) {
											throw new CollectException("主题[" + topic + "]连接数超出服务器限制,连接被拒绝,请稍候再试!");
										}
										num.incrementAndGet();
										reg.put(topic, true);
									}
									try {
										List<Object> arr = QueueUtils.getMsgCollentInfo(msgPO);
										for (MsgFieldPO fieldPO : msgPO.getMsgFields()) {
											Object o = null;
											switch (fieldPO.getType()) {
											case 2:
												b = new byte[8];
												System.arraycopy(cache, l, b, 0, b.length);
												l += b.length;
												o = Convert.toLong(b);
												break;
											case 4:
												b = new byte[8];
												System.arraycopy(cache, l, b, 0, b.length);
												l += b.length;
												o = Convert.toDouble(b);
												break;
											case 8:
												b = new byte[8];
												System.arraycopy(cache, l, b, 0, b.length);
												l += b.length;
												o = Convert.toLong(b);
												break;
											case 16:
												b = new byte[4];
												System.arraycopy(cache, l, b, 0, b.length);
												l += b.length;

												b = new byte[Convert.toInt(b)];
												System.arraycopy(cache, l, b, 0, b.length);
												l += b.length;
												o = b;
											default:
												b = new byte[4];
												System.arraycopy(cache, l, b, 0, b.length);
												l += b.length;

												b = new byte[Convert.toInt(b)];
												System.arraycopy(cache, l, b, 0, b.length);
												l += b.length;
												o = new String(b);
											}
											arr.add(o);
										}
										if (!arr.isEmpty()) {
											if (DaemonMaster.daemonMaster.queueUtils.appendMsg(msgPO, arr)) {
												DaemonMaster.daemonMaster.workerService.notifyCollectOK(
														msgPO.getMSG_ID(), cache.length, ci.getClientIp());
											}
											okNum++;
											if (total == -1) {
												// 持续的流，每成功接收一条回写一条记录
												writeToClient(okNum, "ok");
											}
										} else {
											if (total == -1) {
												writeToClient(okNum, "不完整消息");
											}
										}
									} catch (Exception e) {
										throw new CollectException("处理主题[" + topic + "]数据出错:" + e.getMessage(), e);
									}
								} else {
									throw new CollectException("未找到相关主题[" + topic + "]处理规则,数据不能处理");
								}
							}
							channel.updateTimeout(timeout);// 为下一条消息设置超时时间
						} catch (CollectException e) {
							recordErrorMsg(msgPO, e.getMessage(), ci, e);
							writeToClient(okNum, e.getMessage());
							return;
						} catch (Exception e) {
							LogUtils.warn("解析[" + sc + "]数据出错!" + e.getMessage(), e);
							if (msgPO != null) {
								recordErrorMsg(msgPO, e.getMessage(), ci, e);
							}
							String str = "解析错误,本服务器接收两种合法字节流格式(用每条消息前消息长度正负值区分):\n"
									+ "1,每条消息的长度(正)4个字节,然后就是消息数据(普通字符串,第一个逗号之前的标示主题,之后是Json格式数据)\n"
									+ "2,每条消息的长度(负)4个字节,然后就是消息数据(字节数组【2,topic,4,field,4,field,4,field,……】\n"
									+ "   【2个字节标示主题长度,主题内容;4个字节标示字段1长度,字段1内容;4个字节标示字段2长度,字段2内容……依次类推】)";
							writeToClient(okNum, str);
							break;
						}
					}
					// 接收完,无异常(如果是持续流total=-1，是不可能走到这一步的)
					writeToClient(okNum, "ok");// 返回结果
				} catch (SocketTimeoutException ste) {
					LogUtils.warn("客户端[" + sc + "]连接超时!", ste);
				} catch (Exception e) {
					// 当客户端在读取数据操作执行之前断开连接会产生异常信息
					LogUtils.warn("接收客户端[" + sc + "]数据异常!" + e.getMessage(), e);
				}
			} finally {
				// 将连接计数-1
				for (String topic : reg.keySet()) {
					Map<String, AtomicInteger> numMap = ClientNumMap.get(topic);
					AtomicInteger num = numMap.get(ci.getClientIp());
					if (num != null) {
						num.addAndGet(-1);
					}
				}
				LogUtils.info("客户端[" + sc + "]连接结束");
				channel.cleanup();
			}
		}

		void recordErrorMsg(MsgPO msgPO, String content, ClientInfo clientInfo, Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			String estr = sw.toString();
			DaemonMaster.daemonMaster.workerService.notifyCollectError(msgPO.getMSG_ID(), estr,
					clientInfo.getClientIp());
		}

		/**
		 * 返回客户端数据，前4个字节是长度，后面是值
		 * 
		 * @param okNum
		 * @param resultData
		 *            返回结果数据(错误信息)
		 */
		void writeToClient(long okNum, String resultData) {
			try {
				byte[] bs = (okNum + "," + resultData).getBytes();
				channel.send(ByteBuffer.wrap(Convert.toBytes(bs.length)));
				channel.send(ByteBuffer.wrap(bs));
			} catch (Exception e) {
				LogUtils.error("返回[" + sc + "]数据发生异常!" + e.getMessage(), e);
			}
		}

	}

}
