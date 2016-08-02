package com.ery.ertc.collect.remote.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Server;
import org.mortbay.thread.QueuedThreadPool;

import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.zk.NodeTracker;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.base.support.log4j.LogUtils;

public class HttpCollectServer extends CollectServer {

	private Server server;
	private HttpHandler handler;
	private boolean started;

	public HttpCollectServer() {
		server = new Server(Config.getServerHttpPort());
		handler = new HttpHandler();
		server.addHandler(handler);
	}

	@Override
	public void start() {
		if (started) {
			return;
		}
		try {
			QueuedThreadPool thp = new QueuedThreadPool(5);
			thp.setMinThreads(2);
			thp.setName("http");
			server.setThreadPool(thp);
			server.start();
			started = true;
			NodeTracker.noticeTryNetServer(ZkConstant.NODE_HTTP_PORT, Config.getServerHttpPort());
			LogUtils.info("消息接收HTTP服务[http://" + Config.getHostName() + ":" + Config.getServerHttpPort() +
					"/{topicName}]启动 OK!");
			// server.join();
		} catch (Exception e) {
			started = false;
			NodeTracker.noticeTryNetServer(ZkConstant.NODE_HTTP_PORT, e.getMessage());
			LogUtils.error("启动HTTP服务[" + Config.getServerHttpPort() + "]出错:" + e.getMessage(), e);
		}
	}

	@Override
	public void stop() {
		try {
			server.stop();
			started = false;
			LogUtils.info("消息接收HTTP服务[http://" + Config.getHostName() + ":" + Config.getServerHttpPort() +
					"/{topicName}]停止 OK!");
		} catch (Exception e) {
			LogUtils.error("停止HTTP服务出错:" + e.getMessage(), e);
		}
	}

	public boolean isStarted() {
		return started;
	}

	public Server getServer() {
		return server;
	}

	/**
	 * http请求处理分发
	 */
	class HttpHandler extends com.ery.ertc.collect.ui.Handler {

		@Override
		public void handle(String path, HttpServletRequest request, HttpServletResponse response, int i)
				throws IOException, ServletException {
			String charset = "UTF-8";
			String topic_ = path.substring(1);
			if (reqHandlerMap.containsKey(topic_)) {
				charset = reqHandlerMap.get(topic_).getCharset();
			}
			response.setContentType("text/html;charset=" + charset);
			// response.setContentType("binary/octet-stream");
			PrintWriter out = response.getWriter();
			String str = "";
			try {
				// 用path区分MsgTag
				if (reqHandlerMap.containsKey(topic_)) {
					LogUtils.debug("接收到HTTP数据 大小=" + request.getContentLength());
					str = reqHandlerMap.get(topic_).doHttp(request, response);
				} else {
					str = "未找到相关主题[" + topic_ + "]业务,数据不能处理!";
				}
			} catch (Exception e) {
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				str = sw.toString();
				LogUtils.error("处理客户端[" + request.getRemoteHost() + "]数据异常", e);
			} finally {
				try {
					out.print(new String(str.getBytes()));
				} catch (Exception e) {
					LogUtils.error("向客户端print数据出错!", e);
				}
				out.flush();
				out.close();
			}
		}

	}
}
