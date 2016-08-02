package com.ery.ertc.collect.ui;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Server;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.remote.server.ServiceReqHandler;
import com.ery.ertc.collect.ui.servlet.NodeInfoServlet;
import com.ery.ertc.collect.ui.servlet.RootServlet;
import com.ery.ertc.collect.ui.servlet.TaskInfoServlet;
import com.ery.ertc.collect.zk.NodeTracker;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.DataSourceManager;

public class UIServer {

	private DaemonMaster daemonMaster;
	private Server server;
	private Handler handler;
	private Map<String, ServiceReqHandler> servletMap = new HashMap<String, ServiceReqHandler>();
	private boolean started;

	public UIServer(DaemonMaster daemonMaster) {
		this.daemonMaster = daemonMaster;
		this.server = new Server(Config.getUiPort());
		handler = new Handler() {
			@Override
			public void handle(String s, HttpServletRequest request, HttpServletResponse response, int i)
					throws IOException, ServletException {
				String charset = "UTF-8";
				if (servletMap.containsKey(s)) {
					charset = servletMap.get(s).getCharset();
				}
				response.setContentType("text/html;charset=" + charset);
				PrintWriter out = response.getWriter();
				String str = "";
				try {
					if (servletMap.containsKey(s)) {
						str = servletMap.get(s).doHttp(request, response);
					} else {
						str = "未找到" + s + "页面!";
					}
				} catch (Exception e) {
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					e.printStackTrace(pw);
					str = "您访问的采集监控UI界面:[" + s + "]出错:" + sw.toString();
				} finally {
					out.print(new String(str.getBytes()).replaceAll("\n", "<br>"));
					out.flush();
					out.close();
					DataSourceManager.destroy();
				}
			}
		};
		server.setHandler(handler);
		initServlets();
	}

	public void start() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					server.start();
					LogUtils.info("UI[http://" + Config.getHostName() + ":" + Config.getUiPort() + "]启动 OK!");
					started = true;
					NodeTracker.noticeTryNetServer(ZkConstant.NODE_UI_PORT, Config.getUiPort());
					server.join();
				} catch (Exception e) {
					started = false;
					NodeTracker.noticeTryNetServer(ZkConstant.NODE_UI_PORT, e.getMessage());
					LogUtils.error("启动UI[" + Config.getUiPort() + "]服务出错!" + e.getMessage(), e);
				}
			}
		}).start();
	}

	public void stop() {
		try {
			started = false;
			server.stop();
		} catch (Exception e) {
			LogUtils.error("停止UI服务出错!" + e.getMessage(), e);
		}
	}

	private void initServlets() {
		servletMap.put("/", new RootServlet(daemonMaster));
		servletMap.put("/node", new NodeInfoServlet(daemonMaster));
		servletMap.put("/task", new TaskInfoServlet(daemonMaster));
	}

	public boolean isStarted() {
		return started;
	}
}
