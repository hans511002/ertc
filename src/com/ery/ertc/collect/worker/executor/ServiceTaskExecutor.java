package com.ery.ertc.collect.worker.executor;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.ery.ertc.collect.remote.StreamMsgHelper;
import com.ery.ertc.collect.remote.server.ClientInfo;
import com.ery.ertc.collect.remote.server.ServiceReqHandler;
import com.ery.ertc.collect.worker.WorkerService;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.po.POConstant;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;

public class ServiceTaskExecutor extends AbstractTaskExecutor {

	public ServiceTaskExecutor(MsgPO msgPO, WorkerService workerService) {
		this.msgPO = msgPO;
		this.workerService = workerService;
	}

	@Override
	public void start() {
		switch (msgPO.getTRIGGER_TYPE()) {
		case POConstant.TRI_TYPE_P_HTTP:
			workerService.getHttpServer().register(msgPO.getMSG_TAG_NAME(), new ServiceReqHandler(msgPO) {
				@Override
				public String doHttp(HttpServletRequest request, HttpServletResponse response) {
					String contentType = Convert.toString(msgPO.getDataSchemeMap().get(
							POConstant.Msg_DATA_SCHEME_contentType));
					String ret = "";
					try {
						String isBetch = request.getParameter(POConstant.Msg_DATA_SCHEME_isBatch);
						boolean betch = Convert.toBool(isBetch, false);
						if (!"binary".equals(contentType)) {
							String contentKey = Convert.toString(msgPO.getParamMap().get(
									POConstant.Msg_REQ_PARAM_httpContentKey));
							String content = request.getParameter(contentKey);
							if (LogUtils.debugEnabled()) {
								if (content.length() < 1024)
									LogUtils.debug("接收到客户端[" + request.getRemoteHost() + "] HTTP数据  content=" + content);
								else
									LogUtils.debug("接收到客户端[" + request.getRemoteHost() + "] HTTP数据  content=" +
											content.substring(0, 1000) + "......");
							}
							if (content != null && !"".equals(content)) {
								if (parserSend(msgPO, content, betch, request.getRemoteHost())) { // 记录日志
									ret = "ok";
								} else {
									ret = "不完整消息";
								}
							} else {
								throw new RuntimeException("参数[" + contentKey + "]无数据");
							}
						} else {
							InputStream sis = request.getInputStream();
							StreamMsgHelper.parseStream(msgPO, sis, betch, request.getRemoteHost());
							sis.close();
							ret = "ok";
						}
					} catch (Exception e) {
						StringWriter sw = new StringWriter();
						PrintWriter pw = new PrintWriter(sw);
						e.printStackTrace(pw);
						String estr = sw.toString();
						workerService.notifyCollectError(msgPO.getMSG_ID(), estr, request.getRemoteHost());
						ret = "您访问的采集服务HTTP请求[/" + msgPO.getMSG_TAG_NAME() + "]出错:" + estr;
					} finally {
					}
					return ret;
				}

				@Override
				public String getCharset() {
					return Convert.toString(msgPO.getParamMap().get(POConstant.Msg_REQ_PARAM_charset), "UTF-8");
				}
			});
			break;
		case POConstant.TRI_TYPE_P_WS:
			workerService.getWsServer().register(msgPO.getMSG_TAG_NAME(), new ServiceReqHandler(msgPO) {
				@Override
				public String doWS(String path, String content, boolean isBetch, ClientInfo clientInfo) {
					try {
						if (content != null && !"".equals(content)) {
							if (parserSend(msgPO, content, isBetch, clientInfo.getClientIp())) { // 记录日志
								return "ok";
							} else {
								return "不完整消息";
							}
						} else {
							throw new RuntimeException("无数据");
						}
					} catch (Exception e) {
						StringWriter sw = new StringWriter();
						PrintWriter pw = new PrintWriter(sw);
						e.printStackTrace(pw);
						String estr = sw.toString();
						workerService.notifyCollectError(msgPO.getMSG_ID(), estr, clientInfo.getClientIp());
						return "你访问的采集服务WS请求[" + path + "]出错:" + estr;
					}
				}
			});
			break;
		case POConstant.TRI_TYPE_P_SOCKET:
			workerService.getSocketServer().register(msgPO.getMSG_TAG_NAME(), new ServiceReqHandler(msgPO) {
				@Override
				public String doSocket(String path, String content, ClientInfo clientInfo) {
					try {
						if (content != null && !"".equals(content)) {
							if (parserSend(msgPO, content, false, clientInfo.getClientIp())) {
								return "ok";
							} else {
								return "不完整消息";
							}
						} else {
							throw new RuntimeException("无数据");
						}
					} catch (Exception e) {
						StringWriter sw = new StringWriter();
						PrintWriter pw = new PrintWriter(sw);
						e.printStackTrace(pw);
						String estr = sw.toString();
						workerService.notifyCollectError(msgPO.getMSG_ID(), estr, clientInfo.getClientIp());
						return "你访问的采集服务Socket请求[" + path + "]出错:" + estr;
					}
				}
			});
			break;
		}
	}

	@Override
	public void stop() {
		switch (msgPO.getTRIGGER_TYPE()) {
		case POConstant.TRI_TYPE_P_HTTP:
			workerService.getHttpServer().unRegister(msgPO.getMSG_TAG_NAME());
			break;
		case POConstant.TRI_TYPE_P_WS:
			workerService.getWsServer().unRegister(msgPO.getMSG_TAG_NAME());
			break;
		case POConstant.TRI_TYPE_P_SOCKET:
			workerService.getSocketServer().unRegister(msgPO.getMSG_TAG_NAME());
			break;
		}
	}

	@Override
	public boolean isRunning() {
		switch (msgPO.getTRIGGER_TYPE()) {
		case POConstant.TRI_TYPE_P_HTTP:
			return workerService.getHttpServer().isStarted();
		case POConstant.TRI_TYPE_P_WS:
			return workerService.getWsServer().isStarted();
		case POConstant.TRI_TYPE_P_SOCKET:
			return workerService.getSocketServer().isStarted();
		}
		return false;
	}
}
