package com.ery.ertc.collect.remote.server;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import javax.annotation.Resource;
import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;

import com.sun.net.httpserver.HttpExchange;
import com.sun.xml.internal.ws.developer.JAXWSProperties;

@WebService
public class CollectWS {

	@Resource
	private WebServiceContext context;

	public WSCollectServer wsServer;

	@WebMethod
	public String sendData(String topic, String content, boolean isBetch) {
		if (wsServer != null) {
			try {
				if (wsServer.getReqHandlerMap().containsKey(topic)) {
					ClientInfo clientInfo = getClientInfo();
					return wsServer.getReqHandlerMap().get(topic).doWS(topic, content, isBetch, clientInfo);
				} else {
					return "未找到相关主题[" + topic + "]业务,数据不能处理!";
				}
			} catch (Exception e) {
				return "服务出错!" + e.getMessage();
			}
		}
		return "服务出错!wsServer未初始";
	}

	@WebMethod(exclude = true)
	public void setWsServer(WSCollectServer wsServer) {
		this.wsServer = wsServer;
	}

	@WebMethod(exclude = true)
	private ClientInfo getClientInfo() {
		try {
			MessageContext mc = context.getMessageContext();
			HttpExchange exchange = (HttpExchange) mc.get(JAXWSProperties.HTTP_EXCHANGE);
			ClientInfo clientInfo = new ClientInfo();
			InetSocketAddress isa = exchange.getRemoteAddress();
			InetAddress addr = isa.getAddress();
			clientInfo.setClientIp(addr.getHostAddress());
			clientInfo.setClientHostName(addr.getHostName());
			clientInfo.setSocketPort(isa.getPort());
			return clientInfo;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
