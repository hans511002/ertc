package com.ery.ertc.collect.remote.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.CharBuffer;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.estorm.po.MsgPO;

public class WSCollectExec extends CollectExec {

	public WSCollectExec(AbstractTaskExecutor executor) {
		super(executor);
	}

	@Override
	public void execute() throws Exception {
		MsgPO msgPO = executor.getMsgPO();
		URL url = new URL(msgPO.getURL());
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		httpConn.setRequestProperty("Content-Length", reqSendXMLTemp.getBytes().length + "");
		httpConn.setRequestProperty("Content-Type", "text/xml; charset=" + reqCharset);
		httpConn.setRequestProperty("soapActionString", "");
		httpConn.setRequestMethod("POST");
		httpConn.setDoOutput(true);
		httpConn.setDoInput(true);
		srcClient = httpConn.getURL().getHost();

		// 请求
		OutputStream out = httpConn.getOutputStream();
		out.write(reqSendXMLTemp.getBytes(reqCharset));
		out.close();

		// 返回
		InputStreamReader isr = new InputStreamReader(httpConn.getInputStream(), dataCharset);
		BufferedReader in = new BufferedReader(isr);
		CharBuffer charBuffer = CharBuffer.allocate(512);
		StringBuilder retXml = new StringBuilder();
		int len = 0;
		while ((len = in.read(charBuffer)) > 0) {
			retXml.append(new String(charBuffer.array(), 0, len));
			charBuffer.clear();
		}

		Document doc = DocumentHelper.parseText(retXml.toString());
		Node node = doc.selectSingleNode(xmlNodePath);
		if (isBatch) {
			String[] contents = QueueUtils.convertDataToContents(node.getStringValue(), allType, itemSplitCh);
			for (String content : contents) {
				sendMsg(msgPO, content);
			}
		} else {
			sendMsg(msgPO, node.getStringValue());
		}

	}
}
