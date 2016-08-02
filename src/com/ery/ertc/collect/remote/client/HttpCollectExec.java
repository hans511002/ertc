package com.ery.ertc.collect.remote.client;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.PostMethod;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.remote.StreamMsgHelper;
import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.utils.Convert;

public class HttpCollectExec extends CollectExec {

	private HttpClient httpClient;
	private String[] contentKeys;

	public HttpCollectExec(AbstractTaskExecutor executor) {
		super(executor);
	}

	@Override
	public void execute() throws Exception {
		MsgPO msgPO = executor.getMsgPO();
		if (httpClient == null) {
			httpClient = new HttpClient();
			String url = msgPO.getURL();
			URI uri = new URI(url);
			httpClient.getHostConfiguration().setHost(uri);
			httpClient.setTimeout(reqTimeout);
			contentKeys = reqContentKey.split(",");
			srcClient = httpClient.getHostConfiguration().getHost();
		}
		List<String> reqValues = JSON.parseArray(Convert.toString(reqSendContent, "[]"), String.class);
		if (contentKeys.length != reqValues.size()) {
			throw new RuntimeException("http请求参数key与值个数不对应!");
		}
		PostMethod post = new PostMethod();
		// 构造请求参数
		NameValuePair[] nvps = new NameValuePair[contentKeys.length];// 请求参数和值
		for (int i = 0; i < nvps.length; i++) {
			// 值可能有些宏变量
			nvps[i] = new NameValuePair(contentKeys[i], reqValues.get(i));
		}
		post.setRequestBody(nvps);
		httpClient.executeMethod(post);
		if ("html".equals(contentType)) {
			String res = post.getResponseBodyAsString();// 返回值[可能包含html标记]
			res = new String(res.getBytes(), dataCharset);
			Document doc = DocumentHelper.parseText(res);
			Node node = doc.selectSingleNode(xmlNodePath);
			if (isBatch) {
				String[] contents = QueueUtils.convertDataToContents(node.getStringValue(), allType, itemSplitCh);
				for (String content : contents) {
					sendMsg(msgPO, content);
				}
			} else {
				sendMsg(msgPO, node.getStringValue());
			}
		} else if ("text".equals(contentType)) {
			String res = post.getResponseBodyAsString();// 返回值[可能包含html标记]
			res = new String(res.getBytes(), dataCharset);
			String[] contents = QueueUtils.convertDataToContents(res, allType, itemSplitCh);
			for (String content : contents) {
				sendMsg(msgPO, content);
			}
		} else if ("binary".equals(contentType)) {
			InputStream ins = post.getResponseBodyAsStream();// 返回的数据是二进制流
			StreamMsgHelper.parseStream(msgPO, ins, isBatch, srcClient);
			ins.close();
		}
		post.releaseConnection();
	}

}
