package com.ery.ertc.collect.remote.server;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.CollectConstant;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.po.POConstant;
import com.ery.base.support.utils.Convert;

public class ServiceReqHandler {

	protected MsgPO msgPO;

	public ServiceReqHandler() {
	}

	public ServiceReqHandler(MsgPO msgPO) {
		this.msgPO = msgPO;
	}

	// 获取编码类型
	public String getCharset() {
		return "UTF-8";
	}

	/**
	 * http服务。处理接口
	 * 
	 * @param request
	 * @param response
	 * @return 将页面内容按字符串返回
	 */
	public String doHttp(HttpServletRequest request, HttpServletResponse response) {
		// 期待HttpCollectServer处理时子类实现
		return null;
	}

	public String doWS(String path, String content, boolean isBetch, ClientInfo clientInfo) {
		// 期待WSCollectServer处理时子类实现
		return null;
	}

	public String doSocket(String path, String content, ClientInfo clientInfo) {
		// 期待SocketCollectServer处理时子类实现
		return null;
	}

	// static final ObjectMapper om = new ObjectMapper();

	protected boolean parserSend(MsgPO taskInfo, String content, boolean isBtech, String srcClient)
			throws JsonParseException, JsonMappingException, IOException {
		Map<String, Object> dataScheme = taskInfo.getDataSchemeMap();
		String dataType = Convert.toString(dataScheme.get(POConstant.Msg_DATA_SCHEME_dataType), "array").toLowerCase();
		String splitCh = Convert.toString(dataScheme.get(POConstant.Msg_DATA_SCHEME_splitCh));
		if (isBtech) {
			List<String> btsRows = CollectConstant.objectMapper.readValue(content, List.class);
			for (String string : btsRows) {
				List<Object> arr = QueueUtils.convertMsgToTuple(taskInfo, dataType, string, splitCh);
				if (arr != null && !arr.isEmpty()) {
					DaemonMaster.daemonMaster.queueUtils.appendMsg(taskInfo, arr);
					DaemonMaster.daemonMaster.workerService.notifyCollectOK(msgPO.getMSG_ID(),
							string.getBytes().length, srcClient);
				}
			}
			return true;
		} else {
			List<Object> arr = QueueUtils.convertMsgToTuple(taskInfo, dataType, content, splitCh);
			if (arr != null && !arr.isEmpty()) {
				DaemonMaster.daemonMaster.queueUtils.appendMsg(taskInfo, arr);
				DaemonMaster.daemonMaster.workerService.notifyCollectOK(msgPO.getMSG_ID(), content.getBytes().length,
						srcClient);
				return true;
			} else {
				return false;
			}
		}

	}
}
