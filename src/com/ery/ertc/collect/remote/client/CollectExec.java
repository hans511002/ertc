package com.ery.ertc.collect.remote.client;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.po.POConstant;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.StringUtils;

public abstract class CollectExec {

	protected AbstractTaskExecutor executor;
	private int state = -1;// -1标示未运行，1标示正常运行，0标示运行有错
	private String stateTime;
	private String info;
	protected String lastPointer;
	protected String srcClient;

	// 请求参数
	protected int reqTimeout;
	protected String reqCharset;
	protected String reqContentKey;
	protected String reqSendContent;
	protected String reqSendXMLTemp;
	protected String reqFtpType;
	protected String reqFileName;
	protected String reqConnJSON;
	protected String reqConnXML;
	protected String reqTableName;
	protected String reqColFamily;
	protected String reqStartRowKey;
	protected String reqJarFile;
	protected String reqJavaCode;
	protected String reqClassName;
	protected String reqMethodName;
	protected String reqArgs;
	protected int reqIsNotPoll;
	protected String reqZkRoot;
	protected int reqZkConnectOMS;
	protected int reqZkSessionOMS;

	// 数据解析参数
	protected String dataType;
	protected String splitCh;
	protected String dataCharset;
	protected boolean isBatch;
	protected String allType;
	protected String itemSplitCh;
	protected String newItemStartCh;
	private String startChRegex;
	protected String xmlNodePath;
	protected int headerSize;
	protected String contentType;

	public CollectExec(AbstractTaskExecutor executor) {
		this.executor = executor;
		this.lastPointer = executor.getMsgPO().getLAST_COLLECT_POINTER();
		Map<String, Object> map = executor.getMsgPO().getParamMap();
		if (map != null) {
			reqTimeout = Convert.toInt(map.get(POConstant.Msg_REQ_PARAM_timeout), 30000);
			reqCharset = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_charset), "utf-8");
			reqContentKey = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_httpContentKey), null);
			reqSendContent = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_sendContent), null);
			reqSendXMLTemp = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_wsSendXMLTemp), null);
			reqFtpType = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_fileFtpType), null);
			reqFileName = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_fileFileName), null);
			reqConnJSON = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_hbaseConnJSON), null);
			reqConnXML = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_hbaseConnXML), null);
			reqTableName = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_hbaseTableName), null);
			reqColFamily = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_hbaseColFamily), null);
			reqStartRowKey = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_hbaseStartRowKey), null);
			reqJarFile = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_classJarFile), null);
			reqJavaCode = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_classJavaCode), null);
			reqClassName = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_classClassName), null);
			reqMethodName = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_classMethodName), null);
			reqArgs = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_classArgs), null);
			reqIsNotPoll = Convert.toInt(map.get(POConstant.Msg_REQ_PARAM_classIsNotPoll), 0);
			reqZkRoot = Convert.toString(map.get(POConstant.Msg_REQ_PARAM_zkRoot), null);
			reqZkConnectOMS = Convert.toInt(map.get(POConstant.Msg_REQ_PARAM_zkConnectOMS), Config.getZkConnectTOMS());
			reqZkSessionOMS = Convert.toInt(map.get(POConstant.Msg_REQ_PARAM_zkSessionOMS), Config.getZkSessionTOMS());
		}

		map = executor.getMsgPO().getDataSchemeMap();
		if (map != null) {
			dataType = Convert.toString(map.get(POConstant.Msg_DATA_SCHEME_dataType), "text").toLowerCase();
			splitCh = Convert.toString(map.get(POConstant.Msg_DATA_SCHEME_splitCh), null);
			dataCharset = Convert.toString(map.get(POConstant.Msg_DATA_SCHEME_charset), "utf-8");
			isBatch = Convert.toBool(map.get(POConstant.Msg_DATA_SCHEME_isBatch), false);
			allType = Convert.toString(map.get(POConstant.Msg_DATA_SCHEME_allType), "text").toLowerCase();
			itemSplitCh = Convert.toString(map.get(POConstant.Msg_DATA_SCHEME_itemSplitCh), null);
			newItemStartCh = Convert.toString(map.get(POConstant.Msg_DATA_SCHEME_newItemStartCh), null);
			xmlNodePath = Convert.toString(map.get(POConstant.Msg_DATA_SCHEME_xmlDataNodePath), null);
			headerSize = Convert.toInt(map.get(POConstant.Msg_DATA_SCHEME_headerSize), 0);
			contentType = Convert.toString(map.get(POConstant.Msg_DATA_SCHEME_contentType), null);
		}
	}

	public int getState() {
		return state;
	}

	public String getInfo() {
		return info;
	}

	public String getStateTime() {
		return stateTime;
	}

	public void setStateOrInfo(int state, String info) {
		this.state = state;
		this.stateTime = Convert.toTimeStr(new Date(), null);
		this.info = info;
	}

	public abstract void execute() throws Exception;

	public void stopRun() throws Exception {
	}

	public String getSrcClient() {
		return srcClient;
	}

	// 获取上次采集的点
	public String getLastCollectPointer() {
		return lastPointer;
	}

	protected String getStartChRegex() {
		if (startChRegex == null && newItemStartCh != null && !"".equals(newItemStartCh)) {
			String regex = "\\$\\{(\\w+)\\}";
			startChRegex = StringUtils.replaceAll(newItemStartCh, regex, new StringUtils.ReplaceCall() {
				@Override
				public String replaceCall(String... groupStrs) {
					String d = groupStrs[1].toLowerCase();
					if ("yy".equals(d)) {
						return "(\\\\d{2})";// 2位年
					} else if ("yyyy".equals(d)) {
						return "(\\\\d{4})";// 4位年
					} else if ("mm".equals(d)) {
						return "(0?[1-9]|1[0-2])";// 月
					} else if ("dd".equals(d)) {
						return "(0?[1-9]|[12]\\\\d|3[0-1])";// 日
					} else if ("h".equals(d)) {
						return "([0-1]\\\\d|2[0-3])";// 时
					} else if ("m".equals(d)) {
						return "([0-5]\\\\d)";// 分
					} else if ("s".equals(d)) {
						return "([0-5]\\\\d)";// 秒
					} else if ("sss".equals(d)) {
						return "(\\\\d{3})";// 毫秒
					} else if ("mmm".equals(d)) {
						return "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sept|Oct|Nov|Dec)";// 英文月
					} else if ("eee".equals(d)) {
						return "(Mon|Tues|Wed|Thurs|Fri|Sat|Sun)";// 英文星期
					} else if ("z".equals(d)) {
						return "(CST|GMT)";// 时间类型（时区）
					}
					return groupStrs[0];
				}
			});
		}
		return startChRegex;
	}

	/**
	 * 发送消息（http,socket,ws,zk等方法会公用到）
	 * 
	 * @param msgPO
	 *            消息元数据
	 * @param msg
	 *            消息数据
	 */
	protected void sendMsg(MsgPO msgPO, String msg) {
		if (msg == null || "".equals(msg)) {
			return;
		}
		List<Object> arr = QueueUtils.convertMsgToTuple(msgPO, dataType, msg, splitCh);
		if (arr != null && !arr.isEmpty()) {
			if (DaemonMaster.daemonMaster.queueUtils.appendMsg(msgPO, arr)) {
				long l = msg.getBytes().length;
				DaemonMaster.daemonMaster.workerService.notifyCollectOK(msgPO.getMSG_ID(), l, srcClient);
			}
		}
	}

}
