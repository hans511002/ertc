package com.ery.ertc.collect.ui.servlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.I0Itec.zkclient.ZkClient;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.CollectConstant;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.remote.server.ServiceReqHandler;
import com.ery.ertc.collect.task.dao.CollectDAO;
import com.ery.ertc.collect.ui.po.HostTotal;
import com.ery.ertc.collect.ui.po.MsgTotal;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.StringUtils;

public class RootServlet extends ServiceReqHandler {

	private DaemonMaster daemonMaster;
	private ZkClient zkClient;

	public RootServlet(DaemonMaster daemonMaster) {
		this.daemonMaster = daemonMaster;
		zkClient = daemonMaster.getZkClient();
	}

	@Override
	public String doHttp(HttpServletRequest request, HttpServletResponse response) {
		CollectDAO dao = new CollectDAO();
		StringBuilder str = new StringBuilder();
		try {
			String masterId = daemonMaster.getNodeTracker().getMasterName();
			Map<String, Boolean> nodes = daemonMaster.getNodeTracker().getClusterNodes();
			Map<String, List<String>> assignInfo = new HashMap<String, List<String>>(daemonMaster
					.getTaskAssignTracker().getAllAssignInfo());
			str.append("<div style='font-size:20px;font-weight:bold;width:99%;text-align:center;'>采集集群总览</div>");

			LinkedHashMap<String, MsgTotal> taskTotals = new LinkedHashMap<String, MsgTotal>();// 任务统计信息
			long collectSize = 0;
			long collectNum = 0;
			long collectError = 0;
			long sendSize = 0;
			long filterNum = 0;
			long distinctNum = 0;
			long sendNum = 0;
			long sendError = 0;
			long leaveSize = 0;
			long collectTps = 0;
			long sendTps = 0;

			if (nodes.size() == 0) {
				str.append("<br><span style='color:#e9e9e9'>(无活动节点)</span>");
			} else {
				str.append("<table border='1' align='center'>");
				str.append("<tr><th colspan=100% style='text-align:left;'>节点信息(total:" + nodes.size() + ")</th></tr>" +
						"<tr>" + "<th style='width:180px;'>ID-host</th>" + "<th style='width:125px;'>启动时间</th>" +
						"<th style='width:85px;'>服务端口</th>" + "<th style='width:85px;'>任务数</th>" +
						"<th style='width:70px;'>遗留Size</th>" + "<th style='width:70px;'>采集Size</th>" +
						"<th style='width:70px;'>采集Num</th>" + "<th style='width:70px;'>采集Error</th>" +
						"<th style='width:70px;'>被过滤</th>" + "<th style='width:70px;'>去除重复</th>" +
						"<th style='width:70px;'>发送Size</th>" + "<th style='width:70px;'>发送Num</th>" +
						"<th style='width:70px;'>发送Error</th>" + "<th style='width:70px;'>5秒内采集TPS</th>" +
						"<th style='width:70px;'>5秒内发送TPS</th>" + "</tr>");
				for (String nodeId : nodes.keySet()) {
					List<String> tids = assignInfo.get(nodeId);
					Object o = zkClient.readData("/" + Config.getZkBaseZone() + "/" + ZkConstant.HOST_NODE + "/" +
							nodeId);
					Map<String, Object> nodeInfo = JSON.parseObject(o.toString());
					String hostName = nodeInfo.get(ZkConstant.NODE_INFO_HOST_NAME).toString();
					String startTime = nodeInfo.get(ZkConstant.NODE_START_TIME).toString();
					Object http = nodeInfo.get(ZkConstant.NODE_HTTP_PORT);
					Object ws = nodeInfo.get(ZkConstant.NODE_WS_PORT);
					Object socket = nodeInfo.get(ZkConstant.NODE_SOCKET_PORT);
					Object ui = nodeInfo.get(ZkConstant.NODE_UI_PORT);
					HostTotal nodeTotal = new HostTotal();// 主机节点统计
					// 节点信息
					str.append("<tr><td><a href='/node?nodeId=" + nodeId + "' target='_blank' " +
							(nodes.get(nodeId) ? "" : "disable style='color:#ef0000'") + ">" + nodeId + "-" + hostName +
							(nodeId.equals(masterId) ? "(Master)" : (nodes.get(nodeId) ? "online" : "dead")) +
							"</a></td>");
					str.append("<td>" + startTime.substring(2) + "</td>");
					str.append("<td style='text-align:right'>" +
							"http:" +
							((http != null && http instanceof Integer) ? http
									: ("<span title='" + http + "' style='color:#e9e9e9'>NONE</span>")) +
							"<br>ws:" +
							((ws != null && ws instanceof Integer) ? ws
									: ("<span title='" + ws + "' style='color:#e9e9e9'>NONE</span>")) +
							"<br>socket:" +
							((socket != null && socket instanceof Integer) ? socket
									: ("<span title='" + socket + "' style='color:#e9e9e9'>NONE</span>")) +
							"<br>ui:" +
							((ui != null && ui instanceof Integer) ? ui
									: ("<span title='" + ui + "' style='color:#e9e9e9'>NONE</span>")) + "</td>");
					if (tids != null && tids.size() > 0) {
						str.append("<td>" + tids.size() + "&nbsp;(" + StringUtils.join(tids, ",") + ")</td>");
						long ct = 0;
						long st = 0;
						for (String tid : tids) {
							MsgTotal taskTotal = taskTotals.get(tid);// 任务汇总
							if (taskTotal == null) {
								taskTotal = new MsgTotal();
								taskTotals.put(tid, taskTotal);
							}
							String zkpath = "/" + Config.getZkBaseZone() + "/" + ZkConstant.TASKINFO_NODE + "/" +
									nodeId + "/" + tid;
							List<List<Object>> list = new ArrayList<List<Object>>();
							if (zkClient.exists(zkpath)) {
								list = JSON.parseObject(zkClient.readData(zkpath).toString(), list.getClass());
							}
							if (list != null && list.size() > 0) {
								List<Object> ls = list.get(list.size() - 1);
								taskTotal.rels.add(nodeId + "(" + ls.get(6) + ")");// 各存储发送状态
								long ct_ = 0;
								long st_ = 0;
								Map<String, Object> lstoreInfo = null;
								Map<String, Object> storeInfo = null;
								for (int i = 0; i < list.size(); i++) {
									ls = list.get(i);
									String storeMapStr = ls.get(3).toString();
									lstoreInfo = storeInfo;
									storeInfo = CollectConstant.objectMapper.readValue(storeMapStr, Map.class);
									if (i == list.size() - 1) {
										long _collectNum = Convert.toLong(ls.get(0));
										long _collectSize = Convert.toLong(ls.get(1));
										long _collectError = Convert.toLong(ls.get(2));
										// long _filterNum =
										// Convert.toLong(ls.get(7));
										// long _distinctNum =
										// Convert.toLong(ls.get(8));
										// long _sendNum =
										// Convert.toLong(ls.get(3));
										// long _sendSize =
										// Convert.toLong(ls.get(4));
										// long _sendError =
										// Convert.toLong(ls.get(5));
										// long _leaveSize =
										// Convert.toLong(ls.get(6));

										// 节点统计
										nodeTotal.collectNum += _collectNum;
										nodeTotal.collectSize += _collectSize;
										nodeTotal.collectError += _collectError;
										// 任务统计
										taskTotal.collectNum += _collectNum;
										taskTotal.collectSize += _collectSize;
										taskTotal.collectError += _collectError;

										long maxLeave = 0;
										for (String storeId : storeInfo.keySet()) {
											List stInfo = (List) storeInfo.get(storeId);
											long fnum = Convert.toLong(stInfo.get(0), 0);
											nodeTotal.filterNum += fnum;
											long dnum = Convert.toLong(stInfo.get(1), 0);
											nodeTotal.distinctNum += dnum;
											long sdNum = Convert.toLong(stInfo.get(2), 0);
											nodeTotal.sendNum += sdNum;
											long sdSize = Convert.toLong(stInfo.get(3), 0);
											nodeTotal.sendSize += sdSize;
											long sdErr = Convert.toLong(stInfo.get(4), 0);
											nodeTotal.sendError += sdErr;

											long strid = Convert.toLong(storeId);
											taskTotal.filterNum.put(
													strid,
													taskTotal.filterNum.containsKey(strid) ? taskTotal.filterNum
															.get(strid) + fnum : fnum);
											taskTotal.distinctNum.put(
													strid,
													taskTotal.distinctNum.containsKey(strid) ? taskTotal.distinctNum
															.get(strid) + dnum : dnum);
											taskTotal.sendNum
													.put(strid,
															taskTotal.sendNum.containsKey(strid) ? taskTotal.sendNum
																	.get(strid) + sdNum : sdNum);
											taskTotal.sendSize.put(
													strid,
													taskTotal.sendSize.containsKey(strid) ? taskTotal.sendSize
															.get(strid) + sdSize : sdSize);
											taskTotal.sendError.put(
													strid,
													taskTotal.sendError.containsKey(strid) ? taskTotal.sendError
															.get(strid) + sdErr : sdErr);
											long leave = Convert.toLong(stInfo.get(5), 0);
											taskTotal.leaveSize.put(
													strid,
													taskTotal.leaveSize.containsKey(strid) ? taskTotal.leaveSize
															.get(strid) + leave : leave);
											if (maxLeave < Convert.toLong(stInfo.get(5), 0)) {
												maxLeave = Convert.toLong(stInfo.get(5), 0);
											}
										}
										nodeTotal.leaveSize += maxLeave;
										String tmps = Convert.toString(ls.get(5));
										taskTotal.minStartTime(tmps);// 最后一位为启动时间
									}
									if (i > 0) {
										List<Object> pls = list.get(i - 1);
										long thisaddnum = Convert.toLong(ls.get(0)) - Convert.toLong(pls.get(0));
										ct_ += thisaddnum;
										for (String storeId : storeInfo.keySet()) {
											List stInfo = (List) storeInfo.get(storeId);
											List lstInfo = (List) lstoreInfo.get(storeId);
											thisaddnum = (Convert.toLong(stInfo.get(2)) - Convert
													.toLong(lstInfo.get(2)));
											st_ += thisaddnum;
										}
									}
								}
								// 每个任务的平均tps
								if (list.size() > 1) {
									ct_ = ct_ / (list.size() - 1);
									st_ = st_ / (list.size() - 1);
								}
								ct += ct_;
								st += st_;
								taskTotal.collectTps += ct_;
								taskTotal.sendTps += st_;
							} else {
								taskTotal.rels.add(nodeId + "(none)");
							}
						}
						// 每个节点平均tps=各任务平均tps之和
						nodeTotal.collectTps = ct;
						nodeTotal.sendTps = st;
					} else {
						str.append("<td style='color:#e9e9e9'>无任务</td>");
					}

					str.append("<td>" + nodeTotal.leaveSize + "</td>");
					str.append("<td>" + nodeTotal.collectSize + "</td>");
					str.append("<td>" + nodeTotal.collectNum + "</td>");
					str.append("<td>" + nodeTotal.collectError + "</td>");
					str.append("<td>" + nodeTotal.filterNum + "</td>");
					str.append("<td>" + nodeTotal.distinctNum + "</td>");
					str.append("<td>" + nodeTotal.sendSize + "</td>");
					str.append("<td>" + nodeTotal.sendNum + "</td>");
					str.append("<td>" + nodeTotal.sendError + "</td>");
					str.append("<td>" + nodeTotal.collectTps + "</td>");
					str.append("<td>" + nodeTotal.sendTps + "</td></tr>");

					collectSize += nodeTotal.collectSize;
					collectNum += nodeTotal.collectNum;
					collectError += nodeTotal.collectError;
					sendSize += nodeTotal.sendSize;
					sendNum += nodeTotal.sendNum;
					sendError += nodeTotal.sendError;
					leaveSize += nodeTotal.leaveSize;
					collectTps += nodeTotal.collectTps;
					sendTps += nodeTotal.sendTps;
					filterNum += nodeTotal.filterNum;
					distinctNum += nodeTotal.distinctNum;
				}
				str.append("<tr><th colspan=4 style='text-align:right;padding-right:10px;'>合计:</th>"
						+ "<th style='text-align:right'>" +
						leaveSize +
						"</th>" +
						"<th style='text-align:right'>" +
						collectSize +
						"</th>" +
						"<th style='text-align:right'>" +
						collectNum +
						"</th>" +
						"<th style='text-align:right'>" +
						collectError +
						"</th>" +
						"<th style='text-align:right'>" +
						filterNum +
						"</th>" +
						"<th style='text-align:right'>" +
						distinctNum +
						"</th>" +
						"<th style='text-align:right'>" +
						sendSize +
						"</th>" +
						"<th style='text-align:right'>" +
						sendNum +
						"</th>" +
						"<th style='text-align:right'>" +
						sendError +
						"</th>" +
						"<th style='text-align:right'>" +
						collectTps + "</th>" + "<th style='text-align:right'>" + sendTps + "</th>" + "</tr>");

				// 任务信息
				str.append("<tr><th colspan=100% style=';text-align:left;'>任务信息(total:" + taskTotals.size() +
						")</th></tr>");
				str.append("<tr>" + "<th>MSG_ID</th>" + "<th>启动时间</th>" + "<th colspan=2>分布节点</th>"
						+ "<th>启动遗留Size</th>" + "<th>采集Size</th>" + "<th>采集Num</th>" + "<th>采集Error</th>"
						+ "<th>被过滤</th>" + "<th>去除重复</th>" + "<th>发送Size</th>" + "<th>发送Num</th>" + "<th>发送Error</th>"
						+ "<th>5秒内采集TPS</th>" + "<th>5秒内发送TPS</th>" + "</tr>");
				for (String tid : taskTotals.keySet()) {
					MsgTotal taskTotal = taskTotals.get(tid);
					str.append("<tr>");
					str.append("<td><a href='/task?msgId=" + tid + "' target='_blank'>" + tid + "[" +
							daemonMaster.workerService.getMsgPO(Convert.toLong(tid)).getTriggerName() + "]</td>");
					str.append("<td>" + (taskTotal.startTime != null ? taskTotal.startTime.substring(2) : "") + "</td>");
					str.append("<td colspan=2>" + StringUtils.join(taskTotal.rels, "<br/>") + "</td>");
					str.append("<td>" + taskTotal.leaveSize + "</td>");
					str.append("<td>" + taskTotal.collectSize + "</td>");
					str.append("<td>" + taskTotal.collectNum + "</td>");
					str.append("<td>" + taskTotal.collectError + "</td>");
					str.append("<td>" + taskTotal.filterNum + "</td>");
					str.append("<td>" + taskTotal.distinctNum + "</td>");
					str.append("<td>" + taskTotal.sendSize + "</td>");
					str.append("<td>" + taskTotal.sendNum + "</td>");
					str.append("<td>" + taskTotal.sendError + "</td>");
					str.append("<td>" + taskTotal.collectTps + "</td>");
					str.append("<td>" + taskTotal.sendTps + "</td>");
					str.append("</tr>");
				}
				str.append("</table>");
				str.append("<span style='margin-left:30px;color:red'>一条消息采集的原始数据大小不一定等于发送数据大小</span>");
			}

			str.append("<br><br><div style='width:1200px;margin-left:30px'>PS：提交消息数据(包括两部分topicName,content)到采集集群方式"
					+ "<br><span style='margin-left:60px;'>1)访问Http服务,地址：<span style='color:blue'>http://{host}:{httpPort}/{topicName}</span>"
					+ "【消息主题作为RootPath,POST请求传参数?content=XXX】</span>"
					+ "<br><span style='margin-left:60px;'>2)访问WS服务,wsdl地址：<span style='color:blue'>http://{host}:{wsPort}/CollectWS?wsdl</span>"
					+ "【包含一个接口sendData(String topicName,String content)】</span>"
					+ "<br><span style='margin-left:60px;'>3)通过Socket端口传输"
					+ "【字节流,前4个byte表示整个消息的长度,消息字符串的第一个(,逗号)之前的内容作为topicName，后面的作为content】</span>"
					+ "<br><br>content格式："
					+ "<br><span style='margin-left:60px;'></span>1)整个是一个JSON字符串,可以是array,可以是map.(具体类型需要在DB配置)"
					+ "<br><span style='margin-left:60px;'></span>2)array的话，需要在DB配置索引与消息字段的映射关系"
					+ "<br><span style='margin-left:60px;'></span>3)map的话，需要在DB配置key与消息字段的映射关系" + "</div>");

			return str.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			dao.close();
		}
	}
}
