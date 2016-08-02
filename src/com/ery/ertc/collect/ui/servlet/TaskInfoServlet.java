package com.ery.ertc.collect.ui.servlet;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.I0Itec.zkclient.ZkClient;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.remote.server.ServiceReqHandler;
import com.ery.ertc.collect.task.dao.CollectDAO;
import com.ery.ertc.collect.worker.WorkerService;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.utils.Convert;

public class TaskInfoServlet extends ServiceReqHandler {

	private DaemonMaster daemonMaster;
	private ZkClient zkClient;

	public TaskInfoServlet(DaemonMaster daemonMaster) {
		this.daemonMaster = daemonMaster;
		zkClient = daemonMaster.getZkClient();
	}

	public String doHttp(HttpServletRequest request, HttpServletResponse response) {
		StringBuilder str = new StringBuilder();
		String msgId = request.getParameter("msgId");
		if (msgId != null && !"".equals(msgId)) {
			MsgPO msgPO = WorkerService.getMsgPO(Convert.toLong(msgId, 0));
			if (msgPO == null) {
				CollectDAO dao = new CollectDAO();
				msgPO = dao.getCollectMsgInfo(msgId);
			}
			if (msgPO != null) {
				str.append("<table border='1' align='center'>");
				str.append("<tr><th style='width:150px'>属性</th><th style='width:800px'>值</th><th style='width:250px'>描述</th></tr>");
				str.append("<tr><td colspan=100%>基本信息（见表ST_MSG_STREAM）</td></tr>");
				str.append("<tr><td>MSG_ID</td><td>" + msgId + "</td><td>消息ID</td></tr>");
				str.append("<tr><td>MSG_NAME</td><td>" + msgPO.getMSG_NAME() + "</td><td>名称</td></tr>");
				str.append("<tr><td>MSG_DESC</td><td>" + msgPO.getMSG_DESC() + "</td><td>描述</td></tr>");
				str.append("<tr><td>MSG_TAG_NAME</td><td>" + msgPO.getMSG_TAG_NAME() +
						"</td><td>业务标签(发送至Kafka的topic,必须唯一)</td></tr>");
				str.append("<tr><td>TRIGGER_TYPE</td><td>" + msgPO.getTRIGGER_TYPE() + "</td><td>采集触发类型</td></tr>");
				str.append("<tr><td>URL</td><td>" + msgPO.getURL() + "</td><td>采集连接URL(如果需要)</td></tr>");
				str.append("<tr><td>USER</td><td>" + msgPO.getUSER() + "</td><td>连接用户(如果需要)</td></tr>");
				str.append("<tr><td>PASS</td><td>*</td><td>密码</td></tr>");
				str.append("<tr><td>REQ_PARAM</td><td>" + msgPO.getREQ_PARAM() + "</td><td>请求参数JSON串</td></tr>");
				str.append("<tr><td>DATA_SCHEME</td><td>" + msgPO.getDATA_SCHEME() + "</td><td>数据解析参数JSON串</td></tr>");
				str.append("<tr><td>DATA_SOURCE_ID</td><td>" + msgPO.getDATA_SOURCE_ID() +
						"</td><td>数据源ID(如果需要)</td></tr>");
				str.append("<tr><td>MSG_ORDER_FLAG</td><td>" + msgPO.getMSG_ORDER_FLAG() +
						"</td><td>排序模式(准排序)</td></tr>");
				str.append("<tr><td>CREATE_DATE</td><td>" + msgPO.getCREATE_DATE() + "</td><td>创建时间</td></tr>");
				str.append("<tr><td>CREATE_USER</td><td>" + msgPO.getCREATE_USER() + "</td><td>创建用户</td></tr>");
				str.append("<tr><td>LAST_COLLECT_POINTER</td><td>" + msgPO.getLAST_COLLECT_POINTER() +
						"</td><td>记录最后采集点(如果需要)</td></tr>");
				str.append("<tr><td>CACHE_DS</td><td>" + msgPO.getCACHE_DS() + "</td><td>缓存数据源(-1标示存本地文件)</td></tr>");
				str.append("<tr><td>TODO_SEND_QUEUE_SIZE</td><td>" + msgPO.getTODO_SEND_QUEUE_SIZE() +
						"</td><td>发送缓冲队列大小</td></tr>");
				str.append("<tr><td>STATE</td><td>" + msgPO.getSTATE() + "</td><td>状态</td></tr>");

				// 扩展信息
				for (MsgExtPO extPO : msgPO.getExtPO()) {
					str.append("<tr><td colspan=100%>扩展信息（见表ST_msg_store_cfg）</td></tr>");
					str.append("<tr><td>CACHE_DS</td><td>" + extPO.getSTORE_ID() + "</td><td>外部存储ID</td></tr>");
					str.append("<tr><td>SEND_THD_POOL_SIZE</td><td>" + extPO.getSEND_THD_POOL_SIZE() +
							"</td><td>发送线程池大小</td></tr>");
					str.append("<tr><td>SEND_TARGET</td><td>" + extPO.getSEND_TARGET() +
							"</td><td>发送目标(0kafka,1hbase,2hdfs)</td></tr>");
					str.append("<tr><td>LOG_LEVEL</td><td>" + extPO.getLOG_LEVEL() + "</td><td>记录日志级别</td></tr>");
					str.append("<tr><td>FILTER_RULE</td><td>" + extPO.getFILTER_RULE() + "</td><td>过滤规则</td></tr>");
					str.append("<tr><td>DISTINCT_RULE</td><td>" + extPO.getDISTINCT_RULE() + "</td><td>去重规则</td></tr>");

					Map<String, String> pars = extPO.getStoreConfigParams();
					if (pars != null) {
						str.append("<tr><td colspan=100%>外部存储参数</td></tr>");
						for (String key : pars.keySet()) {
							str.append("<tr><td>" + key + "</td><td>" + pars.get(key) + "</td><td> </td></tr>");

						}
						// str.append("<tr><th>参数</th><th>值</th><th>描述</th></tr>");
						// str.append("<tr><td>PART_NUM</td><td>" +
						// msgPO.getQueueParam().getPART_NUM() +
						// "</td><td>kafka分区数</td></tr>");
						// str.append("<tr><td>PART_RULE</td><td>" +
						// msgPO.getQueueParam().getPART_RULE() +
						// "</td><td>kafka分区规则</td></tr>");
						// str.append("<tr><td>PART_RULE</td><td>" +
						// msgPO.getQueueParam().getPART_REPLICA_NUM() +
						// "</td><td>kafka分区备份数</td></tr>");
						// str.append("<tr><td>SEND_TYPE</td><td>" +
						// msgPO.getQueueParam().getSEND_TYPE() +
						// "</td><td>发送类型0同步，1异步</td></tr>");
						// str.append("<tr><td>SEND_ACK_LEVEL</td><td>" +
						// msgPO.getQueueParam().getSEND_ACK_LEVEL() +
						// "</td><td>同步发送应答级别</td></tr>");
						// str.append("<tr><td>SEND_BATCH_NUM</td><td>" +
						// msgPO.getQueueParam().getSEND_BATCH_NUM() +
						// "</td><td>异步发送批次大小</td></tr>");
						// str.append("<tr><td>COMPRESSION_TYPE</td><td>" +
						// msgPO.getQueueParam().getCOMPRESSION_TYPE() +
						// "</td><td>压缩类型</td></tr>");

					}
				}

				// 字段
				str.append("<tr><td colspan=100%>消息字段（见表ST_MSG_FIELDS）</td></tr>");
				str.append("<tr><th>字段别名</th><th>中文名 - 类型 - 来源规则</th><th>描述</th></tr>");
				for (MsgFieldPO fieldPO : msgPO.getMsgFields()) {
					str.append("<tr><td>" + fieldPO.getFIELD_NAME() + "</td><td>" + fieldPO.getFIELD_CN_NAME() +
							"<span style='width:20px;inline-block'></span>" + fieldPO.getFIELD_DATA_TYPE() +
							"<span style='width:20px;inline-block'></span>" + fieldPO.getSRC_FIELD() + "</td><td>" +
							fieldPO.getFIELD_DESC() + "</td></tr>");
				}

				str.append("</table>");
			} else {
				str.append("未找到MSG_ID为[" + msgId + "]的消息定义!");
			}
		} else {
			str.append("未传入[msgId]参数!");
		}
		return str.toString();
	}
}
