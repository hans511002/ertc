package com.ery.ertc.collect.remote.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.jdbc.mapper.CellDataCall;
import com.ery.base.support.jdbc.mapper.MapColumnMapper;
import com.ery.base.support.sys.DataSourceManager;
import com.ery.base.support.utils.Convert;

public class SQLCollectExec extends CollectExec {

	public SQLCollectExec(AbstractTaskExecutor executor) {
		super(executor);
	}

	@Override
	public void execute() throws Exception {
		MsgPO msgPO = executor.getMsgPO();
		String sql = msgPO.getDATA_SCHEME();
		long dsId = msgPO.getDATA_SOURCE_ID();
		srcClient = dsId + "";
		try {
			Connection conn = DataSourceManager.getConnection(Convert.toString(dsId));
			ResultSet rs = conn.prepareStatement(sql).executeQuery();
			List<MsgFieldPO> msgFieldPOs = msgPO.getMsgFields();
			MapColumnMapper mapper = new MapColumnMapper();
			for (MsgFieldPO fieldPO : msgFieldPOs) {
				switch (fieldPO.getType()) {
				case 16:// 二进制
					mapper.setBlobColumnCode(fieldPO.getSRC_FIELD(), CellDataCall.DataContentCode.BINARY);
					break;
				}
			}
			Map<String, Object> row = null;
			while ((row = mapper.convert(rs)) != null) {
				List<Object> arr = QueueUtils.getMsgCollentInfo(msgPO);
				long bs = 0;//
				for (MsgFieldPO fpo : msgFieldPOs) {
					String fieldName = fpo.getSRC_FIELD();
					Object o = row.get(fieldName);
					switch (fpo.getType()) {
					case 2:
						o = Convert.toLong(o);
						bs += 8;
						break;
					case 4:
						o = Convert.toDouble(o);
						bs += 8;
						break;
					case 8:
						if (o instanceof java.util.Date || o instanceof java.sql.Date ||
								o instanceof java.sql.Timestamp) {
							o = ((java.util.Date) o).getTime();
						} else if (o instanceof Long) {
							o = Convert.toLong(o);
						} else {
							try {
								o = fpo.getSimpleDateFormat().parse(o.toString()).getTime();
							} catch (Exception e) {
								o = 0l;
							}
						}
						bs += 8;
						break;
					case 16:
						byte[] b = (byte[]) o;
						bs += b.length;
						break;
					default:
						String str = Convert.toString(o, "");
						bs += str.getBytes().length;
						o = str;
					}
					arr.add(o);
				}
				if (!arr.isEmpty()) {
					if (DaemonMaster.daemonMaster.queueUtils.appendMsg(msgPO, arr)) {
						// 记录日志
						DaemonMaster.daemonMaster.workerService.notifyCollectOK(msgPO.getMSG_ID(), bs, srcClient);
					}
				}
			}
			rs.close();
		} finally {
			DataSourceManager.destroy();
		}
	}
}
