package com.ery.ertc.collect.task.assign;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ery.ertc.collect.task.TaskAssignTracker;
import com.ery.base.support.utils.Convert;

public class TaskAssign extends ITaskAssign {

	public TaskAssign(TaskAssignTracker taskAssignTracker) {
		super(taskAssignTracker);
	}

	// 简单任务数平衡
	public String getMinHost(Map<String, List<String>> newAssign) {
		String minHost = null;
		for (String host : newAssign.keySet()) {
			if (minHost == null)
				minHost = host;
			if (newAssign.get(host).size() < newAssign.get(minHost).size()) {
				minHost = host;
			}
		}
		return minHost;
	}

	@Override
	public Map<String, List<String>> assignChange(List<Map<String, Object>> tasks, List<String> nodes,
			Map<String, List<String>> oldAssign) {
		delTasks.clear();
		modTasks.clear();// 需要更新的任务列表
		addTasks.clear();// 需要更新的任务列表
		Map<String, Integer> movTasks = new HashMap<String, Integer>();// 迁移任务列表

		Map<String, List<String>> newAssign = new HashMap<String, List<String>>();
		Map<String, String> accAssign = new HashMap<String, String>();

		Map<String, List<String>> runingTasks = new HashMap<String, List<String>>();// 已经运行的任务在主机的列表
		for (String node : oldAssign.keySet()) {
			List<String> msgs = oldAssign.get(node);
			for (String msgId : msgs) {
				List<String> hosts = runingTasks.get(msgId);
				if (hosts == null) {
					hosts = new ArrayList<String>();
					runingTasks.put(msgId, hosts);
				}
				hosts.add(node);
			}
		}
		for (String node : nodes) {
			newAssign.put(node, new ArrayList<String>());
		}
		for (Map<String, Object> task : tasks) {
			String msgId = Convert.toString(task.get("MSG_ID"));
			int triggerType = Convert.toInt(task.get("TRIGGER_TYPE"));
			int state = Convert.toInt(task.get("STATE"));
			if (state == 1) {
				if (runingTasks.containsKey(msgId)) {
					modTasks.put(msgId, triggerType);
				} else {
					addTasks.put(msgId, triggerType);
				}
			} else {// 删除无效的
				delTasks.put(msgId, triggerType);
			}
		}
		// 保留原有分配
		for (String msgId : runingTasks.keySet()) {
			if (modTasks.containsKey(msgId) || delTasks.containsKey(msgId)) {
				continue;
			}
			List<String> hosts = runingTasks.get(msgId);
			for (String host : hosts) {
				if (newAssign.containsKey(host)) {// 保持原有分配主机，主机有退出
					newAssign.get(host).add(msgId);
					accAssign.put(msgId, msgId);// 完成分配的
				}
			}
			// 因主机退出需要迁移的
			if (!accAssign.containsKey(msgId)) {// 分配未成功
				movTasks.put(msgId, 0);
			}
		}

		// 先分配迁移的，根据先前分配主机数判断是否多主机分配
		for (String movMsgId : movTasks.keySet()) {
			List<String> hosts = runingTasks.get(movMsgId);
			if (hosts.size() > 1) {
				for (String host : nodes) {
					newAssign.get(host).add(movMsgId);
				}
			}
		}
		for (String movMsgId : movTasks.keySet()) {
			List<String> hosts = runingTasks.get(movMsgId);
			if (hosts.size() == 1) {
				String host = getMinHost(newAssign);
				newAssign.get(host).add(movMsgId);
			}
		}
		// 更新修改的
		for (String msgId : modTasks.keySet()) {
			List<String> hosts = runingTasks.get(msgId);
			if (modTasks.get(msgId) > 20) {
				for (String host : nodes) {
					newAssign.get(host).add(msgId);
					accAssign.put(msgId, msgId);// 完成分配的
				}
			} else {
				for (String host : hosts) {
					if (newAssign.containsKey(host)) {// 保持原有分配主机，主机有退出
						newAssign.get(host).add(msgId);
						accAssign.put(msgId, msgId);// 完成分配的
					}
				}
				if (!accAssign.containsKey(msgId)) {// 分配未成功
					String host = getMinHost(newAssign);
					newAssign.get(host).add(msgId);
				}
			}
		}
		// 分配新增的
		for (String msgId : addTasks.keySet()) {
			if (addTasks.get(msgId) > 20) {
				for (String host : nodes) {
					newAssign.get(host).add(msgId);
					accAssign.put(msgId, msgId);// 完成分配的
				}
			} else {
				String host = getMinHost(newAssign);
				newAssign.get(host).add(msgId);
			}
		}
		return newAssign;
	}

}
