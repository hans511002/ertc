package com.ery.ertc.collect.task.assign;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ery.ertc.collect.task.TaskAssignTracker;

public abstract class ITaskAssign {
	public Map<String, Integer> delTasks = new HashMap<String, Integer>();// 需要停止的任务列表
	public Map<String, Integer> modTasks = new HashMap<String, Integer>();// 需要更新的任务列表
	public Map<String, Integer> addTasks = new HashMap<String, Integer>();// 需要更新的任务列表

	public final TaskAssignTracker taskAssignTracker;

	public ITaskAssign(TaskAssignTracker taskAssignTracker) {
		this.taskAssignTracker = taskAssignTracker;
	}

	/**
	 * 后续追加分配，或改变分配（需要实现动态负载）
	 * 
	 * @param tasks
	 *            任务列表
	 * @param nodes
	 *            节点ID
	 * @param oldAssign
	 *            原分配方案
	 * @param taskRunInfo
	 *            原任务运行情况(内部包含一些实时的统计信息)
	 * @return
	 */
	abstract Map<String, List<String>> assignChange(List<Map<String, Object>> tasks, List<String> nodes,
			Map<String, List<String>> oldAssign);

}
