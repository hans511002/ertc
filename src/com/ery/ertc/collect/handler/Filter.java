package com.ery.ertc.collect.handler;

import java.util.HashMap;
import java.util.Map;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.ertc.collect.exception.ConfigException;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.ToolUtil;

public class Filter {

	private MsgPO msgPO;
	MsgExtPO extPo;
	Expression expr = null;
	boolean isNullRule;
	String[] macNames = null;
	int[] filedIndexRel = null;

	public Filter(MsgPO msgPO, MsgExtPO extPo) {
		this.msgPO = msgPO;
		this.extPo = extPo;
		intRule();
	}

	public void intRule() {
		String filterRule = extPo.getFilterRule();
		if (filterRule == null || filterRule.trim().equals("")) {
			isNullRule = true;
			return;
		} else {
			isNullRule = false;
		}
		filterRule = filterRule.substring("expr:".length());
		expr = AviatorEvaluator.compile(filterRule);
		macNames = expr.getVariableNames().toArray(new String[0]);
		filedIndexRel = new int[macNames.length];
		for (int i = 0; i < filedIndexRel.length; i++) {
			String macName = macNames[i].toUpperCase();
			if (!msgPO.getFieldIdxMapping().containsKey(macName)) {
				throw new ConfigException("消息[" + msgPO.getMSG_ID() + ":" + msgPO.getMSG_TAG_NAME() + "] 存储ID:" +
						extPo.getSTORE_ID() + " 过滤规则解析异常,宏变量:" + macName + "在字段中不存在");
			}
			filedIndexRel[i] = msgPO.getFieldIdxMapping().get(macName);
		}

	}

	/**
	 * 过滤
	 * 
	 * @param tuples
	 *            数据元组
	 * @return 返回true。表示被过滤掉
	 */
	public boolean filter(final Object[] tuples) {
		if (isNullRule)
			return false;
		Map<String, Object> val = new HashMap<String, Object>();
		if (filedIndexRel != null) {
			for (int i = 0; i < filedIndexRel.length; i++) {// 转计算对象
				if (tuples[filedIndexRel[i]] != null) {
					val.put(this.macNames[i], ToolUtil.convertToExecEnvObj(tuples[filedIndexRel[i]].toString()));
				} else {
					val.put(this.macNames[i], null);
				}
			}
		}
		Object res = this.expr.execute(val);
		if (ToolUtil.getEexcBoolean(res)) {
			return true;
		}
		return false;
	}

}
