package com.ery.ertc.collect.forwards.send;

import java.util.HashMap;
import java.util.Map;

import com.ery.ertc.dynamic.DynamicEngine;

public class DynaManager {
	public static DynaManager INSTANCE = new DynaManager();
	Map<String, Long> cacheObject = new HashMap<String, Long>();

	private DynaManager() {

	}

	public ISendPlugin getPluginObject(String fullName, String code) throws Exception {
		DynamicEngine de = DynamicEngine.getInstance();
		Object obj = null;
		try {
			obj = de.javaCodeToObject(fullName, code.toString());
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		}

		if (!(obj instanceof ISendPlugin)) {
			throw new Exception("");
		}
		return (ISendPlugin) obj;
	}
}
