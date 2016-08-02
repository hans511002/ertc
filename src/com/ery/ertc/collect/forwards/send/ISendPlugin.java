package com.ery.ertc.collect.forwards.send;

import java.io.IOException;
import java.lang.reflect.Field;

import com.ery.ertc.collect.exception.ConfigException;
import com.ery.ertc.collect.forwards.Send;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.util.StringUtils;
import com.ery.base.support.log4j.LogUtils;

/**
 * 插件接口
 * 
 */
public abstract class ISendPlugin {
	public Send send;

	public static void main(String[] args) throws Exception {

	}

	public static Field getClassField(Class<?> clz, String fieldName) {
		Field[] fds = clz.getDeclaredFields();
		for (Field field : fds) {
			if (field.getName().equals(fieldName)) {
				return field;
			}
		}
		return null;
	}

	public static Class<?> getClassSubCls(Class<?> clz, String className) {
		Class<?>[] subcs = clz.getDeclaredClasses();
		for (Class<?> class1 : subcs) {
			if (class1.getName().endsWith("$" + className)) {
				return class1;
			}
		}
		return null;
	}

	public static void beforeSend(ISendPlugin remotePlugin, Object sendObj, Object[] row) throws IOException {
		if (remotePlugin != null) {// 插件处理
			remotePlugin.beforeSend(sendObj, row);
		}
	}

	public static ISendPlugin configurePlugin(Send send, MsgExtPO extPo) {
		String code = extPo.getSTORE_PLUGIN();
		if (code == null || code.equals(""))
			return null;
		String className = StringUtils.getClassFullName(code);
		try {
			LogUtils.info("开始初始化插件");
			long start = System.currentTimeMillis();
			ISendPlugin remotePlugin = DynaManager.INSTANCE.getPluginObject(className, code);
			long end = System.currentTimeMillis();
			LogUtils.info("成功初始化插件, 耗时:" + (end - start) + "ms");
			remotePlugin.configure(send);
			return remotePlugin;
		} catch (Exception e) {
			String msg = "初始化插件失败:" + StringUtils.printStackTrace(e);
			throw new ConfigException(msg);
		}
	}

	public static void closePlugin(ISendPlugin remotePlugin) throws IOException {
		if (null != remotePlugin) {
			remotePlugin.close();
		}
	}

	public void beforeSend(Object sendObj, Object[] row) throws IOException {

	};

	/**
	 * 配置相关内容
	 * 
	 * @param job
	 */
	public void configure(Send send) throws Exception {
		this.send = send;
	};

	/**
	 * 关闭资源
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
	};
}