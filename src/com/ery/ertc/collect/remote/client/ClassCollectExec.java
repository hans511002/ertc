package com.ery.ertc.collect.remote.client;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.base.support.utils.ClassUtils;

public class ClassCollectExec extends CollectExec {

	private boolean inited = false;
	private Class clazz;
	private Object classInstance;
	private boolean execed = false;

	public ClassCollectExec(AbstractTaskExecutor executor) {
		super(executor);
	}

	@Override
	public void execute() throws Exception {
		// 不是轮询
		try {
			init();
		} catch (Exception e) {
			if (!execed) {
				throw new RuntimeException(e);
			}
		}
		if (reqIsNotPoll == 0) {
			execed = true;
			// 轮询
			exec();
		} else if (!execed) {
			execed = true;
			// 不轮询
			exec();
		}
	}

	private void init() throws Exception {
		if (inited) {
			return;
		}
		inited = true;
		if (reqJarFile != null && !"".equals(reqJarFile)) {
			clazz = ClassUtils.getClassByJar(reqJarFile, reqClassName);
		} else if (reqJavaCode != null && !"".equals(reqJavaCode)) {
			clazz = ClassUtils.getClassByCode(reqJavaCode, reqClassName);
		} else {
			throw new RuntimeException("配置不全，无法找到执行类[" + reqClassName + "]");
		}
	}

	private void exec() throws Exception {
		if (clazz != null) {
			if (clazz.isAssignableFrom(CollectPlugin.class)) {
				// 接口子类
				if (classInstance == null) {
					CollectPlugin cp = (CollectPlugin) (clazz.newInstance());
					classInstance = cp;
					cp.setMsgPO(executor.getMsgPO());
					cp.call();
				} else {
					((CollectPlugin) classInstance).call();
				}
			} else {
				Method method = clazz.getDeclaredMethod(reqMethodName, String.class);
				// 非接口子类
				if (classInstance == null) {
					classInstance = clazz.newInstance();
				}
				method.invoke(classInstance, reqArgs);
			}
		}
	}

	@Override
	public void stopRun() {
		if (clazz != null && clazz.isAssignableFrom(CollectPlugin.class) && classInstance != null) {
			((CollectPlugin) classInstance).destroy();
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException,
			IllegalAccessException, InstantiationException, InvocationTargetException {
		// 1.将代码写入内存中
		StringWriter writer = new StringWriter(); // 内存字符串输出流
		PrintWriter out = new PrintWriter(writer);
		out.println("package com.dongtai.hello;");
		out.println("public class Hello{");
		out.println("public void main(String[] args){");
		out.println("System.out.println(\"HelloWorld! 3\");");
		out.println("}");
		out.println("}");
		out.flush();
		out.close();

		// 2.开始编译
		JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
		JavaFileObject fileObject = new ClassUtils.JavaStringObject("Hello", writer.toString());
		JavaCompiler.CompilationTask task = javaCompiler.getTask(null, null, null,
				Arrays.asList("-d", ClassLoader.getSystemClassLoader().getResource("").getPath()), null,
				Arrays.asList(fileObject));
		boolean success = task.call();

		if (!success) {
			System.out.println("编译失败");
		} else {
			System.out.println("编译成功");
		}
		Class<?> classl = ClassLoader.getSystemClassLoader().loadClass("com.dongtai.hello.Hello");
		Method method = classl.getDeclaredMethod("main", String[].class);
		Object[] argsl = { null };
		method.invoke(classl.newInstance(), argsl);
	}

}
