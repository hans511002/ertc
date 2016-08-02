package com.ery.ertc.collect;

import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.zk.StringZkSerializer;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.SystemConstant;
import com.ery.base.support.sys.SystemVariable;
import com.ery.base.support.utils.Utils;

/**
 * Copyrights @ 2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @description 采集主线程 集群中，运行多个采集程序，单Master模式，Master选取靠抢占，谁抢到算谁。
 *              Master：运行任务（是否可运行任务，可通过配置），分配任务 Worker：运行任务
 * 
 *              后台运行：> /dev/null 2>&1 &
 * 
 * @date 15-7-7 -
 * @modify
 * @modifyDate 15-7-7
 */
public class CollectMain {

	public static CommandLine buildCommandline(String[] args) {
		final Options options = new Options();
		Option opt = new Option("h", "help", false, "打印帮助");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("start", null, false, "启动，不可与stop同时出现");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("stop", null, true, "停止[节点ID]，不可与start同时出现-999表停止所有");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("localTest", null, false, "本机测试");
		opt.setRequired(false);
		options.addOption(opt);

		PosixParser parser = new PosixParser();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(110);
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption('h')) {
				hf.printHelp("queue", options, true);
				return null;
			}
			if (commandLine.hasOption("start") && commandLine.hasOption("stop")) {
				hf.printHelp("queue", options, true);
				return null;
			}
			if (!commandLine.hasOption("start") && !commandLine.hasOption("stop")) {
				hf.printHelp("queue", options, true);
				return null;
			}
		} catch (ParseException e) {
			hf.printHelp("queue", options, true);
			return null;
		}

		return commandLine;
	}

	// 采集程序信号捕获接口
	static class ProcessSignal implements SignalHandler {

		private DaemonMaster daemonMaster;

		ProcessSignal(DaemonMaster daemonMaster) {
			this.daemonMaster = daemonMaster;
		}

		@Override
		public void handle(Signal signal) {
			String nm = signal.getName();
			if (nm.equals("TERM") || nm.equals("INT") || nm.equals("KILL")) {
				LogUtils.info("程序捕获到[" + nm + "]信号,即将停止!");
				daemonMaster.stop();
			}
		}
	}

	public static void main(String[] args) {
		System.setProperty("logFileName", "collect");
		Utils.sleep(10000);
		CommandLine cmd = buildCommandline(args);
		if (cmd != null) {
			boolean start = cmd.hasOption("start");
			SystemConstant.setSYS_CONF_FILE("collect.properties");
			SystemConstant.setLOAD_DB_DATA_SOURCE(true);// 加载数据库数据源
			System.setProperty("logFileName", SystemVariable.getLogFileName());
			DaemonMaster daemonMaster = null;
			if (start) {
				try {
					daemonMaster = new DaemonMaster();// 注册捕获kill信号
					ProcessSignal processSignal = new ProcessSignal(daemonMaster);
					Signal.handle(new Signal("TERM"), processSignal);// kill命令
					Signal.handle(new Signal("INT"), processSignal);// ctrl+c 命令
					// Signal.handle(new Signal("KILL"), processSignal);//kill
					// -9 不可被捕获
					// 启动各个线程
					daemonMaster.start();
				} catch (Throwable e) {
					LogUtils.error("发生错误:" + e.getMessage() + " --程序即将退出!", e);
					try {
						if (daemonMaster != null) {
							daemonMaster.stop();
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					} finally {
						System.exit(0);
					}
				}
			} else {
				String stopid = cmd.getOptionValue("stop", "-999");
				// 改变ZK信息，通知本机运行程序中各线程停止运行
				ZkClient zkClient = new ZkClient(Config.getZkUrl(), Config.getZkSessionTOMS(),
						Config.getZkConnectTOMS(), new StringZkSerializer());
				try {
					if (stopid != null && !"".equals(stopid) && !"-999".equals(stopid)) {
						String path = ZkConstant.BASE_NODE + "/" + ZkConstant.HOST_NODE + "/" + stopid;
						if (zkClient.exists(path)) {
							Object o = zkClient.readData(path);
							Map<String, Object> clusterNodeInfo = JSON.parseObject(o.toString());
							clusterNodeInfo.put(ZkConstant.NODE_STOP_FLAG, "1");
							zkClient.writeData(path, JSON.toJSONString(clusterNodeInfo));
							LogUtils.info("已发布停止命令，节点[" + stopid + "]即将停止！");
						}
					} else {
						String path = ZkConstant.BASE_NODE + "/" + ZkConstant.HOST_NODE;
						if (zkClient.exists(path)) {
							List<String> nodes = zkClient.getChildren(path);
							for (String collectId : nodes) {
								Object o = zkClient.readData(path + "/" + collectId);
								Map<String, Object> clusterNodeInfo = JSON.parseObject(o.toString());
								clusterNodeInfo.put(ZkConstant.NODE_STOP_FLAG, "1");
								zkClient.writeData(path + "/" + collectId, JSON.toJSONString(clusterNodeInfo));
							}
							LogUtils.info("已发布停止命令，集群即将停止！");
						}
					}
					System.exit(0);
				} finally {
					zkClient.close();
				}
			}
		}
	}
}
