package com.ery.ertc.collect.remote.client;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.forwards.QueueUtils;
import com.ery.ertc.collect.task.dao.CollectDAO;
import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.collect.worker.executor.PollTaskExecutor;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.po.POConstant;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.DataSourceManager;
import com.ery.base.support.sys.SystemConstant;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.RemoteFileTransport;
import com.ery.base.support.utils.StringUtils;
import com.ery.base.support.utils.Utils;

public class FileCollectExec extends CollectExec {

	/**
	 * key:目录 (因为存在通配符和宏变量，因此可能会读取到多个目录的文件) value:数组： arg0:最后时刻读取的文件名
	 * arg1:最后时刻读取文件的修改时间 arg2:最后读取位置offset
	 */
	private Map<String, List<Object>> lastPointerData;

	private final int TRANSPORT_FTP = 1;
	private final int TRANSPORT_SFTP = 2;

	private RemoteFileTransport remoteFtp;
	private int transportType;
	private boolean inited = false;

	public FileCollectExec(AbstractTaskExecutor executor) {
		super(executor);
		lastPointerData = new HashMap<String, List<Object>>();
		if (lastPointer != null && !"".equals(lastPointer)) {
			for (Map.Entry<String, Object> entry : JSON.parseObject(lastPointer).entrySet()) {
				lastPointerData.put(entry.getKey(), (List<Object>) entry.getValue());
			}
		}
	}

	@Override
	public String getLastCollectPointer() {
		lastPointer = JSON.toJSONString(lastPointerData);
		return lastPointer;
	}

	private void init() throws Exception {
		if (!inited) {
			MsgPO msgPO = executor.getMsgPO();
			String[] arr = msgPO.getURL().split(":");
			remoteFtp = new RemoteFileTransport(arr[0], Convert.toInt(arr[1]), msgPO.getUSER(), msgPO.getPASS());
			if ("ftp".equals(reqFtpType)) {
				transportType = TRANSPORT_FTP;
				remoteFtp.setTransportType(RemoteFileTransport.TRANSPORT_TYPE_FTP);
			} else if ("sftp".equals(reqFtpType)) {
				transportType = TRANSPORT_SFTP;
				remoteFtp.setTransportType(RemoteFileTransport.TRANSPORT_TYPE_FTP);
			}
			remoteFtp = new RemoteFileTransport(arr[0], Convert.toInt(arr[1]), msgPO.getUSER(), msgPO.getPASS());
			remoteFtp.setTimeout(reqTimeout);
			remoteFtp.setCharset(reqCharset);
			remoteFtp.setContentType(RemoteFileTransport.CONTENT_TYPE_BINARY);
			remoteFtp.setTransportType(transportType);
			remoteFtp.init();
			inited = true;
			srcClient = arr[0];
		}
		remoteFtp.connect();
	}

	@Override
	public void execute() throws Exception {
		init();
		String fileName = reqFileName;
		fileName = fileName.replaceAll("//", "/");
		fileName = fileName.replaceAll("\\*", ".*");
		String[] arr = fileName.split("/");// 按目录隔断
		searchFile(0, arr, "");
	}

	// 搜索文件（根据宏变量和通配符搜索）
	private void searchFile(int a, String[] arr, String rootPath) throws Exception {
		String regex = "\\$\\{(\\w*)\\}";
		for (int i = a; i < arr.length; i++) {
			String name = arr[i];
			if (name.matches(regex) || name.contains(".*")) {// 发现带宏变量或通配符
				name = StringUtils.replaceAll(name, regex, new StringUtils.ReplaceCall() {
					@Override
					public String replaceCall(String... groupStrs) {
						String d = groupStrs[1].toLowerCase();
						if ("yy".equals(d)) {
							return "(\\\\d{2})";
						} else if ("yyyy".equals(d)) {
							return "(\\\\d{4})";
						} else if ("mm".equals(d)) {
							return "(0?[1-9]|1[0-2])";
						} else if ("dd".equals(d)) {
							return "(0?[1-9]|[12]\\\\d|3[0-1])";
						} else if ("h".equals(d)) {
							return "([0-1]\\\\d|2[0-3])";
						} else if ("m".equals(d)) {
							return "([0-5]\\\\d)";
						} else if ("s".equals(d)) {
							return "([0-5]\\\\d)";
						}
						return groupStrs[0];
					}
				});
				if (remoteFtp.exists(rootPath)) {
					remoteFtp.changeDir(rootPath);
					List<RemoteFileTransport.RemoteFile> fs = remoteFtp.listFiles(RemoteFileTransport.SORT_TIME_ASC,
							name);
					if (i != arr.length - 1) {
						for (RemoteFileTransport.RemoteFile f_ : fs) {
							if (f_.isDir()) {
								searchFile(i + 1, arr, rootPath + f_.getName() + "/");
							}
						}
					} else {
						// 最后一级标示文件
						for (RemoteFileTransport.RemoteFile f_ : fs) {
							if (f_.isFile()) {
								readContent(f_);
							}
						}
					}
				}
			} else {
				if (i != arr.length - 1) {
					rootPath += name + "/";
				} else {
					if (remoteFtp.exists(rootPath)) {
						remoteFtp.changeDir(rootPath);
						if (remoteFtp.exists(rootPath + name)) {
							RemoteFileTransport.RemoteFile f = remoteFtp.getFile(rootPath + name);
							if (f.isFile()) {
								readContent(f);
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void stopRun() throws Exception {
		if (remoteFtp != null) {
			remoteFtp.disconnect();
		}
	}

	// 读取信息
	private void readContent(final RemoteFileTransport.RemoteFile file) throws Exception {
		// 根据规则，lastPointer（上次的采集点)验证，将采集过的数据排除掉
		List<Object> pointer = lastPointerData.get(file.getDir());
		if (pointer == null) {
			pointer = new ArrayList<Object>() {
				{
					add(file.getName());
					add(0);
					add(0);
				}
			};
			lastPointerData.put(file.getDir(), pointer);
		}
		String fn = Convert.toString(pointer.get(0));
		long time = Convert.toLong(pointer.get(1), 0);
		long offset = Convert.toLong(pointer.get(2), 0);
		if (file.getLastModified().getTime() < time) {
			return;// 读过的文件
		} else if (file.getLastModified().getTime() == time && fn.equals(file.getName()) && offset == file.getSize()) {
			return;// 数据库记录的文件状态（未发生任何改变）
		} else if (file.getLastModified().getTime() == time && fn.equals(file.getName()) && offset < file.getSize()) {
			// 上次未读完
			LogUtils.info("[" + srcClient + "] Old File:" + file.getPath() + ",Lave:" + (file.getSize() - offset));
		} else {
			if (file.getLastModified().getTime() > time && fn.equals(file.getName())) {
				// 文件内容在实时改变
				LogUtils.info("[" + srcClient + "] File Changed:" + file.getPath() + ",Last offset:" + offset);
			} else {
				// 新文件
				LogUtils.info("[" + srcClient + "] New File:" + file.getPath() + ",Size:" + file.getSize());
				offset = 0;
				fn = file.getName();
			}
			time = file.getLastModified().getTime();
			pointer.set(0, fn);
			pointer.set(1, time);
			pointer.set(2, offset);
		}
		MsgPO msgPO = executor.getMsgPO();
		long size = file.getSize();
		InputStream ftpStream = remoteFtp.getInputStream(file.getPath(), offset);
		try {
			InputStreamReader isr = new InputStreamReader(ftpStream, dataCharset);// 将字节流转换成字符流,读取到指定分割符时，识别成一条记录
			try {
				long s = System.currentTimeMillis();
				if (itemSplitCh != null) {
					parseMsgForItemSplitCh(isr, size, offset, pointer, msgPO);
				} else if (newItemStartCh != null) {
					parseMsgForItemStartCh(isr, size, offset, pointer, msgPO);
				}
				LogUtils.info("[" + srcClient + "][" + file.getPath() + "]" + (size - offset) + " bytes耗时:" +
						(System.currentTimeMillis() - s) / 1000.0 + " s!");
			} finally {
				isr.close();
			}
		} finally {
			ftpStream.close();

			((PollTaskExecutor) executor).saveCollectPointer();// 读完一个文件，保存一次位置
		}
	}

	// 解析消息流（通过拆分符）
	private void parseMsgForItemSplitCh(InputStreamReader isr, long size, long offset, List<Object> pointer, MsgPO msgPO)
			throws Exception {
		long len = size - offset;
		if ("\r".equals(itemSplitCh)) {
			itemSplitCh = "\n";
		} // 统一识别回车符和换行符
		int lds = itemSplitCh.toCharArray().length;// 拆分符字符数
		int chbs = itemSplitCh.getBytes().length;
		if (lds == 0) {
			throw new RuntimeException("拆分符长度必须大于0");
		}
		len = (len <= 1024 * 64 ? len : 1024 * 4);// 如果整个未读取内容不超过64k（因为中文字符占2b关系，可能不准），一次读完，否则一次读4k
		char[] chs = new char[lds];// 依次存放读取的拆分符那么长的字符
		char[] laveChs = null;// 每次读取对比剩余的字符
		StringBuilder sb = new StringBuilder();
		int rl;// 读取出的字符数量
		int chl_ = 0;// 匹配后，下次匹配需要过滤的字符标记
		// System.out.println("----------------------");
		while (offset < size) {
			char[] cb = new char[(int) len];
			rl = isr.read(cb);
			if (rl == -1) {
				// 读完了，处理缓冲区剩余的字符
				if (laveChs != null) {
					char[] lave = new char[laveChs.length - chl_];
					System.arraycopy(laveChs, chl_, lave, 0, lave.length);
					sb.append(lave);
				}
				if (sb.length() > 0) {
					String data = sb.toString();
					offset = sendMsg(msgPO, data, offset, 0);
					pointer.set(2, offset);
					sb.delete(0, sb.length());
				}
				break;
			}
			int laveLen = laveChs != null ? laveChs.length : 0;
			if (rl < cb.length) {
				// 末次，取出其中有效字符和缓冲区剩余字符
				char[] nch = new char[rl + laveLen];
				if (laveLen > 0) {
					System.arraycopy(laveChs, 0, nch, 0, laveLen);
				}
				System.arraycopy(cb, 0, nch, laveLen, rl);
				cb = nch;
				laveChs = null;
				rl = cb.length;
			} else {
				// 将上次缓冲剩余的字符放到字符前端
				if (laveLen > 0) {
					char[] nch = new char[cb.length + laveLen];
					System.arraycopy(laveChs, 0, nch, 0, laveLen);
					System.arraycopy(cb, 0, nch, laveLen, cb.length);
					cb = nch;
					laveChs = null;
				}
			}

			for (int x = 0; x < rl;) {
				if (rl - x >= lds) {
					// 依次取出末尾与分割符等长的字符
					System.arraycopy(cb, x, chs, 0, lds);
					String lastCh = new String(chs);
					if (itemSplitCh.equals(lastCh) || (itemSplitCh.equals("\n") && lastCh.equals("\r"))) {
						String data = sb.toString();
						offset = sendMsg(msgPO, data, offset, chbs);
						pointer.set(2, offset);
						sb.delete(0, sb.length());
						chl_ = lds - 1;
					} else if (chl_ == 0) {
						sb.append(chs[0]);
					} else if (chl_ > 0) {
						chl_--;
					}
					x++;
				} else {
					laveChs = new char[rl - x];
					System.arraycopy(cb, x, laveChs, 0, laveChs.length);
					x += laveChs.length;
					if (chl_ > 0) {
						chl_ -= (rl - x);
					}
				}
			}
		}
	}

	// 解析消息流（通过消息起始识别符）
	private void parseMsgForItemStartCh(InputStreamReader isr, long size, long offset, List<Object> pointer, MsgPO msgPO)
			throws Exception {
		long len = size - offset;
		if (newItemStartCh.length() == 0) {
			throw new RuntimeException("起始符长度必须大于0");
		}
		len = (len <= 1024 * 64 ? len : 1024 * 4);// 如果整个未读取内容不超过64k（因为中文字符占2b关系，可能不准），一次读完，否则一次读4k
		StringBuilder sb = new StringBuilder();
		int rl;// 读取出的字符数量
		while (offset < size) {
			char[] cb = new char[(int) len];
			rl = isr.read(cb);
			if (rl == -1) {
				// 已读完
				if (sb.length() > 0) {
					offset = sendMsg(msgPO, sb.toString(), offset, 0);
					pointer.set(2, offset);
					sb.delete(0, sb.length());
				}
				break;
			}
			if (rl < cb.length) {
				// 末次
				char[] nch = new char[rl];
				System.arraycopy(cb, 0, nch, 0, rl);
				cb = nch;
			}

			String str = new String(cb);
			Matcher matcher = Pattern.compile(getStartChRegex()).matcher(str);
			int index = 0;
			while (matcher.find()) {
				if (matcher.start() > index) {
					// 前半截需要追加到上次
					sb.append(str.substring(index, matcher.start()));
					offset = sendMsg(msgPO, sb.toString(), offset, 0);
					pointer.set(2, offset);
					sb.delete(0, sb.length());
				}
				sb.append(matcher.group());
				index = matcher.end();
			}
			if (index == 0) {
				sb.append(str);// 整个字符都属于上次
			} else {
				sb.append(str.substring(index, str.length()));
			}
		}

	}

	// 本地测试时，需要将里面代码切换
	private long sendMsg(MsgPO msgPO, String msg, long offset, int chsize) {
		long l = 0l;
		if (msg != null && !"".equals(msg)) {
			// 运行时代码，发送至kafka
			List<Object> arr = QueueUtils.convertMsgToTuple(msgPO, dataType, msg, splitCh);
			if (DaemonMaster.daemonMaster.queueUtils.appendMsg(msgPO, arr)) {
				l = msg.getBytes().length;
				DaemonMaster.daemonMaster.workerService.notifyCollectOK(msgPO.getMSG_ID(), l, srcClient);
			}
		}

		// //调试代码(本地测试时打开)
		// System.out.println("===="+msg);
		// CNT ++;
		// SIZE += l + chsize;

		return offset + l + chsize;
	}

	static long CNT = 0;
	static long SIZE = 0;

	public static void main(String[] args) throws Exception {
		System.out.println(System.currentTimeMillis());
		// 本地测试需要将发送kafka代码注释
		MsgPO po = new MsgPO();
		po.setMSG_ID(999);
		po.setTRIGGER_TYPE(POConstant.TRI_TYPE_I_FILE);
		po.setMSG_TAG_NAME("TEST_KAFKA_SERVER_LOG");
		po.setURL("192.168.10.103:22");
		po.setUSER("mq");
		po.setPASS("mqmq");
		Map<String, Object> param = new HashMap<String, Object>();
		param.put(POConstant.Msg_REQ_PARAM_fileFtpType, "sftp");
		param.put(POConstant.Msg_REQ_PARAM_fileFileName, "/home/mq/kafka_2.8.0-0.8.0/bin/logs/server.log.2014-05-04-09");
		po.setREQ_PARAM(JSON.toJSONString(param));

		Map<String, Object> dataScheme = new HashMap<String, Object>();
		// dataScheme.put(POConstant.Msg_DATA_SCHEME_itemSplitCh,"\n");
		dataScheme.put(POConstant.Msg_DATA_SCHEME_newItemStartCh, "\\[${yyyy}-${mm}-${dd} ${h}:${m}:${s},${sss}\\]");
		po.setDATA_SCHEME(JSON.toJSONString(dataScheme));

		SystemConstant.setSYS_CONF_FILE("conf/collect.properties");
		DataSourceManager.dataSourceInit();
		MsgPO po1 = new CollectDAO().getCollectMsgInfo("3");

		po.setMsgFields(po1.getMsgFields());
		po.setDATA_SCHEME(po1.getDATA_SCHEME());
		FileCollectExec fce = new FileCollectExec(new PollTaskExecutor(po, null));

		Thread th = new Thread() {
			public void run() {
				long cnt = 0;
				long size = 0;
				while (true) {
					long c = CNT;
					long s = SIZE;
					System.out.println("总数:" + c + ",总大小:" + s + ",cntTps:" + (c - cnt) + ",sizeTps:" + (s - size));
					cnt = c;
					size = s;
					Utils.sleep(1000);
				}
			}
		};

		th.start();
		fce.execute();
		fce.stopRun();
		// System.out.println(Pattern.matches("\\(9i专用.*.sql",
		// "(9i专用)列行转换函数_01.sql"));
		Utils.sleep(30000);
		th.stop();
	}
}
