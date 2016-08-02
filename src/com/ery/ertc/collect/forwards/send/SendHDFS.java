package com.ery.ertc.collect.forwards.send;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.forwards.MsgCache;
import com.ery.ertc.collect.forwards.Send;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgFieldPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.ertc.estorm.util.HDFSUtils;
import com.ery.ertc.estorm.util.HDFSUtils.CompressCodec;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;

public class SendHDFS extends Send {
	public String DataDir = "/collect";
	public String fileNamePriex;// host.yyyymmdd
	public String fileNameSuffix;// host.yyyymmdd
	public String writingFileSuffix = "~";// 正在写的文件的后缀名
	public String mergeFieldSplitChar = ",";
	public String mergeRowSplitChar = "\n";
	public long maxFileSize = 1 << 26;
	long lastCheckFileTime = 0;// 上次检查文件的时间,一小时检查一次是否需要重新创建新文件
	public final Map<String, String> storeParams;
	public int beachSize = 100;
	public MsgFieldPO[] msgFields;
	public Configuration conf;
	FileSystem fs;
	public DataOutputStream out;
	String compresseCodec;
	boolean isCompressed;
	boolean isCutoverDay;
	String yyyymmdd;
	Path curfile;
	long fileSeqId;
	long rowCount = 0;
	long fileSize = 0;
	Object muntx = new Object();

	public SendHDFS(MsgPO msgPO, MsgExtPO extPo, MsgCache cache) throws IOException, InterruptedException {
		super(msgPO, extPo, cache);
		yyyymmdd = new SimpleDateFormat("yyyyMMdd").format(new Date());
		storeParams = extPo.getStoreConfigParams();
		DataDir += "/" + msgPO.getMSG_TAG_NAME();
		DataDir = Convert.toString(storeParams.get("hdfs.filepath"), DataDir).replaceAll("(?i)\\{TAG_NAME\\}",
				msgPO.getMSG_TAG_NAME());
		beachSize = Convert.toInt(storeParams.get("hdfs.beachSize"), beachSize);
		msgFields = this.msgPO.getMsgFields().toArray(new MsgFieldPO[0]);
		conf = new Configuration();
		for (String key : storeParams.keySet()) {
			conf.set(key, storeParams.get(key));
		}
		isCompressed = storeParams.containsKey("hdfs.data.compresse");
		isCutoverDay = storeParams.containsKey("hdfs.data.cutover.day");
		compresseCodec = conf.get("hdfs.data.compresseCodec", CompressCodec.DefaultCodec.toString());
		writingFileSuffix = conf.get("hdfs.data.writingFileSuffix", writingFileSuffix);
		fileNamePriex = conf.get("hdfs.data.fileNamePriex", msgPO.getMSG_TAG_NAME());
		fileNameSuffix = conf.get("hdfs.data.fileNameSuffix", ".txt");
		maxFileSize = conf.getLong("hdfs.data.maxFileSize", maxFileSize);
		mergeFieldSplitChar = Convert.toString(storeParams.get("hdfs.data.column.SplitChar"), ",");
		mergeRowSplitChar = Convert.toString(storeParams.get("hdfs.data.row.SplitChar"), "\n");

		fs = FileSystem.get(conf);
		fileSeqId = getFileSeqId();
		out = getOutPutStream(fileSeqId);
	}

	// 获取HDFS上的文件列表 ，获取文件ID
	public long getFileSeqId() throws FileNotFoundException, IllegalArgumentException, IOException {
		Path p = new Path(DataDir);
		if (!fs.exists(p)) {
			fs.mkdirs(p);
			return 0;
		}
		FileStatus[] fss = fs.listStatus(p);
		if (fss == null || fss.length == 0)
			return 0;
		fileSeqId = 0;
		String fileNap = fileNamePriex + "_" + Config.getHostName() + "_yyyyMMdd.";
		String lastFile = "";
		for (FileStatus fileStatus : fss) {
			String fileName = fileStatus.getPath().getName();
			if (fileName.startsWith(fileNamePriex + "_" + Config.getHostName())) {
				long sid = 0;
				String ff = fileName.substring(fileNap.length());
				int ifd = ff.indexOf(".");
				ff = ff.substring(0, ifd > 0 ? ifd : ff.length());
				sid = Convert.toLong(ff);
				if (sid >= fileSeqId) {
					fileSeqId = sid;
					lastFile = fileName;
				}
			}
		}
		if (lastFile.equals("")) {
			return fileSeqId;
		} else if (writingFileSuffix != null && !writingFileSuffix.equals("") && lastFile.endsWith(writingFileSuffix)) {
			if (isCompressed) {// 默认不修改压缩方法
				CompressionCodec codec = HDFSUtils.getCompressCodec(compresseCodec, conf);
				// 不相同压缩 或未压缩
				if (!lastFile.endsWith(fileNameSuffix + codec.getDefaultExtension() + writingFileSuffix)) {
					checkFile(true);
					return fileSeqId + 1;
				}
			} else {
				if (!lastFile.endsWith(fileNameSuffix + writingFileSuffix)) {
					checkFile(true);
					return fileSeqId + 1;
				}
			}
			return fileSeqId;
		}
		return fileSeqId + 1;
	}

	public DataOutputStream getOutPutStream(long fileSeqId) throws IOException {
		long dt = System.currentTimeMillis();
		String ymd = new SimpleDateFormat("yyyyMMdd").format(new Date(dt));
		String fileName = DataDir + "/" + fileNamePriex + "_" + Config.getHostName() + "_" + ymd + "." + fileSeqId +
				fileNameSuffix;
		if (!isCompressed) {
			curfile = new Path(fileName + writingFileSuffix);
			FSDataOutputStream fsDataOut = null;
			Path p = curfile.getParent();
			if (!fs.exists(p)) {
				fs.mkdirs(p);
			}
			if (!fs.exists(curfile)) {
				fsDataOut = fs.create(curfile);
			} else {
				fsDataOut = fs.append(curfile);
			}
			return fsDataOut;
		} else {// 需要压缩 获取压缩标识
			CompressionCodec codec = HDFSUtils.getCompressCodec(compresseCodec, conf);
			curfile = new Path(fileName + codec.getDefaultExtension() + writingFileSuffix);
			Path p = curfile.getParent();
			if (!fs.exists(p)) {
				fs.mkdirs(p);
			}
			FSDataOutputStream fsDataOut = null;
			if (!fs.exists(curfile)) {
				fsDataOut = fs.create(curfile);
			} else {
				fsDataOut = fs.append(curfile);
			}
			return new DataOutputStream(codec.createOutputStream(fsDataOut));
		}
	}

	public void checkFile(boolean isforce) throws IOException {
		if (fileSize >= maxFileSize || isforce) {// 关闭文件
			out.close();
			String fileName = this.curfile.getName();
			if (writingFileSuffix != null && !writingFileSuffix.equals("") && fileName.endsWith(writingFileSuffix)) {
				fileName = fileName.substring(0, fileName.length() - writingFileSuffix.length());
				fs.rename(curfile, new Path(curfile.getParent(), fileName));
			}
			synchronized (muntx) {
				fileSeqId++;
				out = getOutPutStream(fileSeqId);// 创建新文件
			}
			fileSize = 0;
		}
		lastCheckFileTime = System.currentTimeMillis();
	}

	@Override
	public int send() throws IOException {
		// 切换文件
		if (isCutoverDay) {
			String _yyyymmdd = new SimpleDateFormat("yyyyMMdd").format(new Date());
			if (!_yyyymmdd.equals(yyyymmdd)) {
				checkFile(true);
			}
		}
		List<Object[]> todoSends = getMore(beachSize);
		if (todoSends == null || todoSends.size() == 0)
			return 0;
		try {
			long byteSize = 0;
			StringBuffer sb = new StringBuffer(1024);
			for (Object[] val : todoSends) {
				sb.append(val[0].toString());
				sb.append(mergeFieldSplitChar);
				sb.append(val[1].toString());
				for (int i = 0; i < this.msgFields.length; i++) {
					sb.append(mergeFieldSplitChar);
					Object o = val[i + 2];
					if (this.msgFields[i].getType() == 8) {
						try {
							sb.append(this.msgFields[i].getSimpleDateFormat().format(new Date(Convert.toLong(o))));
						} catch (Exception e) {
							sb.append(o.toString());
						}
					} else {
						sb.append(o.toString());
					}
				}
				if (sendPlugin != null) {
					ISendPlugin.beforeSend(sendPlugin, sb, val);
				}
				sb.append(this.mergeRowSplitChar);
				if (sb.length() > 1 << 20) {// 1M
					byte[] tmpbts = sb.toString().getBytes();
					byteSize += tmpbts.length;
					out.write(tmpbts);
					sb.setLength(0);
				}
			}
			byte[] tmpbts = sb.toString().getBytes();
			byteSize += tmpbts.length;
			fileSize += byteSize;
			out.write(tmpbts);
			out.flush();
			long more1000 = rowCount / 10000;
			rowCount += todoSends.size();
			if ((rowCount / 10000) != more1000 || System.currentTimeMillis() - lastCheckFileTime > 3600000) {
				checkFile(false);
			}
			DaemonMaster.daemonMaster.workerService.notifySendOK(msgId, stroeId, todoSends.size(), byteSize);
			return todoSends.size();
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			DaemonMaster.daemonMaster.workerService.notifySendError(msgId, stroeId, sw.toString(), todoSends);
			LogUtils.error("发送向HDFS出错!" + e.getMessage(), e);
			sendFail(todoSends);
			isExped = true;
			throw new IOException(e);
		}
	}

	boolean isExped = false;

	public void stop() {
		try {
			super.stop();
			if (!isExped)
				out.flush();
			out.close();
			String fileName = this.curfile.getName();
			if (writingFileSuffix != null && !writingFileSuffix.equals("") && fileName.endsWith(writingFileSuffix)) {
				fileName = fileName.substring(0, fileName.length() - writingFileSuffix.length());
				fs.rename(curfile, new Path(fileName));
			}
			fileSize = 0;
		} catch (Exception e) {

		}
	}

}
