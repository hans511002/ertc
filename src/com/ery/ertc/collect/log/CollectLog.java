package com.ery.ertc.collect.log;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.estorm.po.LogPO;
import com.ery.ertc.estorm.po.MsgExtPO;
import com.ery.ertc.estorm.po.MsgPO;
import com.ery.base.support.jdbc.DBUtils;
import com.ery.base.support.jdbc.IParamsSetter;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.podo.BaseDAO;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class CollectLog {

	private final static Logger logger = Logger.getLogger(CollectLog.class.getCanonicalName());// 日志打印对象
	static ConcurrentLinkedQueue<CollectLogDetail> logDetails = new ConcurrentLinkedQueue<CollectLogDetail>();
	static ConcurrentLinkedQueue<CollectLogDetail> dbDetails = new ConcurrentLinkedQueue<CollectLogDetail>();

	public static String LOG_PATH = "../collect_logs";

	static String TYPE_FILTER = "FILTER";
	static String TYPE_DISTINCT = "DISTINCT";
	static String TYPE_COLLECT = "COLLECT";
	static String TYPE_SEND = "SEND";
	static String HOST_NAME = Config.getHostName();
	static boolean started = false;
	static AtomicLong logNum = new AtomicLong(0);
	static AtomicLong dbNum = new AtomicLong(0);
	static Thread logThread;
	static Thread dbThread;
	static boolean running = true;
	static {
		logger.setUseParentHandlers(false);
		Handler[] handlers = logger.getHandlers();
		for (Handler handler : handlers) {
			logger.removeHandler(handler);
		}
		logger.setLevel(Level.ALL);
		String filePath;
		if (Config.getCollectLogDir().startsWith("/")) {
			filePath = Config.getCollectLogDir();
		} else {
			filePath = Config.getCurrentWorkDir() + "/" + Config.getCollectLogDir();
		}
		File file = new File(filePath);
		if (!file.exists()) {
			file.mkdirs();
		}
		try {
			Handler handler = new FileStreamHandler(file.getAbsolutePath(), "detailLog");
			handler.setLevel(Level.ALL);
			handler.setEncoding("UTF-8");
			handler.setFormatter(new LogFormatter());
			logger.addHandler(handler);
			logThread = new Thread() {
				public void run() {
					while (running) {
						try {
							CollectLogDetail logDetail = logDetails.poll();
							if (logDetail == null) {
								Utils.sleep(500);
							} else {
								logNum.decrementAndGet();
								StringBuilder sb = new StringBuilder();
								sb.append(logDetail.TIME);
								String logmsgid = "[" + logDetail.getMSG_ID() + "," + logDetail.LOG_SN_ID + "]";
								sb.append(logmsgid).append(" ");
								sb.append(logDetail.getSERVER_HOST()).append(" ");
								sb.append(logDetail.TYPE);
								for (int i = 0; i < 10 - logDetail.TYPE.length(); i++) {
									sb.append(" ");
								}
								sb.append(logDetail.CONTENT).append("\n");
								recordLog(sb.toString());
							}
						} catch (Throwable e) {
							LogUtils.error(null, e);
						} finally {
						}
					}
				}
			};
			dbThread = new Thread() {
				public void run() {
					List<CollectLogDetail> dbs = new ArrayList<CollectLogDetail>(2000);
					DAO dao = null;
					while (running) {
						try {
							if (dao == null) {
								dao = new DAO();
							}
							dbs.clear();
							boolean empty = false;
							for (int i = 0; i < 2000; i++) {
								CollectLogDetail logDetail = dbDetails.poll();
								if (logDetail == null) {
									empty = true;
									break;
								} else {
									dbNum.decrementAndGet();
									dbs.add(logDetail);
								}
							}
							if (dbs.size() > 0) {// 存储到数据库
								dao.recordLog(dbs);
							}
							if (empty) {
								Utils.sleep(3000);
								dao.close();
								dao = null;
							} else if (dbs.size() < 2000) {
								Utils.sleep(800);
								dao.close();
								dao = null;
							}
						} catch (Throwable e) {
							try {
								dbDetails.addAll(dbs);
								dao.close();
								dao = null;
							} catch (Exception ex) {
								LogUtils.error(null, ex);
							}
							LogUtils.error(null, e);
						}
					}
				}
			};
			logThread.start();
			dbThread.start();
			started = true;
		} catch (Exception e) {
		}
	}

	public static void destory() {
		running = false;
		Utils.sleep(2000);
		if (logThread != null) {
			logThread.stop();
		}
		if (dbThread != null) {
			dbThread.stop();
		}
		started = false;
	}

	// 对应表ST_MSG_DETAIL_LOG
	static class CollectLogDetail extends LogPO {
		long LOG_SN_ID;
		long STORE_ID;
		String TIME;
		String TYPE;
		String CONTENT;
	}

	static class DAO extends BaseDAO {
		void recordLog(final List<CollectLogDetail> logDetailList) {
			String sql = "INSERT INTO ST_MSG_DETAIL_LOG (LOG_ID,MSG_ID,STORE_ID,LOG_SN_ID,TIME,TYPE,CONTENT,SERVER_HOST) "
					+ "VALUES(?,?,?,?,?,?,?,?)";
			getDataAccess().execUpdateBatch(sql, new IParamsSetter() {
				@Override
				public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
					CollectLogDetail logDetail = logDetailList.get(i);
					preparedStatement.setLong(1, DBUtils.getPrimaryKeyID("ST_MSG_DETAIL_LOG"));
					preparedStatement.setLong(2, logDetail.getMSG_ID());
					preparedStatement.setLong(3, logDetail.STORE_ID);
					preparedStatement.setLong(4, logDetail.LOG_SN_ID);
					preparedStatement.setString(5, logDetail.TIME);
					preparedStatement.setString(6, logDetail.TYPE);
					preparedStatement.setString(7, logDetail.CONTENT);
					preparedStatement.setString(8, logDetail.getSERVER_HOST());
				}

				@Override
				public int batchSize() {
					return logDetailList.size();
				}
			});
		}
	}

	// 过滤日志
	public static void filterLog(MsgPO msgPO, MsgExtPO extPo, Object[] tuples, long logIdSn) {
		if (extPo.getLOG_LEVEL() == 0 || !started)
			return;
		CollectLogDetail logDetail = new CollectLogDetail();
		logDetail.TIME = Convert.toTimeStr(new Date(), null);
		logDetail.TYPE = TYPE_FILTER;
		logDetail.setMSG_ID(msgPO.getMSG_ID());
		logDetail.LOG_SN_ID = Convert.toLong(tuples[tuples.length - 1]);
		logDetail.STORE_ID = extPo.getSTORE_ID();
		logDetail.setSERVER_HOST(HOST_NAME);
		logDetail.CONTENT = getContent(tuples);
		if (extPo.getLOG_LEVEL() == 1) {
			logDetails.offer(logDetail);
			logNum.incrementAndGet();
		} else if (extPo.getLOG_LEVEL() == 2) {
			dbDetails.offer(logDetail);
			dbNum.incrementAndGet();
		}
	}

	// 去重日志
	public static void distinctLog(MsgPO msgPO, MsgExtPO extPo, Object[] tuples, long logIdSn) {
		if (extPo.getLOG_LEVEL() == 0 || !started)
			return;
		CollectLogDetail logDetail = new CollectLogDetail();
		logDetail.TIME = Convert.toTimeStr(new Date(), null);
		logDetail.TYPE = TYPE_DISTINCT;
		logDetail.setMSG_ID(msgPO.getMSG_ID());
		logDetail.LOG_SN_ID = logIdSn;
		logDetail.STORE_ID = extPo.getSTORE_ID();
		logDetail.setSERVER_HOST(HOST_NAME);
		logDetail.CONTENT = getContent(tuples);
		if (extPo.getLOG_LEVEL() == 1) {
			logDetails.offer(logDetail);
			logNum.incrementAndGet();
		} else if (extPo.getLOG_LEVEL() == 2) {
			dbDetails.offer(logDetail);
			dbNum.incrementAndGet();
		}
	}

	// 采集日志
	public static void collectLog(MsgPO msgPO, List<Object> tuples, long logIdSn) {
		MsgExtPO extPo = msgPO.getExtPO().get(0);
		if (extPo.getLOG_LEVEL() == 0 || !started)
			return;
		CollectLogDetail logDetail = new CollectLogDetail();
		logDetail.TIME = Convert.toTimeStr(new Date(), null);
		logDetail.TYPE = TYPE_COLLECT;
		logDetail.setMSG_ID(msgPO.getMSG_ID());
		logDetail.LOG_SN_ID = logIdSn;
		logDetail.setSERVER_HOST(HOST_NAME);
		logDetail.CONTENT = getContent(tuples);
		if (extPo.getLOG_LEVEL() == 1) {
			logDetails.offer(logDetail);
			logNum.incrementAndGet();
		} else if (extPo.getLOG_LEVEL() == 2) {
			dbDetails.offer(logDetail);
			dbNum.incrementAndGet();
		}
	}

	// 发送日志
	public static void sendLog(MsgPO msgPO, MsgExtPO extPo, Object[] msg, long logIdSn) {
		if (extPo.getLOG_LEVEL() == 0 || !started)
			return;
		CollectLogDetail logDetail = new CollectLogDetail();
		logDetail.TIME = Convert.toTimeStr(new Date(), null);
		logDetail.TYPE = TYPE_SEND;
		logDetail.STORE_ID = extPo.getSTORE_ID();
		logDetail.setMSG_ID(msgPO.getMSG_ID());
		logDetail.LOG_SN_ID = logIdSn;
		logDetail.setSERVER_HOST(HOST_NAME);
		logDetail.CONTENT = getContent(msg);
		if (extPo.getLOG_LEVEL() == 1) {
			logDetails.offer(logDetail);
			logNum.incrementAndGet();
		} else if (extPo.getLOG_LEVEL() == 2) {
			dbDetails.offer(logDetail);
			dbNum.incrementAndGet();
		}
	}

	private static String getContent(Object[] tuples) {
		return getContent(Arrays.asList(tuples));
	}

	private static String getContent(List<Object> tuples) {
		StringBuilder sb = new StringBuilder("[");
		for (Object o : tuples) {
			if (o instanceof byte[]) {
				sb.append(Convert.toHexString((byte[]) o));
			} else {
				sb.append(o);
			}
			sb.append("|");
		}
		sb.setLength(sb.length() - 1);
		sb.append("]");
		return sb.toString();
	}

	private static void recordLog(String msg) {
		logger.log(Level.INFO, msg);
	}

	static class FileStreamHandler extends StreamHandler {
		// 希望写入的日志路径
		private String fileDir;
		// 文件名前缀
		private String filePrefix;
		private long lastTime = 0L;
		private Timer timer = new Timer();
		private String lastFile = null;

		public FileStreamHandler(String fileDir, String filePrefix) throws Exception {
			super();
			this.fileDir = fileDir;
			this.filePrefix = filePrefix;
			openWriteFiles();
		}

		/**
		 * 获得将要写入的文件
		 */
		private synchronized void openWriteFiles() throws IllegalArgumentException {
			if (fileDir == null) {
				throw new IllegalArgumentException("文件路径不能为null");
			}
			getLastFile();
			setTimer();
		}

		public void close() {
			super.close();
			if (lastFile != null) {
				if (new File(lastFile).length() == 0) {
					new File(lastFile).delete();
				}
			}
		}

		/**
		 * 打开需要写入的文件
		 * 
		 * @param file
		 *            需要打开的文件
		 * @param append
		 *            是否将内容添加到文件末尾
		 */
		private void openFile(File file, boolean append) throws Exception {
			FileOutputStream fout = new FileOutputStream(file.toString(), append);
			setOutputStream(fout);
		}

		/**
		 * 将离现在最近的文件作为写入文件的文件 例如 D:logmylog_30_2008-02-19.log
		 * mylog表示自定义的日志文件名，2008-02-19表示日志文件的生成日期，30 表示此日期的第30个日志文件
		 */
		private void getLastFile() {
			try {
				close();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
				String trace = sdf.format(new Date(System.currentTimeMillis()));
				lastFile = fileDir + "/" + filePrefix + ".log." + trace;
				File file = new File(fileDir, filePrefix + ".log." + trace);
				openFile(file, true);
				LogUtils.debug("Changed CollectLog file.");
			} catch (Exception ex) {
				LogUtils.error("Get CollectLog file failed.", ex);
			}
		}

		private void setTimer() {
			Date date = new Date(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(Calendar.HOUR_OF_DAY, 1);// 一小时一个文件
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MILLISECOND, 0);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			LogUtils.debug("Next time:" + sdf.format(calendar.getTime()));
			timer.schedule(new TimerTask() {
				public void run() {
					openWriteFiles();
				}
			}, calendar.getTime());
		}

		/**
		 * 发布日志信息
		 */
		public synchronized void publish(LogRecord record) {
			lastTime = record.getMillis();
			super.publish(record);
			super.flush();
		}

		public long getLastTime() {
			return lastTime;
		}
	}

	static class LogFormatter extends Formatter {
		@Override
		public String format(LogRecord record) {
			return record.getMessage();
		}
	}

	public static void main(String[] args) {
		final MsgPO msgPO = new MsgPO();
		msgPO.setMSG_ID(12);
		final MsgExtPO extPo = new MsgExtPO();
		msgPO.getExtPO().add(extPo);
		extPo.setLOG_LEVEL(1);
		final Random random = new Random();
		Thread th = new Thread() {
			public void run() {
				for (int i = 0;; i++) {
					List<Object> obj = new ArrayList<Object>(4);
					obj.add(random.nextDouble() * i);
					byte[] bytes = new byte[i % 100 + 1];
					random.nextBytes(bytes);
					obj.add(bytes);
					obj.add(random.nextInt(i + 1));
					obj.add("很好很强大");
					switch (i % 4) {
					case 0:
						filterLog(msgPO, extPo, obj.toArray(), i);
						break;
					case 1:
						distinctLog(msgPO, extPo, obj.toArray(), i);
						break;
					case 2:
					case 3:
						collectLog(msgPO, obj, i);
					}
				}
			}
		};
		th.start();
		Utils.sleep(1000000000);
	}

}
