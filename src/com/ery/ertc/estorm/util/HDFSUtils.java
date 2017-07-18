package com.ery.ertc.estorm.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tools.tar.TarInputStream;

import com.ery.base.support.log4j.LogUtils;

public class HDFSUtils {
	public static final Log LOG = LogFactory.getLog(HDFSUtils.class);
	public static final byte[] newLine;

	static {
		try {
			newLine = "\n".getBytes("UTF-8");// 定义换行符
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find UTF-8 encoding");
		}
	}

	/**
	 * 连接HDFS，得到指定的FileSystem
	 * 
	 * @param ip
	 *            系统IP
	 * @param port
	 *            系统端口
	 * @return 文件系统
	 */
	public static FileSystem getFileSystem(String ip, String port) {
		int iport = StringUtil.stringToInt(port, -1);
		if (-1 == iport) {
			return null;
		}
		return getFileSystem(ip, iport);
	}

	/**
	 * 连接HDFS，得到指定的FileSystem
	 * 
	 * @param ip
	 *            系统IP
	 * @param port
	 *            系统端口
	 * @return 文件系统
	 */
	public static FileSystem getFileSystem(String ip, int port) {
		String url = "hdfs://" + ip + ":" + String.valueOf(port);
		Configuration config = new Configuration();
		config.set("fs.defaultFS", url);
		try {
			return FileSystem.get(config);
		} catch (Exception e) {
			LOG.error("getFileSystem failed");
		}
		return null;
	}

	/**
	 * 得到指定的HDFS的地址
	 * 
	 * @param ip
	 *            系统IP
	 * @param port
	 *            系统端口
	 * @return 文件系统
	 */
	public static String getHDFSAddress(String ip, String ports) {
		int port = StringUtil.stringToInt(ports, -1);
		if (null == ip || ip.trim().length() <= 0) {
			return null;
		}
		if (port <= 0 || port > 65535) {
			return "hdfs://" + ip;
		} else {
			return "hdfs://" + ip + ":" + String.valueOf(port);
		}
	}

	/**
	 * 移动目录下的文件
	 * 
	 * @param conf
	 *            配置对象
	 * @param srcfilePath
	 *            源文件地址
	 * @param targetFilePath
	 *            目标文件地址
	 * @throws IOException
	 *             IO异常
	 */
	public static void mvFiles(Configuration conf, String srcfilePath, String targetFilePath) throws IOException {
		Path tempPath = new Path(targetFilePath);
		FileSystem tempfs = FileSystem.get(conf);
		if (!tempfs.exists(tempPath)) {
			tempfs.mkdirs(tempPath);
		}

		String cmd = System.getenv("HADOOP_HOME") + "/bin/hadoop fs -mv " + srcfilePath + " " + targetFilePath;
		HDFSUtils.runCommand(cmd);
	}

	/**
	 * Move files that match the file pattern <i>srcf</i> to a destination file. When moving mutiple files, the destination must
	 * be a directory. Otherwise, IOException is thrown.
	 * 
	 * @param srcf
	 *            a file pattern specifying source files
	 * @param dstf
	 *            a destination local file/directory
	 * @param dstPrefix
	 *            a prefix name of that after moved file/directory name, it can be null or empty
	 * @throws IOException
	 */
	public static void move(Configuration conf, String srcf, String dstf, String dstPrefix) throws IOException {
		Path srcPath = new Path(srcf);
		Path dstPath = new Path(dstf);
		FileSystem srcFs = srcPath.getFileSystem(conf);
		FileSystem dstFs = dstPath.getFileSystem(conf);

		Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
		boolean srcIsFile = false;

		try {
			FileStatus status = srcFs.getFileStatus(srcPath);
			if (!status.isDir())
				srcIsFile = true;
		} catch (Exception e) {
			// throw new IOException(srcPath + ": get file status error; " +
			// e.getMessage());
		}

		// 创建文件目录(若不存在)
		Path dst = new Path(dstf);
		if (!dstFs.exists(dst)) {
			dstFs.mkdirs(dst);
		} else {
			if (dstFs.isFile(dst)) {
				throw new IOException("目标目录是已经存在, 并且是文件.");
			}
		}

		if (srcs.length > 1 && !dstFs.getFileStatus(dst).isDir()) {
			throw new IOException("When moving multiple files, destination should be a directory.");
		}

		Path srcFile = null;
		Path dstFile = dst;

		for (int i = 0; i < srcs.length; i++) {
			srcFile = srcs[i];
			if (null != dstPrefix && !dstPrefix.trim().isEmpty()) {
				dstFile = srcIsFile ? new Path(dst.getParent(), dstPrefix + "-" + dst.getName()) : new Path(dst,
						dstPrefix + "-" + srcFile.getName());
			}

			if (!srcFs.rename(srcs[i], dstFile)) {
				FileStatus srcFstatus = null;
				FileStatus dstFstatus = null;
				try {
					srcFstatus = srcFs.getFileStatus(srcs[i]);
				} catch (FileNotFoundException e) {
					throw new FileNotFoundException(srcs[i] + ": No such file or directory");
				}
				try {
					dstFstatus = dstFs.getFileStatus(dst);
				} catch (IOException e) {
					throw new IOException(srcs[i] + ": get file status error;" + e.getMessage());
				}

				if ((srcFstatus != null) && (dstFstatus != null)) {
					if (srcFstatus.isDir() && !dstFstatus.isDir()) {
						throw new IOException("cannot overwrite non directory " + dst + " with directory " + srcs[i]);
					}
				}
				throw new IOException("Failed to rename " + srcs[i] + " to " + dst);
			}
		}
	}

	/**
	 * 新建临时目录
	 * 
	 * @param conf
	 *            配置对象
	 * @param path
	 *            地址
	 * @return 临时目录
	 * @throws IOException
	 *             IO异常
	 */
	public static String mkdir(Configuration conf, String path) throws IOException {
		Path tempPath = new Path(path + "_" + System.currentTimeMillis() + "_" + 0);
		FileSystem tempfs = FileSystem.get(conf);
		int i = 0;
		while (tempfs.exists(tempPath)) {
			tempPath = new Path(path + "_" + (System.currentTimeMillis()) + "_" + i);
			i++;
		}

		tempfs.mkdirs(tempPath);
		return tempPath.toUri().toString();
	}

	/**
	 * 新建未创建的临时目录
	 * 
	 * @param conf
	 *            配置对象
	 * @param path
	 *            地址
	 * @return 未创建的临时目录
	 * @throws IOException
	 */
	public static String getNoExistDir(Configuration conf, String path) throws IOException {
		Path tempPath = new Path(path + "_" + System.currentTimeMillis() + "_" + 0);
		FileSystem tempfs = FileSystem.get(conf);
		int i = 0;
		while (tempfs.exists(tempPath)) {
			tempPath = new Path(path + "_" + (System.currentTimeMillis()) + "_" + i);
			i++;
		}

		return tempPath.toUri().toString();
	}

	/**
	 * 获取文件大小
	 * 
	 * @param conf
	 * @param filePath
	 * @return
	 */
	public static long getFileSize(Configuration conf, String filePath) {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path tpath = new Path(filePath);
			if (!fs.exists(tpath)) {
				return -1;
			}

			if (fs.isFile(tpath)) {
				return fs.getFileStatus(tpath).getLen();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	/**
	 * 删除目录或文件
	 * 
	 * @param conf
	 *            配置对象
	 * @param path
	 *            地址
	 * @return true:删除成功
	 * @throws IOException
	 */
	public static boolean deleteDir(Configuration conf, String path) throws IOException {
		Path tempPath = new Path(path);
		FileSystem tempfs = FileSystem.get(conf);
		if (tempfs.exists(tempPath)) {
			return tempfs.delete(tempPath, true);
		}

		return false;
	}

	public static Map<FileAttributePO, String> directoryList(FileSystem fsystem, String[] srcPaths, Pattern pattern,
			Set<String> lstRootPath) throws IOException {
		Map<FileAttributePO, String> tempMap = new HashMap<FileAttributePO, String>();
		if (null == fsystem || null == srcPaths || srcPaths.length <= 0) {
			return tempMap;
		}

		for (String srcPath : srcPaths) {
			Path tpath = new Path(srcPath.toString());
			if (!fsystem.exists(tpath)) {
				continue;
			}

			if (fsystem.isFile(tpath)) {
				if (StringUtil.matcher(tpath.getName(), pattern) && notContains(lstRootPath, tpath, true)) {
					FileAttributePO fap = new FileAttributePO();
					fap.setPath(tpath.toUri().getPath());
					fap.setLastModifyTime(fsystem.getFileStatus(tpath).getModificationTime());
					fap.setSize(fsystem.getFileStatus(tpath).getLen());
					tempMap.put(fap, srcPath);
				}
			}

			if (fsystem.getFileStatus(tpath).isDir() && notContains(lstRootPath, tpath, true)) {
				directoryList(fsystem, tempMap, tpath, srcPath, pattern, lstRootPath);
			}
		}

		return tempMap;
	}

	private static boolean notContains(Set<String> lstRootPath, Path tpath, boolean isSelf) {
		return notContains(lstRootPath, tpath.toUri().getPath(), isSelf);
	}

	private static boolean notContains(Set<String> lstRootPath, String path, boolean isSelf) {
		if (null == lstRootPath) {
			return true;
		}

		if (isSelf) {
			return true;
		}

		return !lstRootPath.contains(path);
	}

	private static boolean notContains(Set<String> lstRootPath, FileStatus fileStatus, boolean isSelf) {
		return notContains(lstRootPath, fileStatus.getPath().toUri().getPath(), isSelf);
	}

	private static void directoryList(FileSystem fsystem, Map<FileAttributePO, String> tempMap, Path path,
			String srcPath, Pattern pattern, Set<String> lstRootPath) throws IOException {
		FileStatus fstatus[] = fsystem.listStatus(path);
		for (FileStatus fileStatus : fstatus) {
			if (fileStatus.isDir() && notContains(lstRootPath, fileStatus, false)) {
				directoryList(fsystem, tempMap, fileStatus.getPath(), srcPath, pattern, lstRootPath);
			} else {
				if (StringUtil.matcher(fileStatus.getPath().getName(), pattern) &&
						notContains(lstRootPath, fileStatus, false)) {
					FileAttributePO fap = new FileAttributePO();
					fap.setPath(fileStatus.getPath().toUri().getPath());
					fap.setLastModifyTime(fileStatus.getModificationTime());
					fap.setSize(fileStatus.getLen());
					tempMap.put(fap, srcPath);
				}
			}
		}
	}

	/**
	 * 运行命令
	 * 
	 * @param cmd
	 *            命令拆分后的数组
	 * @throws IOException
	 *             IO异常
	 */
	public static String runCommand(String cmd) throws IOException {
		Process ps = Runtime.getRuntime().exec(cmd);
		BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
		StringBuffer sb = new StringBuffer();
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line).append("\n");
		}

		return sb.toString();
	}

	/**
	 * 获取过滤数据的文件名称
	 * 
	 * @param fileName
	 * @return
	 */
	public static String getFilterFileName(String taskId, String path) {
		if (path == null) {
			return taskId + ".err";
		}

		int index = path.lastIndexOf("/");
		if (index + 1 <= path.length()) {
			String name = path.substring(index + 1, path.length());
			return name + "_" + taskId + ".err";
		} else {
			return taskId + ".err";
		}
	}

	public static void write(FSDataOutputStream fsos, String value) {
		if (fsos == null || value == null) {
			return;
		}
		try {
			fsos.writeUTF(value);
			fsos.write(newLine);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取过滤输出流
	 * 
	 * @param conf
	 */
	public static FSDataOutputStream getFileOStream(Configuration conf, String outPath) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(outPath);
			if (fs.exists(path)) {
				return fs.append(new Path(outPath), 4096);
			} else {
				return fs.create(path, false, 4096);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * 获取过滤输出流
	 * 
	 * @param conf
	 */
	public static synchronized FSDataOutputStream getFileOStream(Configuration conf, String outPath, int bufferSize) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(outPath);
			if (fs.exists(path)) {
				return fs.append(path, bufferSize);
			} else {
				return fs.create(path, false, bufferSize);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * 获取文件压缩类型
	 * 
	 * @param compresseCodec
	 *            压缩标识
	 * @param job
	 *            JobConf
	 * @return 文件压缩类型
	 */
	public static CompressionCodec getCompressCodec(String compresseCodec, Configuration job) {
		if (CompressCodec.DefaultCodec.toString().equals(compresseCodec)) {
			return ReflectionUtils.newInstance(DefaultCodec.class, job);
		} else if (CompressCodec.BZip2Codec.toString().equals(compresseCodec)) {
			return ReflectionUtils.newInstance(BZip2Codec.class, job);
		} else if (CompressCodec.GzipCodec.toString().equals(compresseCodec)) {
			return ReflectionUtils.newInstance(GzipCodec.class, job);

		} else if (CompressCodec.SnappyCodec.toString().equals(compresseCodec)) {
			return ReflectionUtils.newInstance(SnappyCodec.class, job);
		}

		return ReflectionUtils.newInstance(DefaultCodec.class, job);
	}

	/**
	 * 判断文件压缩类型
	 * 
	 * @param compresseCodec
	 *            压缩标识
	 * @return 文件压缩类型
	 */
	public static boolean isExistCompressCodec(String compresseCodec) {
		if (null == compresseCodec || compresseCodec.trim().length() <= 0) {
			return false;
		}

		if (CompressCodec.DefaultCodec.name().equals(compresseCodec) ||
				CompressCodec.BZip2Codec.name().equals(compresseCodec) ||
				CompressCodec.GzipCodec.name().equals(compresseCodec) ||
				CompressCodec.SnappyCodec.name().equals(compresseCodec)) {
			return true;
		}

		return false;
	}

	/**
	 * 判断文件压缩类型是否为BZip2
	 * 
	 * @param compresseCodec
	 *            压缩标识
	 * @return 文件压缩类型
	 */
	public static boolean isBZip2CompressCodec(String compresseCodec) {
		if (null == compresseCodec || compresseCodec.trim().length() <= 0) {
			return false;
		}

		if (CompressCodec.BZip2Codec.name().equals(compresseCodec)) {
			return true;
		}

		return false;
	}

	/**
	 * 获取输出压缩流
	 * 
	 * @param outFilePath
	 * @param outSystem
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static OutputStream getCompressOutputStream(String outFilePath, Object outSystem, int compress)
			throws IOException, ClassNotFoundException {
		if (outFilePath == null) {
			return null;
		}
		if (compress == 0) {
			LogUtils.info("获取普通流");
			return ((FileSystem) outSystem).create(new Path(outFilePath));
		} else if (compress == 1) {
			LogUtils.info("获取GZ压缩流");
			if (!outFilePath.endsWith(".gz")) {
				outFilePath += ".gz";
			}
			OutputStream tmpos = ((FileSystem) outSystem).create(new Path(outFilePath));
			Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
			CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					((FileSystem) outSystem).getConf());
			return codec.createOutputStream(tmpos);
		}
		return null;
	}

	/**
	 * 获取输出压缩流
	 * 
	 * @param outFilePath
	 * @param outSystem
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static OutputStream getCompressOutputStream(OutputStream os, int compress) throws IOException,
			ClassNotFoundException {
		if (os == null) {
			return null;
		}

		if (compress == 1) {
			LogUtils.info("获取GZ压缩流");
			return new GZIPOutputStream(os);
		}

		LogUtils.info("获取普通流");
		return os;
	}

	/**
	 * 文件压缩标识符
	 * 
	 * @author wanghao
	 * @version v1.0
	 * @create Data 2013-1-14
	 */
	public enum CompressCodec {
		DefaultCodec, BZip2Codec, GzipCodec, SnappyCodec;
	}

	/**
	 * 文件类型
	 * 
	 * @author wanghao
	 * @version v1.0
	 * @create Data 2013-1-14
	 */
	public enum FileType {
		TEXTFILE, SEQUENCEFILE, RCFILE
	}

	/**
	 * 获取hdfs的解压缩后的字节流 1、非压缩文件 2、单个文件压缩（或单个文件打包，单个文件打包压缩）(Gzip,snnapy,bzip2,Default)
	 * 3、多个文件压缩（或多个文件打包压缩）(Gzip,snnapy,bzip2,Default)
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static InputStream getHDFSInputStream(Path file) throws IOException {
		Configuration job = new Configuration();
		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
		CompressionCodec codec = compressionCodecs.getCodec(file);
		FileSystem fs = file.getFileSystem(job);
		FileStatus filestatus = fs.getFileStatus(file);
		FSDataInputStream fileIn = fs.open(file);
		long length = filestatus.getLen();
		if (null != codec) {
			Decompressor decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(fileIn,
						decompressor, 0, length, SplittableCompressionCodec.READ_MODE.BYBLOCK);
				String filename = file.getName();
				if (filename.endsWith(".tar.gz")) {
					return new TarInputStream(cIn);
				} else {
					return cIn;
				}
			} else {
				String filename = file.getName();
				if (filename.endsWith(".tar.gz")) {
					return new TarInputStream(codec.createInputStream(fileIn, decompressor));
				} else {
					return codec.createInputStream(fileIn, decompressor);
				}
			}
		} else {
			return fileIn;
		}
	}
}
