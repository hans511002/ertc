package com.ery.ertc.estorm.util;

import java.text.ParseException;

public class FileAttributePO {
	private String path;
	private long size;
	private String lastModifyTime;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public String getLastModifyTime() {
		return this.lastModifyTime;
	}

	public void setLastModifyTime(long lastModifyTime) {
		try {
			this.lastModifyTime = StringUtils.longToString(lastModifyTime, StringUtils.DATE_FORMAT_TYPE1);
		} catch (ParseException e) {
			e.printStackTrace();
			this.lastModifyTime = "-1";
		}
	}

	@Override
	public String toString() {
		return "path=" + path + "  ,size=" + size + " ,lastModifyTime=" + lastModifyTime;
	}
}
