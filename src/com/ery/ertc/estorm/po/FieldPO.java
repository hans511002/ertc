package com.ery.ertc.estorm.po;

import java.io.Serializable;
import java.text.SimpleDateFormat;

public abstract class FieldPO implements Serializable {

	private long FIELD_ID;// 字段ID
	private String FIELD_NAME;// 字段名称（同一输入或输出集内名称唯一）
	/**
	 * 字段类型 VARCHAR --字符串 NUMBER --整数（包括负数） NUMBER(2) --小数，括号中的值标示小数位数
	 * TIME_STR(yyyyMMddHHmmss) 时间字符串，括号里面标示字符格式，无括号时，默认格式即此示例 TIME_LONG
	 * 时间毫秒，展示或存储时需要转换 BLOB --二进制数据
	 */
	private String FIELD_DATA_TYPE;
	private String FIELD_CN_NAME;// 字段中文名
	private int ORDER_ID;
	private String FIELD_DESC;// 字段描述

	public long getFIELD_ID() {
		return FIELD_ID;
	}

	public void setFIELD_ID(long FIELD_ID) {
		this.FIELD_ID = FIELD_ID;
	}

	public String getFIELD_NAME() {
		return FIELD_NAME;
	}

	public void setFIELD_NAME(String FIELD_NAME) {
		this.FIELD_NAME = FIELD_NAME;
	}

	public String getFIELD_DATA_TYPE() {
		return FIELD_DATA_TYPE;
	}

	public void setFIELD_DATA_TYPE(String FIELD_DATA_TYPE) {
		this.FIELD_DATA_TYPE = FIELD_DATA_TYPE;
	}

	public String getFIELD_CN_NAME() {
		return FIELD_CN_NAME;
	}

	public void setFIELD_CN_NAME(String FIELD_CN_NAME) {
		this.FIELD_CN_NAME = FIELD_CN_NAME;
	}

	public int getORDER_ID() {
		return ORDER_ID;
	}

	public void setORDER_ID(int ORDER_ID) {
		this.ORDER_ID = ORDER_ID;
	}

	public String getFIELD_DESC() {
		return FIELD_DESC;
	}

	public void setFIELD_DESC(String FIELD_DESC) {
		this.FIELD_DESC = FIELD_DESC;
	}

	/**
	 * 下面的属性是方便业务计算，处理过的一些变量
	 */
	private byte type;// 1字符，2数字，4小数，8时间long，16二进制
	// private String dateFormat;// 日期转换格式
	private SimpleDateFormat _dateFormat;// 日期转换格式

	public byte getType() {
		if (type == 0) {
			if (FIELD_DATA_TYPE.equals("NUMBER")) {
				type = 2;
			} else if (FIELD_DATA_TYPE.startsWith("NUMBER")) {
				type = 4;
			} else if (FIELD_DATA_TYPE.equals("DATE") || FIELD_DATA_TYPE.equals("TIME_LONG") ||
					FIELD_DATA_TYPE.startsWith("TIME_STR")) {
				type = 8;
			} else if (FIELD_DATA_TYPE.equals("BLOB")) {
				type = 16;
			} else {
				type = 1;
			}
		}
		return type;
	}

	// public String getDateFormat() {
	// if (dateFormat == null) {
	// dateFormat = "";
	// if (FIELD_DATA_TYPE.equals("TIME_STR")) {
	// dateFormat = "yyyyMMddHHmmss";
	// } else if (FIELD_DATA_TYPE.startsWith("TIME_STR")) {
	// dateFormat = FIELD_DATA_TYPE.substring(9, FIELD_DATA_TYPE.length() - 1);
	// }
	// }
	// return dateFormat;
	// }

	public SimpleDateFormat getSimpleDateFormat() {
		if (_dateFormat == null) {
			_dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			if (FIELD_DATA_TYPE.equals("TIME_STR")) {
				_dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
			} else if (FIELD_DATA_TYPE.startsWith("TIME_STR")) {
				_dateFormat = new SimpleDateFormat(FIELD_DATA_TYPE.substring(9, FIELD_DATA_TYPE.length() - 1));
			}
		}
		return _dateFormat;
	}
}
