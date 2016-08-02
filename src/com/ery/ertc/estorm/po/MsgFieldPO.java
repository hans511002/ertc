package com.ery.ertc.estorm.po;

public class MsgFieldPO extends FieldPO {

	public final static String MAC_MSG_ID = "MSG_ID";// 监控ID
	public final static String MAC_HOST = "HOST";// 采集机器的host
	public final static String MAC_COLLECT_TIME = "COLLECT_TIME";// 采集时间,毫秒数

	private long MSG_ID;//

	/**
	 * <pre>
	 * 来源字段，定义了字段值取自消息的规则。
	 * 如果，整个消息数据类型是jsonarray，jsonmap。src数组的索引或map的key
	 * 如果整个消息数据类型是text。那么取数规则则很多样化，默认会将数据按某个字符拆分后形成数组
	 * 取数规则一：来自拆分后的数组，一个或多个，或进行二次拆分提取
	 *      0       ——拆分数组第一个值
	 *      1-2     ——拆分数组第二个和第三个值按拆分符连接后的值
	 *      3-?     ——拆分数组第四个值起与其他后面的值连接后的值
	 *      idx:0   ——可在此元素后加上“idx:”方面识别，也可不加
	 *      idx:0,split:1:spch  ——将拆分数组第一个元素按“spch”二次拆分后取第一个元素
	 *      idx:1-3,split:1:spch——将拆分数组第二到四连接后按“spch”二次拆分后取第一个元素
	 *      idx:0,substr_s:ch   ——将第一个元素按ch截断，取前半部分
	 *      idx:0,substr_e:ch   ——将第一个元素按ch截断，取后半部分
	 * 取数规则二：来自系统宏变量
	 * 取数规则三：正则表达式匹配
	 * </pre>
	 */
	private String SRC_FIELD;// 源字段

	public long getMSG_ID() {
		return MSG_ID;
	}

	public void setMSG_ID(long MSG_ID) {
		this.MSG_ID = MSG_ID;
	}

	public String getSRC_FIELD() {
		return SRC_FIELD;
	}

	public void setSRC_FIELD(String SRC_FIELD) {
		this.SRC_FIELD = SRC_FIELD;
	}
}
