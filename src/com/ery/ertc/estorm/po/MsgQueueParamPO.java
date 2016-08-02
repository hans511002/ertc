package com.ery.ertc.estorm.po;

import java.io.Serializable;

public class MsgQueueParamPO implements Serializable {

	private long PARAM_ID;// 参数ID
	private long MSG_ID;// 消息ID
	private int PART_NUM;// 分区数
	/**
	 * 分区规则,格式：【typeid:规则】 【0】 ——0或空标示使用默认分区
	 * 【1:fileName】——使用字段的值来分区取模,（fieldName只能是一个字段）
	 * 【2:fileName】——使用字段的值的hashCode来分区取模
	 * ,（fieldName可以是多个字段用逗号分割，取值时先将值相加再hashCode）
	 */
	private String PART_RULE;
	private int PART_REPLICA_NUM;// 分区备份数
	private int SEND_TYPE;// 发送类型0同步，1异步
	/**
	 * ##此值控制可用性级别，不同级别发送性能不同，可配三种级别（0,1，-1）。系统默认为0 ##
	 * 0：服务端领导者接受到数据后立即返回客户端标示（此时未完全持久化到磁盘，节点死了会造成部分数据丢失） ##
	 * 1：服务领导者完全持久化到磁盘后返回客户端标示（备份者未完全备份，尚未备份的数据可能丢失） ##
	 * -1：服务端等待所有备份完全同步后返回客户端标示（完全备份，只要保证服务端任意一个isr存活，则数据不会丢失）
	 * ##三种级别性能梯度：10->5>1 ##————————如果ISR列表全挂，客户端将收到异常，客户端设立失败重发机制
	 */
	private int SEND_ACK_LEVEL;// 发送应答级别【0,1,-1】
	private int SEND_BATCH_NUM;// 发送批次大小(批次大小）
	private int COMPRESSION_TYPE;// 压缩类型0不压缩，1gzip，2snappy——一般用于异步发送

}
