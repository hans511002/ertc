package com.ery.ertc.estorm.po;

public class CollectSendErrorLogPO extends LogPO {

	long STORE_ID;

	private String MSG_CONTENT;
	private String SEND_TIME;
	private String ERROR_DATA;

	public long getSTORE_ID() {
		return STORE_ID;
	}

	public void setSTORE_ID(long sTORE_ID) {
		STORE_ID = sTORE_ID;
	}

	public String getMSG_CONTENT() {
		return MSG_CONTENT;
	}

	public void setMSG_CONTENT(String MSG_CONTENT) {
		this.MSG_CONTENT = MSG_CONTENT;
	}

	public String getSEND_TIME() {
		return SEND_TIME;
	}

	public void setSEND_TIME(String SEND_TIME) {
		this.SEND_TIME = SEND_TIME;
	}

	public String getERROR_DATA() {
		return ERROR_DATA;
	}

	public void setERROR_DATA(String ERROR_DATA) {
		this.ERROR_DATA = ERROR_DATA;
	}
}
