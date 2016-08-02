package com.ery.ertc.collect.ui.po;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ery.base.support.utils.Convert;

public class HostTotal {

	public long collectSize;
	public long collectNum;
	public long collectError;
	public long filterNum;
	public long distinctNum;
	public long sendSize;
	public long sendNum;
	public long sendError;
	public long leaveSize;
	public long collectTps;
	public long sendTps;
	public List<String> rels = new ArrayList<String>();
	public String startTime;

	public void minStartTime(String st) {
		if (startTime == null) {
			startTime = st;
		} else {
			Date old = Convert.toTime(startTime, Convert.DATE_DEFAULT_FMT);
			Date t = Convert.toTime(st, Convert.DATE_DEFAULT_FMT);
			if (t.getTime() < old.getTime())
				startTime = st;
		}
	}

}
