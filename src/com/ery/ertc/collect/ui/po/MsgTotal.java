package com.ery.ertc.collect.ui.po;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ery.base.support.utils.Convert;

public class MsgTotal {
	public long collectSize;
	public long collectNum;
	public long collectError;
	public Map<Long, Long> filterNum = new HashMap<Long, Long>();
	public Map<Long, Long> distinctNum = new HashMap<Long, Long>();
	public Map<Long, Long> sendSize = new HashMap<Long, Long>();
	public Map<Long, Long> sendNum = new HashMap<Long, Long>();
	public Map<Long, Long> sendError = new HashMap<Long, Long>();
	public Map<Long, Long> leaveSize = new HashMap<Long, Long>();

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
