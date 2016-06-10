package com.ulb.code.wit.main;

import java.io.IOException;
import java.io.Writer;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class MyStatusListner implements StatusListener {

	StringBuilder sb;
	Writer wr;
	int count = 0;

	public MyStatusListner(StringBuilder sb) {
		this.sb = sb;
	}

	public MyStatusListner(Writer wr) {
		this.wr = wr;
	}

	@Override
	public void onException(Exception arg0) {
		// TODO Auto-generated method stub
		arg0.printStackTrace();
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onScrubGeo(long userId, long upToStatusId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onStallWarning(StallWarning warning) {
		// TODO Auto-generated method stub
		System.out.println("onstallwarning" + warning.getMessage());
	}

	@Override
	public void onStatus(Status status) {
		// TODO Auto-generated method stub
		Status temp = status.getRetweetedStatus();
		if (temp != null) {
			String line = (temp.getUser().getId() + ","
					+ status.getUser().getId() + ","
					+ status.getCreatedAt().getTime() + "\n");
			System.out.print(line);
			count++;
			if (sb != null) {
				sb.append(line);
			} else {
				try {
					wr.write(line);
					if (count % 100 == 0)
						wr.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		// TODO Auto-generated method stub
		System.out.println("onTrackLimitationNOtice: "
				+ numberOfLimitedStatuses);

	}

}
