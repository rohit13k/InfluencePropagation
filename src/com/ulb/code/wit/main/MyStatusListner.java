package com.ulb.code.wit.main;

import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.UserMentionEntity;

public class MyStatusListner implements StatusListener {

	StringBuilder sb;
	Writer wr;
	Writer wrText;
	
	int count = 0;
	HashSet<Long> users = null;

	public MyStatusListner(StringBuilder sb) {
		this.sb = sb;
	}

	public MyStatusListner(Writer wr, HashSet<Long> users) {
		this.wr = wr;
		this.users = users;
	}
	public MyStatusListner(Writer wr, Writer wrText) {
		this.wr = wr;
		this.wrText = wrText;
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
		
		UserMentionEntity[] mention = status.getUserMentionEntities();
		if (false) {
			for (UserMentionEntity ume : mention) {
				//				if (users.contains(ume.getId())
				//						& users.contains(status.getUser().getId())) {
				if(true){
					String line = (ume.getId() + "," + status.getUser().getId()
							+ "," + status.getCreatedAt().getTime()/1000 + ",M, "+status.getId()+"\n");
					String text=status.getId()+","+status.getText().replaceAll("\\s", " ")+"\n";
					count++;
					if (sb != null) {
						sb.append(line);
					} else {
						try {
							wr.write(line);
							if (count % 100 == 0) {
								wr.flush();
								System.out.println(count);
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						try {
							if(wrText!=null){
								wrText.write(text);
								if (count % 100 == 0) {
									wrText.flush();
									System.out.println(count);
								}
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		} 
		if (temp != null) {
			// if (users.contains(temp.getUser().getId())
			// & users.contains(status.getUser().getId())) {

			if(status.isRetweet()){
				String line = (temp.getUser().getId() + ","
						+ status.getUser().getId() + ","
						+ status.getCreatedAt().getTime()/1000  + ",R, "+temp.getUser().getScreenName()+","+status.getUser().getScreenName()+","+status.getId()+","+status.getQuotedStatusId()+"\n");
				String text=status.getId()+","+status.getText().replaceAll("\\s", " ")+"\n";
				// System.out.print(line);
				count++;
				if (sb != null) {
					sb.append(line);
				} else {
					try {
						wr.write(line);
						if (count % 100 == 0) {
							wr.flush();
							System.out.println(count);
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						if(wrText!=null){
							wrText.write(text);
							if (count % 100 == 0) {
								wrText.flush();
								System.out.println(count);
							}
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		// TODO Auto-generated method stub
		//		System.out.println("onTrackLimitationNOtice: "
		//				+ numberOfLimitedStatuses);

	}

}
