package com.ulb.code.wit.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterTest {

	public static void main(String[] args) throws TwitterException, IOException {
		StringBuilder sb = new StringBuilder();
		Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("D:\\dataset\\twitterEuro2016.csv"), "utf-8"));
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey("kAqpUreGsJ9pwISo0QzLKFanw")
				.setOAuthConsumerSecret(
						"88vzRhhosuGeOMvKQNKzbhBI0cuTSarSdTZygYbW5uTGckzsbC")
				.setOAuthAccessToken(
						"1331303455-noLWMzix3tGUvk8Mglyy0WnO3zyZVjVX92RGdd6")
				.setOAuthAccessTokenSecret(
						"t4h68qVbf7jAADUtGrl1rclv2JDAxvaMVVEm90Py0f2nR");
		// TwitterFactory tf = new TwitterFactory(cb.build());
		// Twitter twitter = tf.getInstance();
		try {
			// MyStatusListner listen = new MyStatusListner(sb);
			MyStatusListner listen = new MyStatusListner(writer);
			TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
					.getInstance();
			FilterQuery fq = new FilterQuery();
			String keywords[] = { "Euro2016","#Euro2016" };

			fq.track(keywords);

			twitterStream.addListener(listen);
			twitterStream.filter(fq);
		} finally {
			
		}
	}
}
