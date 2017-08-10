package com.ulb.code.wit.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import java.nio.file.Files;
import java.util.HashSet;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterTest {

	public static void main(String[] args) throws TwitterException, IOException {
		StringBuilder sb = new StringBuilder();
		Writer writer = new BufferedWriter(
				new OutputStreamWriter(
						new FileOutputStream(
								"D:\\dataset\\backup\\NorthKorea.csv"),
						"utf-8"));
		Writer textWriter = new BufferedWriter(
				new OutputStreamWriter(
						new FileOutputStream(
								"D:\\dataset\\backup\\NorthKorea_text.csv"),
						"utf-8"));
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey("8gy7V82LeXSs7GGaQkksm7SXz")
				.setOAuthConsumerSecret(
						"uGx5r667vbz3wzB2OFYkFYTY2b4Uohz3qvZpAWDeVV9YxzCohr")
				.setOAuthAccessToken(
						"1331303455-noLWMzix3tGUvk8Mglyy0WnO3zyZVjVX92RGdd6")
				.setOAuthAccessTokenSecret(
						"t4h68qVbf7jAADUtGrl1rclv2JDAxvaMVVEm90Py0f2nR");
		// TwitterFactory tf = new TwitterFactory(cb.build());
		// Twitter twitter = tf.getInstance();
		// Charset charset = Charset.forName("US-ASCII");
//		String file = "D:\\dataset\\backup\\twitter_uselection_mentionincluded_2.txt";
//		BufferedReader br = new BufferedReader(new FileReader(new File(file)));

		HashSet<Long> users = new HashSet<Long>();
		String line = null;
		String[] temp = null;
//		while ((line = br.readLine()) != null) {
//			temp = line.split(",");
//			users.add(Long.parseLong(temp[0]));
//			users.add(Long.parseLong(temp[1]));
//		}
		System.out.println(users.size());
//		br.close();
		try {
			// MyStatusListner listen = new MyStatusListner(sb);
			MyStatusListner listen = new MyStatusListner(writer, textWriter);
			TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
					.getInstance();
			FilterQuery fq = new FilterQuery();
			// String keywords[] = { "BigData", "NoSQL", "datascience",
			// "No SQL",
			// "nosql", "bigdata", "datamining", "spark", "data",
			// "analytics" };
			String keywords[] = { "#NorthKorea", "#GUAM", "#FireAndFury"};
			// String keywords[] = { "#rio2016" };
			fq.track(keywords);

			twitterStream.addListener(listen);
			twitterStream.filter(fq);
		} finally {

		}
	}
}
