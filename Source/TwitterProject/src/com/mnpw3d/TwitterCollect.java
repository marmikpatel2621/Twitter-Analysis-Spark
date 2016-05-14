package com.mnpw3d;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class TwitterCollect {
	static BufferedWriter bw;
	static FileWriter fw;

	public static void main(String[] args) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setJSONStoreEnabled(true);
		cb.setOAuthConsumerKey("HKq7xxmTzIHQURiJCTq2F7S63");
		cb.setOAuthConsumerSecret("esGkEi4KG9w5TY9J0s1oDKhSAGXFOn4I4iI9tVPBzAdD7K3HRZ");
		cb.setOAuthAccessToken("796595630-EHolo0o8su2CIATVvo63MIrtHLi7njoNo5XDgSHp");
		cb.setOAuthAccessTokenSecret("urBbRVEa55IrcA3QWxNsArvFqCiVuHtE8UknE7qmK3bWU");
		File txtTwitter = new File("Twitter2.txt");
		if (!txtTwitter.exists()) {
			try {
				txtTwitter.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			fw = new FileWriter(txtTwitter,true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		StatusListener listener = new StatusListener() {
			long count;

			public void onException(Exception arg0) {
				// TODO Auto-generated method stub

			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub

			}

			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub

			}

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

			public void onStatus(Status status) {
				// TODO Auto-generated method stub

				if (status.getGeoLocation() != null) {
					String jsonTweet = TwitterObjectFactory.getRawJSON(status);
					try {
						System.out.println(count++ + "\n");
						
							bw.append(jsonTweet + "\n");
							//bw.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			}

			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub

			}

		};
		FilterQuery fq = new FilterQuery();

		String keywords[] = { "food", "health", "Vegan", "Non Veg", "nonveg", "foodlover", "foodholic", "Chicken",
				"Mutton", "fish", "beef", "salads", "juices", "Fruits", "street", "street food", "streetfood", "gym",
				"yoga", "swimming", "zumba", "indian", "italian", "mexican", "chinese", "thai", "american", "japanese",
				"aloo chat", "aloochat", "momos", "samosas", "vada pav", "vadapau", "dosa", "pani puri", "bhel puri",
				"panipuri", "bhelpuri", "gym diet", "gymdiet", "diet", "calories", "protein", "excercise"};

		fq.track(keywords);

		twitterStream.addListener(listener);
		twitterStream.filter(fq);

	}
}