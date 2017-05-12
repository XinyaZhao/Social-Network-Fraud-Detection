import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.io.*;

public class GetUserAndFollowerList {

    public static final String PATH = "/Users/nan/Desktop/followerfolloweelist.txt";
    public static final ConfigurationBuilder CB = new ConfigurationBuilder();

    public static void main(String[] args) throws TwitterException, IOException {
        CB.setDebugEnabled(true)
                .setOAuthConsumerKey("6op0sbZdywLgp3YLufCesLn0Y")
                .setOAuthConsumerSecret("csK9e6BnUE2jYKK4SpyXO6tNbDbsQqm9N6WpH50N68kEoHZP1a")
                .setOAuthAccessToken("767711405231501312-saVTL4bJksMMrKdAcgdZFVjNy6iOOaU")
                .setOAuthAccessTokenSecret("HUmngroubrnkS9zAvRqSusR38kuugNZzlhIFtPDoB2Avb");

        File outputfile = new File(PATH);
        try {
            outputfile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        TwitterStream twitterStream = new TwitterStreamFactory(CB.build()).getInstance();

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                long userID = status.getUser().getId();

                ConfigurationBuilder cbf = new ConfigurationBuilder();

                cbf.setDebugEnabled(true)
                        .setOAuthConsumerKey("6op0sbZdywLgp3YLufCesLn0Y")
                        .setOAuthConsumerSecret("csK9e6BnUE2jYKK4SpyXO6tNbDbsQqm9N6WpH50N68kEoHZP1a")
                        .setOAuthAccessToken("767711405231501312-saVTL4bJksMMrKdAcgdZFVjNy6iOOaU")
                        .setOAuthAccessTokenSecret("HUmngroubrnkS9zAvRqSusR38kuugNZzlhIFtPDoB2Avb");

                Twitter twitter = new TwitterFactory(cbf.build()).getInstance();

                try {
                    long cursor = -1;
                    //int count = 5000;
                    int i = 0;

                    do {
                        IDs followers = twitter.getFollowersIDs(userID, cursor);
                        for (long follower : followers.getIDs()) {
                            String userandfollower = follower + " " + userID + "\n";
                            try {
                                FileWriter fwrite = new FileWriter(PATH, true);
                                BufferedWriter bwrite = new BufferedWriter(fwrite);
                                bwrite.append(userandfollower);
                                bwrite.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            i++;
                        }
                        cursor = followers.getNextCursor();

                        System.out.println(i);

                            try {
                                Thread.sleep(100000);    //About 2 minutes, 1000 milliseconds is one second.
                            } catch (InterruptedException ex) {
                                Thread.currentThread().interrupt();
                            }

                    } while (cursor != 0);
                } catch (TwitterException te) {
                    te.printStackTrace();
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {}

            @Override
            public void onTrackLimitationNotice(int arg0) {}

            @Override
            public void onException(Exception ex) {ex.printStackTrace();}

            @Override
            public void onScrubGeo(long arg0, long arg1) {}

            @Override
            public void onStallWarning(StallWarning arg0) {}
        };

        twitterStream.addListener(listener);

        twitterStream.sample();
    }
}
