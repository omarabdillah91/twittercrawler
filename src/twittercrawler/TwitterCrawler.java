/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twittercrawler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author omaabdillah
 */
public class TwitterCrawler {

    final static String CONSUMER_KEY = "oZ9RgLLFx70drTdMXpgmG3obw";
    final static String CONSUMER_SECRET = "1yEXJY1wmy7HiE6uJCYQZyp1CAJxgrKrIK4acxgmXd8JnM3piV";
    final static String ACCESS_KEY = "66830155-U4MmXd7JcSEvFYS7XsImD41g6X5fUZZijCSHPR8gv";
    final static String ACCESS_SECRET = "uRZakup5v3LpHAahSG6Ivkp4GwQObSXgtdpiYtGYZrJtb";
    final static String SEARCH_KEY = "SEARCH";
    final static String STREAMING_KEY = "STREAMING";
    private static String PATH = "";
    private static final String COMMA_DELIMITER = "\",\"";
    private static final String NEW_LINE_SEPARATOR = "\"\n";
    private static final String FILE_HEADER = "\"id\",\"username\",\"date\",\"status";
    private static ConfigurationBuilder cb;
    private static TwitterFactory tf;
    private static Twitter twitter;
    FileWriter fileWriter;
    static TwitterCrawler crawler = new TwitterCrawler();
    static int request = 0;

    /**
     * Usage: java twitter4j.examples.search.SearchTweets [query]
     *
     * @param args search query
     */
    public static void main(String[] args) {
        String topic = "EXAMPLE";
        String keyword = "ahy";
        String action = "SEARCH";
        Path currentRelativePath = Paths.get("");
        PATH = currentRelativePath.toAbsolutePath().toString() + "/" + topic;
        System.out.println(PATH);
        File directory = new File(PATH);
        if (!directory.exists()) {
            directory.mkdir();
        }
        //Twitter Conf.
        cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey(CONSUMER_KEY);
        cb.setOAuthConsumerSecret(CONSUMER_SECRET);
        cb.setOAuthAccessToken(ACCESS_KEY);
        cb.setOAuthAccessTokenSecret(ACCESS_SECRET);
        tf = new TwitterFactory(cb.build());
        twitter = tf.getInstance();
        if (action.equalsIgnoreCase(SEARCH_KEY)) {
            crawler.doSearch(keyword);
        } else if (action.equalsIgnoreCase(STREAMING_KEY)) {
            crawler.streaming(keyword, topic);
        }

    }

    public void doSearch(String keyword) {
        String keys[] = keyword.split(":");
        for (String key : keys) {
            if (request == 180) {
                try {
                    Thread.sleep(900000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                request = 0;
            }
            crawler.searching(key);
        }
    }

    private void streaming(String keyword, String topic) {
        ArrayList<Status> tweets = new ArrayList<Status>();
        int indeks = 0;
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                tweets.add(status);
                if (tweets.size() > 100000) {
                    printTweet(tweets, topic, "streaming");
                    tweets.clear();
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        FilterQuery tweetFilterQuery = new FilterQuery(); // See 
        tweetFilterQuery.track(keyword.split(":")); // OR on keywords
        twitterStream.filter(tweetFilterQuery);
    }

    private void printTweet(ArrayList<Status> tweet, String keyword, String action) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        String date = now.getYear() + "" + now.getMonthValue() + "" + now.getDayOfMonth() + "-" + now.getHour() + "" + now.getMinute() + "" + now.getSecond();
        try {
            fileWriter = new FileWriter(PATH + "/" + date + "-" + action + "-" + keyword + ".csv");
            fileWriter.append(FILE_HEADER.toString());
            fileWriter.append(NEW_LINE_SEPARATOR);
            for (Status status : tweet) {
                fileWriter.append(status.getId() + "");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(status.getUser().getScreenName().replaceAll("[,\"]", "") + "");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append((status.getCreatedAt().getYear() + 1900) + "/" + (status.getCreatedAt().getMonth() + 1) + "/" + status.getCreatedAt().getDate());
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(status.getText().replaceAll("[,\"]", "") + "");
                fileWriter.append(NEW_LINE_SEPARATOR);
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            System.out.println("Error while flushing/closing fileWriter !!!");
            e.printStackTrace();
        }
        System.out.println("CSV file was created successfully !!!");
    }

    private void searching(String keyword) {
        Query query = new Query(keyword);
        //query.setCount(10000);
        int numberOfTweets = Integer.MAX_VALUE;
        long lastID = Long.MAX_VALUE;
        query.setMaxId(lastID - 1);
        ArrayList<Status> tweets = new ArrayList<Status>();
        while (tweets.size() < numberOfTweets) {
            if (request == 180) {
                System.out.println(lastID);
                printTweet(tweets, keyword, "search");
                tweets.clear();
                request = 0;
                try {
                    Thread.sleep(900000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            }
            if (numberOfTweets - tweets.size() > 1000) {
                query.setCount(1000);
            } else {
                query.setCount(numberOfTweets - tweets.size());
            }
            try {
                QueryResult result = twitter.search(query);
                int pre_size = tweets.size();
                tweets.addAll(result.getTweets());
                System.out.println("Successfully gathered " + tweets.size() + " tweets");
                int post_size = tweets.size();
                if (pre_size - post_size == 0) {
                    break;
                } else {
                    for (Status t : tweets) {
                        if (t.getId() < lastID) {
                            lastID = t.getId();
                        }
                    }
                    request++;
                }
            } catch (TwitterException te) {
                System.out.println("Couldn't connect: " + te);
                if (tweets.size() > 0) {
                    printTweet(tweets, keyword, "search");
                }
                tweets.clear();
                request = 0;
                try {
                    Thread.sleep(900000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            };
            query.setMaxId(lastID - 1);
        }
        if (tweets.size() > 0) {
            printTweet(tweets, keyword, "search");
        }
        System.out.println("Have successfully crawled for " + keyword);
    }

}
