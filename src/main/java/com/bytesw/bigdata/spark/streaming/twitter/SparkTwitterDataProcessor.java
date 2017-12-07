package com.bytesw.bigdata.spark.streaming.twitter;

//import java.util.logging.Level;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
//import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;


/**
 *
 * @author sangeles
 * @version 0.0.1
 * 
 * Consume data proveniente de Twitter e imprime la cantidad de tweets por usuario en el momento actual (10 segundos)
 */
public class SparkTwitterDataProcessor {
    public static void main(String[] args) {
        // Prepare the spark configuration by setting application name and master node "local" i.e. embedded mode
        final SparkConf sparkConf = new SparkConf().setAppName("SparkTwitterDataProcessor").setMaster("local[2]");
//        final SparkConf sparkConf = new SparkConf().setAppName("SparkTwitterDataProcessor");
        // Create Streaming context using spark configuration and duration for which messages will be batched and fed to Spark Core
        final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Duration.apply(10000));

        Logger.getRootLogger().setLevel(Level.WARN);
        
        // Prepare configuration for Twitter authentication and authorization
        final Configuration conf = new ConfigurationBuilder().setDebugEnabled(false)
                .setOAuthConsumerKey("WNYbNfoKvE12Gc8178zpVuzWp")
                .setOAuthConsumerSecret("sGZ8jFkWZxYsnPIDloYjo3gNfPmsadA5HQjEhRO2Rmy6kITj8I")
                .setOAuthAccessToken("217221545-RGl2qvVsYtcPetOTcoS8Q71VK0yxWUI2mjufcv7p")
                .setOAuthAccessTokenSecret("svbYOQYNBv4Hw3SAbOJcvBEMEkRpGSJjzHBg83Sq3CsUp")
                .build();
        // Create Twitter authorization object by passing prepared configuration containing consumer and access keys and tokens
        final Authorization twitterAuth = new OAuthAuthorization(conf);
        
        String[] filters={"#peru, #Entel, #NED2017, #influencers, @2010MisterChip"};
        
        // Create a data stream using streaming context and Twitter authorization
//        final JavaReceiverInputDStream<Status> inputDStream = TwitterUtils.createStream(streamingContext, twitterAuth, new String[]{});
        final JavaReceiverInputDStream<Status> inputDStream = TwitterUtils.createStream(streamingContext, twitterAuth, filters);
        // Create a new stream by filtering the non english tweets from earlier streams
//        final JavaDStream<Status> enTweetsDStream = inputDStream.filter((status) -> ("es".equalsIgnoreCase(status.getLang()) && status.getText().contains("entel")));
        final JavaDStream<Status> enTweetsDStream = inputDStream.filter((status) -> ("es".equalsIgnoreCase(status.getLang())));
        // Convert stream to pair stream with key as user screen name and value as tweet text
        final JavaPairDStream<String, String> userTweetsStream
                = enTweetsDStream.mapToPair(
//                        (status) -> new Tuple2<String, String>(status.getUser().getScreenName(),"Fecha:"+status.getCreatedAt().toString()+"_"+status.getText())
                        (status) -> new Tuple2<String, String>(status.getUser().getScreenName(),status.getText())
                );

        // Group the tweets for each user
        final JavaPairDStream<String, Iterable<String>> tweetsReducedByUser = userTweetsStream.groupByKey();
        // Create a new pair stream by replacing iterable of tweets in older pair stream to number of tweets
//        final JavaPairDStream<String, Integer> tweetsMappedByUser = tweetsReducedByUser.mapToPair(
//                userTweets -> new Tuple2<String, Integer>(userTweets._1, Iterables.size(userTweets._2))
//        );
        final JavaPairDStream<String, Iterable<String>> tweetsMappedByUser = tweetsReducedByUser.mapToPair(
                userTweets -> new Tuple2<String, Iterable<String>>(userTweets._1, userTweets._2)
        );
        // Iterate over the stream's RDDs and print each element on console
        tweetsMappedByUser.foreachRDD((VoidFunction<JavaPairRDD<String, Iterable<String>>>) pairRDD -> {
            pairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {

                @Override
                public void call(Tuple2<String, Iterable<String>> t) throws Exception {
                    System.out.println(t._1() + "," + t._2());
                }

            });
        });
        // Triggers the start of processing. Nothing happens if streaming context is not started
        streamingContext.start();
        try {
            // Keeps the processing live by halting here unless terminated manually
            streamingContext.awaitTermination();
        } catch (InterruptedException ex) {
            Logger.getLogger(SparkTwitterDataProcessor.class.getName()).log(null, null, ex);
        }

    }
}
