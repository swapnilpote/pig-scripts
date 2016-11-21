/* Use following jar file to parse tweeter data */
REGISTER '/tmp/json-simple-1.1.1.jar';
REGISTER '/tmp/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/tmp/elephant-bird-pig-4.1.jar';
 

/* -nestedLoad used for loading nested data inside json file */
load_tweets = LOAD '/user/cloudera/twitDB.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS myMap;

/* extracting id, text, user nested info & entities nested info */
extract_details = FOREACH load_tweets GENERATE myMap#'id' as id,myMap#'text' as text, myMap#'user' as user, myMap#'entities' as entities;

/* extracting hashtags bag from entities */
hashtag_details = foreach extract_details generate entities#'hashtags' as hashtags;

/* flattening entities data to extract more data from it */
tweet_text = foreach hashtag_details generate FLATTEN(hashtags) as (tweets_bag:map[]);

/* extracting hashtag from tweet bag */
words = foreach tweet_text generate tweets_bag#'text' as word;

/* Will create group of similar words */
grpd = group words by word;

/* Will store hashtag and number of time it occured */
cntd = foreach grpd generate group as hashtag, COUNT(words) as count;

/* Sort data according to hashtag count */
byNumber = order cntd by count;

/* Store into hdfs inside final_count folder */
store byNumber into '/user/cloudera/final_count';