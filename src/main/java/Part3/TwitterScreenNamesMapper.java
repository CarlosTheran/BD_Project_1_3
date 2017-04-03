package Part3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import java.io.IOException;
//import java.util.Scanner;
/**
 * Created by carlos on 03-20-17.
 */
public class TwitterScreenNamesMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            //User user = TwitterObjectFactory.createUser(rawTweet);
            String tweetScreenName = status.getUser().getScreenName();

            context.write(new Text(tweetScreenName), new IntWritable(1));

        }
        catch(TwitterException e){

        }

    }


}
