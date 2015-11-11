package com.edmundophie.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by edmundophie on 11/9/15.
 */

public class Twitter {
    private Cluster cluster;
    private Session session;
    private final static String DEFAULT_HOST = "127.0.0.1";
    private final static String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
    private final static String DEFAULT_REPLICATION_FACTOR = "1";
    private final static String TWITTER_KEYSPACE = "twitter";
    private final static String TABLE_USERS = "users";
    private final static String TABLE_FOLLOWERS = "followers";
    private final static String TABLE_USERLINE = "userline";
    private final static String TABLE_TIMELINE = "timeline";
    private final static String TABLE_TWEETS = "tweets";
    private final static String TABLE_FRIENDS = "friends";

    public static void main(String[] args) throws IOException {
        String host = DEFAULT_HOST;
        String replicationStrategy = DEFAULT_REPLICATION_STRATEGY;
        String replicationFactor = DEFAULT_REPLICATION_FACTOR;
        if(args.length>0)
            host = args[0];

        if(args.length>1)
            replicationStrategy = args[1];

        if(args.length>2)
            replicationFactor = args[2];

        Twitter twitter = new Twitter();
        twitter.connect(host);
        twitter.createSchema(TWITTER_KEYSPACE, replicationStrategy, replicationFactor);

        System.out.println("\n*** DIRECTIVES ***");
        System.out.println("---------****--------");
        System.out.println("* REGISTER <username> <password>");
        System.out.println("* FOLLOW <follower_username> <followed_username>");
        System.out.println("* ADDTWEET <username> <tweet>");
        System.out.println("* VIEWTWEET <username>");
        System.out.println("* TIMELINE <username>");
        System.out.println("* EXIT");
        System.out.println("---------****--------");
        System.out.println("* Type your command...");

        String command = null;
        String unsplittedParams = null;

        do {
            String input = new BufferedReader(new InputStreamReader(System.in)).readLine().trim();

            if(input.isEmpty()){
                printInvalidCommand();
            }
            else {
                String[] parameters = new String[0];
                int i = input.indexOf(" ");

                if (i > -1) {
                    command = input.substring(0, i);
                    unsplittedParams = input.substring(i + 1);
                    parameters = unsplittedParams.split(" ");
                } else
                    command = input;

                System.out.println("Command yang diinput: " + command);
                System.out.println("Byk param: "+ parameters.length);

                if (command.equalsIgnoreCase("REGISTER") && parameters.length == 2) {
                    if(twitter.isUserExist(parameters[0]))
                        System.out.println("* Username already exist!");
                    else {
                        twitter.registerUser(parameters[0], parameters[1]);
                        System.out.println("* " + parameters[0] + " succesfully registered");
                    }
                } else if (command.equalsIgnoreCase("FOLLOW") && parameters.length == 2) {
                    if(!twitter.isUserExist(parameters[0]) || !twitter.isUserExist(parameters[1]))
                        System.out.println("* Failed to execute command!\n* User may have not been registered");
                    else {
                        twitter.followUser(parameters[0], parameters[1]);
                        System.out.println("* " + parameters[0] + " is now following " + parameters[1]);
                    }
                } else if (command.equalsIgnoreCase("ADDTWEET") && parameters.length>=2) {
                    if(!twitter.isUserExist(parameters[0]))
                        System.out.println("* Failed to execute command!\n* User may have not been registered");
                    else {
                        twitter.insertTweet(parameters[0], unsplittedParams.substring(parameters[0].length()));
                        System.out.println("* " + parameters[0] + " tweet has been added");
                    }
                } else if (command.equalsIgnoreCase("VIEWTWEET") && parameters.length==1) {
                    ResultSet results = twitter.getUserline(parameters[0]);
                    printTweet(results);
                } else if (command.equalsIgnoreCase("TIMELINE") && parameters.length==1) {
                    ResultSet results = twitter.getTimeline(parameters[0]);
                    printTweet(results);
                } else if (command.equalsIgnoreCase("EXIT")) {
                    System.out.println("Exiting...");
                } else
                    printInvalidCommand();
            }
        } while(!command.equalsIgnoreCase("EXIT"));

        twitter.close();
    }

    public static void printInvalidCommand() {
        System.err.println("* Invalid Command");
    }

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.println("Connected to cluster: " + metadata.getClusterName());

        for(Host host:metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s, Host: %s, Rack: %s\n", host.getDatacenter(), host.getDatacenter(), host.getRack());
        }
        session = cluster.connect();
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public Session getSession() {
        return this.session;
    }

    public void createSchema(String keyspace, String replicationStrategy, String replicationFactor) {
        System.out.println("\nCreating schema...");

        // Create keyspace
        String query = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class':'" + replicationStrategy + "', 'replication_factor':"+replicationFactor+"};";
        session.execute(query);

        session.execute("USE "+TWITTER_KEYSPACE + ";");

        // Create table 'users'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_USERS +" (" +
                "username text PRIMARY KEY, " +
                "password text" +
                ")";
        session.execute(query);

        // Create table 'friends'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_FRIENDS +" (" +
                " username text, " +
                " friend text, " +
                " since timestamp, " +
                " PRIMARY KEY (username, friend) " +
                ")";
        session.execute(query);

        // Create table 'followers'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_FOLLOWERS+" (" +
                " username text, " +
                " follower text, " +
                " since timestamp, " +
                " PRIMARY KEY (username, follower) " +
                ")";
        session.execute(query);

        // Create table 'tweets'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_TWEETS +" (" +
                " tweet_id uuid PRIMARY KEY, " +
                " username text, " +
                " body text " +
                ")";
        session.execute(query);

        // Create table 'userline'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_USERLINE +" (" +
                " username text, " +
                " time timeuuid, " +
                " tweet_id uuid, " +
                " PRIMARY KEY (username, time)" +
                ") WITH CLUSTERING ORDER BY (time DESC)";
        session.execute(query);

        // Create table 'timeline'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_TIMELINE  +" (" +
                " username text, " +
                " time timeuuid, " +
                " tweet_id uuid, " +
                " PRIMARY KEY (username, time)" +
                ") WITH CLUSTERING ORDER BY (time DESC)";
        session.execute(query);
    }

    public void registerUser(String username, String password) {
        String query = "INSERT INTO " + TABLE_USERS + " (username, password) " +
                "VALUES ('" + username + "', '" + password + "');";
        session.execute(query);
    }

    public void followUser(String followerUsername, String followedUsername) {
        String timeId = UUIDs.timeBased().toString();
        String query = "INSERT INTO " + TABLE_FOLLOWERS + " (username, follower, since) " +
                "VALUES ('" + followedUsername + "', '" + followerUsername+ "', toUnixTimestamp("+timeId+"));";
        session.execute(query);

        query = "INSERT INTO " + TABLE_FRIENDS + " (username, friend, since) " +
                "VALUES ('" + followerUsername + "', '" + followedUsername+ "', toUnixTimestamp("+timeId+"));";
        session.execute(query);
    }

    public void insertTweet(String username, String tweet) {
        String tweetId = UUIDs.random().toString();
        String timeId = UUIDs.timeBased().toString();

        String query = "INSERT INTO " + TABLE_TWEETS + " (tweet_id, username, body) " +
                "VALUES (" + tweetId + ", '" + username +"', '"+ tweet +"');";
        session.execute(query);

        query = "INSERT INTO "+ TABLE_USERLINE +" (username, time, tweet_id) " +
                "VALUES ('" + username + "', " + timeId +", "+ tweetId +");";
        session.execute(query);

        query = "INSERT INTO "+ TABLE_TIMELINE +" (username, time, tweet_id) " +
                "VALUES ('" + username + "', " + timeId +", "+ tweetId +");";
        session.execute(query);

        ResultSet results = session.execute("SELECT follower FROM " + TABLE_FOLLOWERS +
                " WHERE username = '" + username + "';");

        List<Row> rows = results.all();
        if(rows.size()>0) {
            query = "INSERT INTO " + TABLE_TIMELINE + " (username, time, tweet_id) VALUES ";

            for(int i=0;i<rows.size();++i) {
                query += "('" + rows.get(i).getString(0) + "', "+ timeId +", " + tweetId + ")";
                if(i!=rows.size()-1)
                    query += ",";
            }
            query += ";";

            session.execute(query);
        }
    }

    public ResultSet getUserline(String username) {
        ResultSet results = session.execute("SELECT tweet_id FROM " + TABLE_USERLINE +
                " WHERE username = '" + username + "';");
        List<Row> rows = results.all();

        String tweetIds = "";
        for(int i=0;i<rows.size();++i) {
            tweetIds += rows.get(i).getUUID("tweet_id");
            if(i!=rows.size()-1)
                tweetIds += ",";
        }

        String query = "SELECT username, body FROM " + TABLE_TWEETS +
                " WHERE tweet_id IN ("+ tweetIds +");";
        results = session.execute(query);

        return results;
    }

    public ResultSet getTimeline(String username) {
        ResultSet results = session.execute("SELECT tweet_id FROM " + TABLE_TIMELINE +
                " WHERE username = '" + username + "';");
        List<Row> rows = results.all();

        String tweetIds = "";
        for(int i=0;i<rows.size();++i) {
            tweetIds += rows.get(i).getUUID("tweet_id");
            if(i!=rows.size()-1)
                tweetIds += ",";
        }

        String query = "SELECT * FROM " + TABLE_TWEETS +
                " WHERE tweet_id IN ("+ tweetIds +");";
        results = session.execute(query);
        return results;
    }

    public static void printTweet(ResultSet results) {
        if(results.all().isEmpty())
            System.out.println("* No tweet yet");
        for(Row row:results) {
            System.out.println("@" + row.getString("username") + ": " +row.getString("body"));
        }
    }

    public boolean isUserExist(String username) {
        ResultSet results = session.execute("SELECT * FROM " + TABLE_USERS +
                " WHERE username = '" + username + "';");
        List<Row> rows = results.all();
        return rows.size()>0;
    }
}
