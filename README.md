# cassandra-twitter
Simple usage of Cassandra driver on Java for CRUD operation

## Requirements
 - JDK >= 1.7
 - [Maven](https://maven.apache.org/download.cgi) 

## How to Build
1. Resolve maven dependency  

	 ```
	 $ mvn dependency:copy-dependencies
	 ```
2. Build `jar` using maven `mvn`  

	 ```
	 $ mvn package
	 ```

## How to Run	 

1. Run `Twitter` from the generated `jar` in `target` folder  

	 ```
	 $ java -cp target/dependency/*:target/cassandra-twitter-1.0.jar com.edmundophie.cassandra.Twitter
	 ```
2. Run `ChatClient` from the generated `jar` in `target` folder  

	 ```
	 $ java -cp target/dependency/*:target/kafka-chat-1.0.jar com.edmundophie.chat.ChatClient
	 ```

## Commands
- `register <username>`
- `follow <follower_username> <followed_username>`
- `addtweet <username> <tweet>`
- `viewtweet <username>`
- `timeline <username>`
- `exit`

## Team Member
- Edmund Ophie/ 13512095
- Kevin/ 13512097
