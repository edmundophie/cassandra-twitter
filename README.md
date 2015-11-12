# cassandra-twitter
Simple usage of Cassandra driver on Java for CRUD operation

## Requirements
 - JDK >= 1.7
 - [Maven](https://maven.apache.org/download.cgi) 
 - Cassandra

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

Run `Twitter` from the generated `jar` in `target` folder  

	 ```
	 $ java -cp target/dependency/*:target/cassandra-twitter-1.0.jar com.edmundophie.cassandra.Twitter <host> <replication_strategy> <replication_factor>
	 ```
*Note that parameter `host`, `replication_strategy`, and `replication_factor` are optional. If any of them are not provided then they will be set to each own default value;*
Default value:
- `host`: `127.0.0.1`
- `replication_strategy`: `SimpleStrategy`
- `replication_factor`: `1`

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
