## Game​ ​ recommendation​ ​ engine

There is an online gaming company with many games available for its users, The Company. Its
gaming platform is not an ordinary one - it is a hybrid of a social network and amazing gaming
experience.

## Approach
1. Create an aggregate table that contains user, follower scores - PIS
2. Create an aggregate table for user, game score interaction - PGL.
3. Join and aggregate both tables on follower and user_id and then sum multiple of columns i.e. SUM(PIS * PGL)
4. Left merge PGL table with previous table based on the user_id and game_id if PGL(p, g) > 0 then multiply for SUM(PIS * PGL)

## Limitations
* recency factor weighting is not implemented

## Getting Started
Running locally
```
sbt runMain com.satybald.Main
```

Submit to cluster:
```
sbt assembly
spark-submit \
--class com.satybald.Main
--master=MASTER_IP
--game-path=/path/to_game.json
--player-path=/path/to_player.json
```
