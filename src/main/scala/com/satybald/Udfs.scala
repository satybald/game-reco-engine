package com.satybald

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object Udfs {
  private val gameMap = Map(
    Seq("GameRoundStarted" -> 1,
        "GameRoundEnded" -> 1,
        "GameRoundDropped" -> -20,
        "GameWinEvent" -> 3,
        "MajorWinEvent" -> 7): _*).withDefaultValue(0)
  private val playerMap = Map(
    Seq("AchievementLikedEvent" -> +1,
        "AchievementCommentedEvent" -> 2,
        "AchievementSharedEvent" -> 2,
        "ProfileVisitedEvent" -> 5,
        "DirectMessageSentEvent" -> 7): _*).withDefaultValue(0)

  private val ownerMapping = Map(
    "AchievementLikedEvent" -> "achievement_owner_id",
    "AchievementCommentedEvent" -> "achievement_owner_id",
    "AchievementSharedEvent" -> "follower_id",
    "ProfileVisitedEvent" -> "user_id",
    "DirectMessageSentEvent" -> "target_profile_id"
  )
  private val influencerMapping = Map(
    "AchievementLikedEvent" -> "follower_id",
    "AchievementCommentedEvent" -> "follower_id",
    "AchievementSharedEvent" -> "achievement_owner_id",
    "ProfileVisitedEvent" -> "user_id",
    "DirectMessageSentEvent" -> "source_profile_id"
  )

  val playerScoreUdf = udf(getScore(playerMap) _)
  val gameScoreUdf = udf(getScore(gameMap) _)
  val ownerUdf = udf(getUser(ownerMapping) _)
  val infulencerUdf = udf(getUser(influencerMapping) _)

  private def getScore(map: Map[String, Int])(eventType: String): Int = {
    map.getOrElse(eventType, 0)
  }
  private def getUser(map: Map[String, String])(payload: Row,
                                                eventType: String) = {
    if (map.contains(eventType)) payload.getAs[String](map(eventType))
    else null
  }
}
