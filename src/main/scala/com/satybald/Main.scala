package com.satybald

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.satybald.Udfs._

case class GameConfig(playerPath: String, gamePath: String)

class RecommendationEngine(sparkSession: SparkSession, config: GameConfig) {

  def getRecommendation() = {
    val gameDataDf = sparkSession.read.json(config.gamePath)
    val playerDataDf = sparkSession.read.json(config.playerPath)

    val playerScoresDf = playerDataDf
      .select(
        playerScoreUdf(col("event_type")).as("score"),
        ownerUdf(col("payload"), col("event_type")).as("owner_id"),
        infulencerUdf(col("payload"), col("event_type")).as("follower_id")
      )
      .filter(col("score") > 0)
      .groupBy(col("owner_id"), col("follower_id"))
      .agg(sum(col("score")).as("total"))

    val pisDf = gameDataDf
      .select(
        gameScoreUdf(col("event_type")).as("pis_score"),
        col("payload.game_id").as("pis_game_id"),
        col("payload.user_id").as("pis_user_id")
      )
      .filter(col("pis_score") > 0)
      .groupBy(col("pis_user_id"), col("pis_game_id"))
      .agg(sum(col("pis_score")).as("pis_score"))

    val pglDf =
      playerScoresDf
        .join(pisDf, col("follower_id") === col("pis_user_id"))
        .withColumn("pgl_score", col("total") * col("pis_score"))
        .groupBy(col("owner_id").as("pgl_user_id"),
                 col("pis_game_id").as("pgl_game_id"))
        .agg(sum(col("pgl_score")).as("pgl_score"))

    val recoDf = pglDf
      .join(pisDf,
            col("pgl_user_id") === col("pis_user_id")
              && col("pgl_game_id") === col("pis_game_id"),
            "left")
      .withColumn("score",
                  when(col("pgl_score") > 0,
                       col("pgl_score") * col("pis_score"))
                    .otherwise(col("pis_score")))
      .na.fill(0, Seq("score"))
      .select(col("pgl_user_id").as("user_id"),
              col("pgl_game_id").as("game_id"),
              col("score"))
      .orderBy(col("score").desc)

    recoDf.show(100, false)
  }
}

object Main extends App {
  val parser = buildParser()
  val playerPath = getClass.getResource("player_data.json").getPath
  val gameDataPath = getClass.getResource("game_data.json").getPath
  parser.parse(args, GameConfig(playerPath, gameDataPath)) match {
    case Some(config) =>
      val session = SparkSession.builder
        .master("local[*]")
        .appName("reco_app")
        .getOrCreate()
      session.sparkContext.setLogLevel("WARN")

      new RecommendationEngine(session, config).getRecommendation()
      session.close()

    case _ =>
  }

  private def buildParser() = {
    new scopt.OptionParser[GameConfig]("reco") {
      opt[String]('g', "game-path")
        .action((path, c) => c.copy(gamePath = path))
        .text("Path to game file")
      opt[String]('p', "player-path")
        .action((path, c) => c.copy(playerPath = path))
        .text("Path to player file")
    }
  }
}
