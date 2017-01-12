package com.fijimf.deepfij.spark

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.{Vector, Vectors}

object Base {

  val sc = SparkCommon.sparkContext
  val sqlContext = SparkCommon.sparkSQLContext
  val url = "jdbc:mysql://localhost:3306/deepfijdb"

  val prop = new java.util.Properties
  prop.setProperty("user", "root")

  def main(args: Array[String]) {


    val games = sqlContext.read.jdbc(url,
      "(select h.name home_name, h.key home_key, a.name away_name, a.key away_key, g.*, r.home_score, r.away_score, r.periods, r.home_score-r.away_score margin " +
        "from team h, team a, game g" +
        "  left join result r on r.game_id = g.id  " +
        "where h.id = g.home_team_id and a.id = g.away_team_id and r.game_id = g.id) games", prop)


    val x_margin = sqlContext.read.jdbc(url, "(select * from stat_value where stat_key = 'x-margin-ties') x_margin", prop)
    val mean_mrg = sqlContext.read.jdbc(url, "(select * from stat_value where stat_key = 'meanmrgp') meanmrg", prop)
    val var_mrg = sqlContext.read.jdbc(url, "(select * from stat_value where stat_key = 'varmrg') varmrg", prop)
    val hxs = x_margin.withColumnRenamed("team_id", "home_team_id").withColumnRenamed("value", "home_x")
    val hms = mean_mrg.withColumnRenamed("team_id", "home_team_id").withColumnRenamed("value", "home_m")
    val hvs = var_mrg.withColumnRenamed("team_id", "home_team_id").withColumnRenamed("value", "home_v")
    val axs = x_margin.withColumnRenamed("team_id", "away_team_id").withColumnRenamed("value", "away_x")
    val ams = mean_mrg.withColumnRenamed("team_id", "away_team_id").withColumnRenamed("value", "away_m")
    val avs = var_mrg.withColumnRenamed("team_id", "away_team_id").withColumnRenamed("value", "away_v")

    val a = games.join(hxs, Seq("home_team_id", "date")).join(axs, Seq("away_team_id", "date"))
      .join(hms, Seq("home_team_id", "date")).join(ams, Seq("away_team_id", "date"))
      .join(hvs, Seq("home_team_id", "date")).join(avs, Seq("away_team_id", "date"))

    val z: (Double, Double, Double, Double, Double, Double) => Vector = (a, b, c, d, e, f) => {
      Vectors.dense(a, b, c, d, e, f)
    }

    val ff: (Int, Int) => Int = (mrg, nump) => if (nump > 2) {
      21
    } else {
      if (mrg < -19) {
        1
      } else if (mrg > 19) {
        42
      } else {
        mrg + 21
      }
    }
println(a.collect().length)
    a.show(5000)
    System.exit(-255)
    import org.apache.spark.sql.functions._
    val toFeatureVec = udf(z)
    val toLabel = udf(ff)
    val featuresAdded = a.withColumn("features", toFeatureVec(col("home_x"), col("home_m"), col("home_v"), col("away_x"), col("away_m"), col("away_v")))

    val train = featuresAdded.filter(featuresAdded.col("margin").isNotNull) .withColumn("label", toLabel(col("margin"), col("periods")))
    val predict = featuresAdded.filter(featuresAdded.col("margin").isNull)
    train.show(20)
    predict.show(20)
System.exit(-255)
    val layers = Array[Int](6, 12, 12, 43)

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)


    val model = trainer.fit(train)
    // compute precision on the test set
    val result = model.transform(predict)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("precision")
    println("Precision:" + evaluator.evaluate(predictionAndLabels))



  }

}
