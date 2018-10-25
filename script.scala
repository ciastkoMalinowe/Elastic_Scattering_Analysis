import org.dianahep.sparkroot._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import spark.implicits._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit

//*****************************
//*****************************
//**        DISTILL          **
//*****************************
//*****************************

//val path = "root://eostotem//eos/totem/data/cmstotem/2015/90m/Totem/Ntuple/version2/4496/TotemNTuple_9893.000.ntuple.root"
val path = "/net/archive/groups/plggdiamonds/root-spark/data/4495"
val df = spark.sqlContext.read.root(path)


//SELECTED DIAGONAL = diagonal 45b_56t

val d45b_56t_tracks  = List("track_rp_5_", "track_rp_21_", "track_rp_25_", "track_rp_104_", "track_rp_120_", "track_rp_124_")
val ad45b_56b_tracks = List("track_rp_5_", "track_rp_21_", "track_rp_25_", "track_rp_105_", "track_rp_121_", "track_rp_125_")
val d45t_56b_tracks  = List("track_rp_4_", "track_rp_20_", "track_rp_24_", "track_rp_105_", "track_rp_121_", "track_rp_125_")
val ad45t_56t_tracks = List("track_rp_4_", "track_rp_20_", "track_rp_24_", "track_rp_104_", "track_rp_120_", "track_rp_124_")

val diagonal_selected = df.withColumnRenamed(d45b_56t_tracks(0)+".valid", "L_1_F.valid").withColumnRenamed(d45b_56t_tracks(0)+".x", "L_1_F.x").withColumnRenamed(d45b_56t_tracks(0)+".y", "L_1_F.y").withColumnRenamed(d45b_56t_tracks(0)+".z", "L_1_F.z").withColumnRenamed(d45b_56t_tracks(1), "L_2_N").withColumnRenamed(d45b_56t_tracks(2), "L_2_F").withColumnRenamed(d45b_56t_tracks(3), "R_1_F").withColumnRenamed(d45b_56t_tracks(4), "R_2_N").withColumnRenamed(d45b_56t_tracks(5), "R_2_F")

diagonal_selected.printSchema()

val columns = List("L_1_F", "L_2_N", "L_2_F", "R_1_F", "R_2_N", "R_2_F", "event_info_", "trigger_data_", "timestamp")

val distilled = diagonal_selected.filter(($"L_1_F.valid".cast("Int") + $"L_2_N.valid".cast("Int") + $"L_2_F.valid".cast("Int")) >= 2).filter(($"R_1_F.valid".cast("Int") + $"R_2_N.valid".cast("Int") + $"R_2_F.valid".cast("Int")) >= 2).filter($"L_2_N.valid" && $"L_2_F.valid" && $"R_2_F.valid" && $"R_2_N.valid").withColumn("timestamp", $"event_info_.timestamp" - 1444860000).select(columns.head, columns.tail: _*)

distilled.createOrReplaceTempView("distilled")
spark.sqlContext.cacheTable("distilled")

//boundries valid only for DS1

val timestamp_min = 20900.0
val timestamp_max = 31500.0
val timestamp_bins = timestamp_max.toInt - timestamp_min.toInt +1


import org.dianahep.histogrammar._
import org.dianahep.histogrammar.bokeh._
import org.apache.spark.rdd.RDD


//************************************************
//************************************************
//**             DISTRIBUTION                   **
//************************************************
//************************************************

//------------------------------
// kinematics - reconstruction
//------------------------------

val withKinematics = distilled.withColumn("kinematics",doReconstruction($"L_2_N.x", $"L_2_N.y", $"L_2_F.x", $"L_2_F.y", $"R_2_N.x", $"R_2_N.y", $"R_2_F.x", $"R_2_F.y"))

//---------------------------------------
// cut evaluation (only cuts 1, 2 and 7)
//---------------------------------------

val withCuts = withKinematics.withColumn("cut1", createCut($"kinematics.right.th_x", $"kinematics.left.th_x", lit(- cut(1).a), lit(1.0), lit(cut(1).c), lit(cut(1).si))).withColumn("cut2", createCut($"kinematics.right.th_x", $"kinematics.left.th_x", lit(- cut(2).a), lit(1.0), lit(cut(2).c), lit(cut(2).si))).withColumn("cut7", createCut($"kinematics.double.th_x", $"kinematics.right.vtx_x" - $"kinematics.left.vtx_y", lit(- cut(7).a), lit(1.0), lit(cut(7).c), lit(cut(7).si)))

val withCutEvaluated = withCuts.withColumn("cut1Evaluated", evaluateCut($"cut1.params.a", $"cut1.cqa", $"cut1.params.b", $"cut1.cqb", $"cut1.params.c", $"cut1.params.si")).withColumn("cut2Evaluated", evaluateCut($"cut2.params.a", $"cut2.cqa", $"cut2.params.b", $"cut2.cqb", $"cut2.params.c", $"cut2.params.si")).withColumn("cut7Evaluated", evaluateCut($"cut7.params.a", $"cut7.cqa", $"cut7.params.b", $"cut7.cqb", $"cut7.params.c", $"cut7.params.si"))


//---------------------
// histograms
//---------------------

import io.continuum.bokeh._

case class HistogramBase(timestamp: Long, cut1Evaluated: Boolean, cut2Evaluated: Boolean, cut7Evaluated: Boolean)

case class TrackRP(x: Double, y: Double, z:Double, valid: Boolean)
val timestampRDD: RDD[TimestampWithFilter] = withCutEvaluated.select($"timestamp", $"cut1Evaluated", $"cut2Evaluated", $"cut7Evaluated").rdd.map { case x : Row => TimestampWithFilter(x(0).asInstanceOf[Long], x(1).asInstanceOf[Boolean], x(2).asInstanceOf[Boolean], x(3).asInstanceOf[Boolean]) }


val timestamp = timestampRDD.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: TimestampWithFilter => x.timestamp}))(new Increment, new Combine)
//val timestamp_sel = timestampRDD.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: TimestampWithFilter => x.timestamp},{x: TimestampWithFilter => x.cut1Evaluated && x.cut2Evaluated}))(new Increment, new Combine)
val timestamp_cut1 = timestampRDD.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: TimestampWithFilter => x.timestamp},{x: TimestampWithFilter => x.cut1Evaluated }))(new Increment, new Combine)
//val timestamp_cut2 = timestampRDD.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: TimestampWithFilter => x.timestamp},{x: TimestampWithFilter => x.cut2Evaluated }))(new Increment, new Combine)
//val timestamp_cut7 = timestampRDD.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: TimestampWithFilter => x.timestamp},{x: TimestampWithFilter => x.cut7Evaluated }))(new Increment, new Combine)
val glyph_one = timestamp.bokeh(lineColor=Color.Blue)
val glyph_two = timestamp_cut1.bokeh(lineColor=Color.Red)

//add legend, axis decsription etc
val legend = List("timestamp" -> List(glyph_one),"selected timestamp" -> List(glyph_two))
val plots = plot(glyph_one,glyph_two)
val leg = new Legend().plot(plots).legends(legend)
plots.renderers <<= (leg :: _)
save(plots,"rate_cmp.html")
