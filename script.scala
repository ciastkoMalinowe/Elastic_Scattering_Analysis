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
val path = "root://eostotem//eos/totem/data/cmstotem/2015/90m/Totem/Ntuple/version2/4495"
val df = spark.sqlContext.read.root(path)


//SELECTED DIAGONAL = diagonal 45b_56t

val d45b_56t_tracks  = List("track_rp_5_", "track_rp_21_", "track_rp_25_", "track_rp_104_", "track_rp_120_", "track_rp_124_")
val ad45b_56b_tracks = List("track_rp_5_", "track_rp_21_", "track_rp_25_", "track_rp_105_", "track_rp_121_", "track_rp_125_")
val d45t_56b_tracks  = List("track_rp_4_", "track_rp_20_", "track_rp_24_", "track_rp_105_", "track_rp_121_", "track_rp_125_")
val ad45t_56t_tracks = List("track_rp_4_", "track_rp_20_", "track_rp_24_", "track_rp_104_", "track_rp_120_", "track_rp_124_")

val diagonal_selected = df.withColumnRenamed(d45b_56t_tracks(0), "L_1_F").withColumnRenamed(d45b_56t_tracks(1), "L_2_N").withColumnRenamed(d45b_56t_tracks(2), "L_2_F").withColumnRenamed(d45b_56t_tracks(3), "R_1_F").withColumnRenamed(d45b_56t_tracks(4), "R_2_N").withColumnRenamed(d45b_56t_tracks(5), "R_2_F")

val columns = List("L_1_F", "L_2_N", "L_2_F", "R_1_F", "R_2_N", "R_2_F", "event_info_", "trigger_data_", "timestamp")

val distilled = diagonal_selected.filter(($"L_1_F.valid".cast("Int") + $"L_2_N.valid".cast("Int") + $"L_2_F.valid".cast("Int")) >= 2).filter(($"R_1_F.valid".cast("Int") + $"R_2_N.valid".cast("Int") + $"R_2_F.valid".cast("Int")) >= 2).filter($"L_2_N.valid" && $"L_2_F.valid" && $"R_2_F.valid" && $"R_2_N.valid").withColumn("timestamp", $"event_info_.timestamp" - 1444860000).select(columns.head, columns.tail: _*)

distilled.createOrReplaceTempView("distilled")
spark.sqlContext.cacheTable("distilled")

//boundries valid only for DS1

val timestamp_min = 20900.0
val timestamp_max = 31500.0
val timestamp_bins = timestamp_max.toInt - timestamp_min.toInt +1

//*****************************************************
//*****************************************************
//**        HISTOGRAM DRAWING POSSIBILITIES          **
//*****************************************************
//*****************************************************



//---------------------------------------
//scala histo (visible in zeppelin)
//---------------------------------------
//val (startValues, counts) = distilled.select($"timestamp".cast("double") as "timestamp").map(v => v.getDouble(0)).rdd.histogram(timestamp_bins)
//val timestamp_distribution = spark.createDataFrame(startValues.zip(counts))
//timestamp_distribution.createOrReplaceTempView("timestamp_distribution")
//timestamp_distribution.head(10)

//---------------------------------------
//histogrammar ascii histo
//---------------------------------------

//import org.dianahep.histogrammar._
//import org.dianahep.histogrammar.ascii._
//import org.dianahep.histogrammar.sparksql._

//distilled.select($"timestamp" as "timestamp").head(10)
//distilled.histogrammar(Bin(30, timestamp_min, timestamp_max, $"timestamp")).println

//---------------------------------------
//histogrammar bokeh one histo
//---------------------------------------

//import org.dianahep.histogrammar._
//import org.dianahep.histogrammar.bokeh._
import org.apache.spark.rdd.RDD


//val histogram = Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: Long => x})
//val timestamp: RDD[Long] = distilled.select($"timestamp").rdd.map { case x : Row => x(0).asInstanceOf[Long] }
// //timestamp.take(10).foreach(println)
//val final_histogram = timestamp.aggregate(histogram)(new Increment, new Combine)
//val plot = final_histogram.bokeh().plot()
//save(plot,"histogram1.html")

//---------------------------------------
//histogrammar bokeh labeling - not working???
//---------------------------------------

//import io.continuum.bokeh._

//val histos = Label(
//  "timestamp1" -> Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: Long => x}),
//  "timestamp2" -> Histogram(timestamp_bins, timestamp_min - 1000.5, timestamp_max + 1000.5, {x: Long => x - 1000}))

//timestamp.aggregate(histos)(new Increment, new Combine)
//val glyph_one = histos("timestamp1").bokeh(glyphType="histogram",fillColor=Color.Blue)
//val glyph_two = histos("timestamp2").bokeh(glyphType="histogram",fillColor=Color.Red)
//val plot_both = plot(glyph_one,glyph_two)
//save(plot_both,"histogram2.html")

//-------------------------------------------
//histogrammar 2 histos on one plot
//-------------------------------------------

//val timestamp1 = timestamp.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: Long => x}))(new Increment, new Combine)
//val timestamp2 = timestamp.aggregate(Histogram(timestamp_bins, timestamp_min - 1000.5, timestamp_max + 1000.5, {x: Long => x - 1000}))(new Increment, new Combine)

//val glyph_one = timestamp1.bokeh(lineColor=Color.Blue)
//val glyph_two = timestamp2.bokeh(lineColor=Color.Red)
//val plot_both = plot(glyph_one,glyph_two)
//save(plot_both,"histogram3.html")

//************************************************
//************************************************
//**             DISTRIBUTION                   **
//************************************************
//************************************************

//-----------------
// kinematics - reconstruction
//-----------------_

val withKinematics = distilled.withColumn("kinematics",doReconstruction($"L_2_N.x", $"L_2_N.y", $"L_2_F.x", $"L_2_F.y", $"R_2_N.x", $"R_2_N.y", $"R_2_F.x", $"R_2_F.y"))
//withKinematics.printSchema()
withKinematics.head(10)


//----------------
// cut evaluation (only cuts 1, 2 and 7)
//----------------

val withCuts = withKinematics
  .withColumn("cut1", createCut($"kinematics.right.th_x", $"kinematics.left.th_x", lit(- cut(1).a), lit(1.0), lit(cut(1).c), lit(cut(1).si)))
  .withColumn("cut2", createCut($"kinematics.right.th_x", $"kinematics.left.th_x", lit(- cut(2).a), lit(1.0), lit(cut(2).c), lit(cut(2).si)))
  .withColumn("cut7", createCut($"kinematics.right.th_x", $"kinematics.right.vtx_x" - $"kinematics.left.vtx_y", lit(- cut(7).a), lit(1.0), lit(cut(7).c), lit(cut(7).si)))

val withCutEvaluated = withCuts
  .filter(evaluateCut($"cut1.params.a", $"cut1.cqa", $"cut1.params.b", $"cut1.cqb", $"cut1.params.c", $"cut1.params.si"))
  .filter(evaluateCut($"cut2.params.a", $"cut2.cqa", $"cut2.params.b", $"cut2.cqb", $"cut2.params.c", $"cut2.params.si"))
  .filter(evaluateCut($"cut7.params.a", $"cut7.cqa", $"cut7.params.b", $"cut7.cqb", $"cut7.params.c", $"cut7.params.si"))

//print(withCutEvaluated.count())

//---------------------
//- histograms
//---------------------

//import io.continuum.bokeh._

//val timestamp: RDD[Long] = distilled.select($"timestamp").rdd.map { case x : Row => x(0).asInstanceOf[Long] }
//val timestamp_sel: RDD[Long] = withCutEvaluated.select($"timestamp").rdd.map { case x : Row => x(0).asInstanceOf[Long] } 
//val timestamp1 = timestamp.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: Long => x}))(new Increment, new Combine)
//val timestamp2 = timestamp_sel.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: Long => x}))(new Increment, new Combine)

//val glyph_one = timestamp1.bokeh(lineColor=Color.Blue)
//val glyph_two = timestamp2.bokeh(lineColor=Color.Red)
//val plot_both = plot(glyph_one,glyph_two)
//save(plot_both,"histogram5.html")


