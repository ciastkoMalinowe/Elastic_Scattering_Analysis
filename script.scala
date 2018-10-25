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
val track_rp_suffixes = List(".valid", ".x", ".y", ".z")
val track_rp_names = List("L_1_F", "L_2_N", "L_2_F", "R_1_F", "R_2_N", "R_2_F") 

var diagonal_selected = df

for ( (track, name) <- (d45b_56t_tracks zip track_rp_names); sufix <- track_rp_suffixes) diagonal_selected = diagonal_selected.withColumnRenamed(track+suffix, name+suffix)

diagonal_selected.printSchema()

val columns = List("L_1_F", "L_2_N", "L_2_F", "R_1_F", "R_2_N", "R_2_F", "timestamp")

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

val withCutEvaluated = withCuts.withColumn("cut.1", evaluateCut($"cut1.params.a", $"cut1.cqa", $"cut1.params.b", $"cut1.cqb", $"cut1.params.c", $"cut1.params.si")).withColumn("cut.2", evaluateCut($"cut2.params.a", $"cut2.cqa", $"cut2.params.b", $"cut2.cqb", $"cut2.params.c", $"cut2.params.si")).withColumn("cut.7", evaluateCut($"cut7.params.a", $"cut7.cqa", $"cut7.params.b", $"cut7.cqb", $"cut7.params.c", $"cut7.params.si"))


//---------------------
// histograms
//---------------------

import io.continuum.bokeh._

case class HistogramBase(timestamp: Long, cut1Evaluated: Boolean, cut2Evaluated: Boolean, cut7Evaluated: Boolean)

case class TrackRP(x: Double, y: Double, z:Double, valid: Boolean)
case class CutEvaluation(1: Boolean, 2: Boolean, 7: Boolean)
case class NtupleSimplified(L_1_F:TrackRP, L_2_N:TrackRP, L_2_F:TrackRP, R_1_F:TrackRP, R_2_N: TrackRP, R_2_F: TrackRP, timestamp: Long, cut: CutEvalation, kinematics: Kinematics)

val resultRDD: RDD[NtupleSimplified] = withCutEvaluated.select($"L_1_F", $"L_2_N", $"L_2_F", $"R_1_F", $"R_2_N", $"R_2_F", $"timestamp", $"cut", $"kinematics").as[NtupleSimplified].rdd

val timestamp 					= resultRDD.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: NtupleSimplified => x.timestamp}))(new Increment, new Combine)
val timestamp_sel 				= resultRDD.aggregate(Histogram(timestamp_bins, timestamp_min - 0.5, timestamp_max + 0.5, {x: NtupleSimplified => x.timestamp},{x: NtupleSimplified => x.cut.1 }))(new Increment, new Combine)

val y_L_1_F_vs_x_L_1_F_nosel 	= resultRDD.aggregate(TwoDimensionallyHistogram(150, -15., 15., {x: NtupleSimplified => x.L_1_F.x}, 300, -30., +30., {x: NtupleSimplified => x.L_1_F.y}))
val y_L_2_N_vs_x_L_2_N_nosel	= resultRDD.aggregate(TwoDimensionallyHistogram(150, -15., 15., {x: NtupleSimplified => x.L_2_N.x}, 300, -30., +30., {x: NtupleSimplified => x.L_2_N.y}))
val y_L_2_F_vs_x_L_2_F_nosel	= resultRDD.aggregate(TwoDimensionallyHistogram(150, -15., 15., {x: NtupleSimplified => x.L_2_F.x}, 300, -30., +30., {x: NtupleSimplified => x.L_2_F.y}))
val y_R_1_F_vs_x_R_1_F_nosel	= resultRDD.aggregate(TwoDimensionallyHistogram(150, -15., 15., {x: NtupleSimplified => x.R_1_F.x}, 300, -30., +30., {x: NtupleSimplified => x.R_1_F.y}))
val y_R_2_N_vs_x_R_2_N_nosel	= resultRDD.aggregate(TwoDimensionallyHistogram(150, -15., 15., {x: NtupleSimplified => x.R_2_N.x}, 300, -30., +30., {x: NtupleSimplified => x.R_2_N.y}))
val y_R_2_F_vs_x_R_2_F_nosel	= resultRDD.aggregate(TwoDimensionallyHistogram(150, -15., 15., {x: NtupleSimplified => x.R_2_F.x}, 300, -30., +30., {x: NtupleSimplified => x.R_2_F.y}))

val y_L_1_F_vs_x_L_1_F_sel		= resultRDD.aggregate(TwoDimensionallyHistogram( 100, -3., +3., {x: NtupleSimplified => x.L_1_F.x}, 300, -30., +30., {x: NtupleSimplified => x.L_1_F.y}))
val y_L_2_N_vs_x_L_2_N_sel		= resultRDD.aggregate(TwoDimensionallyHistogram( 100, -3., +3., {x: NtupleSimplified => x.L_2_N.x}, 300, -30., +30., {x: NtupleSimplified => x.L_2_N.y}))
val y_L_2_F_vs_x_L_2_F_sel		= resultRDD.aggregate(TwoDimensionallyHistogram( 100, -3., +3., {x: NtupleSimplified => x.L_2_F.x}, 300, -30., +30., {x: NtupleSimplified => x.L_2_F.y}))
val y_R_1_F_vs_x_R_1_F_sel		= resultRDD.aggregate(TwoDimensionallyHistogram( 100, -3., +3., {x: NtupleSimplified => x.R_1_F.x}, 300, -30., +30., {x: NtupleSimplified => x.R_1_F.y}))
val y_R_2_N_vs_x_R_2_N_sel		= resultRDD.aggregate(TwoDimensionallyHistogram( 100, -3., +3., {x: NtupleSimplified => x.R_2_N.x}, 300, -30., +30., {x: NtupleSimplified => x.R_2_N.y}))
val y_R_2_F_vs_x_R_2_F_sel		= resultRDD.aggregate(TwoDimensionallyHistogram( 100, -3., +3., {x: NtupleSimplified => x.R_2_F.x}, 300, -30., +30., {x: NtupleSimplified => x.R_2_F.y}))

val th_x_diffLR 				= resultRDD.aggregate(Histogram(1000, -500E-6, +500E-6, {x: NtupleSimplified => x.kinematics.right.th_x - x.kinematics.left.th_x}))
val th_y_diffLR 				= resultRDD.aggregate(Histogram(500, -50E-6, +50E-6, {x: NtupleSimplified => x.kinematics.right.th_y - x.kinematics.left.th_y}))

val th_x_diffLF 				= resultRDD.aggregate(Histogram(400, -200E-6, +200E-6, {x: NtupleSimplified => x.kinematics.left.th_x - x.kinematics.double.th_x}
val th_x_diffRF 				= resultRDD.aggregate(Histogram(400, -200E-6, +200E-6, {x: NtupleSimplified => x.kinematics.right.th_x - x.kinematics.double.th_x}

val th_x_diffLR_vs_th_x  		= resultRDD.aggregate(TwoDimensionallyHistogram(100, -300E-6, +300E-6, {x: NtupleSimplified => x.kinematics.double.th_x}, 120, -120E-6, +120E-6, {x: NtupleSimplified => x.kinematics.right.th_x - x.kinematics.left.th_x})
val th_y_diffLR_vs_th_y  		= resultRDD.aggregate(TwoDimensionallyHistogram(100, -500E-6, +500E-6, {x: NtupleSimplified => x.kinematics.double.th_y}, 120, -120E-6, +120E-6, {x: NtupleSimplified => x.kinematics.right.th_x - x.kinematics.left.th_x})
val th_x_diffLR_vs_vtx_x 		= resultRDD.aggregate(TwoDimensionallyHistogram(100, -300E-3, +300E-3, {x: NtupleSimplified => x.kinematics.double.vtx_x}, 120, -120E-6, +120E-6, {x: NtupleSimplified => x.kinematics.right.th_x - x.kinematics.left.th_x})

val th_y_L_vs_th_x_L 			= resultRDD.aggregate(TwoDimensionallyHistogram(100, -115E-6, +11E-5, {x: NtupleSimplified => x.kinematics.left.th_x}, 100, 22E-6, +102E-6, {x: NtupleSimplified => x.kinematics.left.th_y}))
val th_y_R_vs_th_x_R 			= resultRDD.aggregate(TwoDimensionallyHistogram(100, -125E-6, +12E-5, {x: NtupleSimplified => x.kinematics.right.th_x}, 100, 27E-6, +102E-6, {x: NtupleSimplified => x.kinematics.right.th_y}))
val th_y_vs_th_x     			= resultRDD.aggregate(TwoDimensionallyHistogram(100, -300E-6, +300E-6, {x: NtupleSimplified => x.kinematics.double.th_x}, 100, -150E-6, +150E-6, {x: NtupleSimplified => x.kinematics.double.th_y}))

val th_y_L_vs_th_y_R 			= resultRDD.aggregate(TwoDimensionallyHistogram(300, -150E-6, +150E-6, {x: NtupleSimplified => x.kinematics.right.th_y}, 300, -150E-6, +150E-6, {x: NtupleSimplified => x.kinematics.left.th_y})

val th_x     					= resultRDD.aggregate(Histogram(250, -500E-6, +500E-6, {x: NtupleSimplified => x.kinematics.double.th_x}))
val th_y     					= resultRDD.aggregate(Histogram(250, -500E-6, +500E-6, {x: NtupleSimplified => x.kinematics.double.th_y}))

val th_y_flipped 				= resultRDD.aggregate(Histogram(250, -500E-6, +500E-6, {x: NtupleSimplified => - x.kinematics.double.th_y}))
 
val th_x_L   					= resultRDD.aggregate(Histogram(250, -500E-6, +500E-6, {x: NtupleSimplified => x.kinematics.left.th_x}))
val th_x_R   					= resultRDD.aggregate(Histogram(250, -500E-6, +500E-6, {x: NtupleSimplified => x.kinematics.right.th_x}))

val th_y_L   					= resultRDD.aggregate(Histogram(250, -500E-6, +500E-6, {x: NtupleSimplified => x.kinematics.left.th_y}))
val th_y_R   					= resultRDD.aggregate(Histogram(250, -500E-6, +500E-6, {x: NtupleSimplified => x.kinematics.right.th_y}))

//all are empty ? values of kinematisc.th_y_L_F, etc not found
// val th_y_L_F = f4.Histo1D(models[0], \"k_th_y_L_F\")\n",
// val th_y_L_N = f4.Histo1D(models[1], \"k_th_y_L_N\")\n",
// val th_y_R_N = f4.Histo1D(models[2], \"k_th_y_R_N\")\n",
// val th_y_R_F = f4.Histo1D(models[3], \"k_th_y_R_F\")\n",

val vtx_x    = resultRDD.aggregate(Histogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.double.vtx_x}))
val vtx_x_L  = resultRDD.aggregate(Histogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.left.vtx_x}))
val vtx_x_R  = resultRDD.aggregate(Histogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_x}))

val vtx_y    = resultRDD.aggregate(Histogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.double.vtx_y}))
val vtx_y_L  = resultRDD.aggregate(Histogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.left.vtx_y}))
val vtx_y_R  = resultRDD.aggregate(Histogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_y}))

val vtx_x_L_vs_vtx_x_R = resultRDD.aggregate(TwoDimensionallyHistogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_x}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.left.vtx_x}))
val vtx_y_L_vs_vtx_y_R = resultRDD.aggregate(TwoDimensionallyHistogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_y}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.left.vtx_y}))

val vtx_x_L_vs_th_x_L = resultRDD.aggregate(TwoDimensionallyHistogram(100, -600E-6, +600E-6, {x: NtupleSimplified => x.kinematics.left.th_x}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.left.vtx_x}))
val vtx_x_R_vs_th_x_R = resultRDD.aggregate(TwoDimensionallyHistogram(100, -600E-6, +600E-6, {x: NtupleSimplified => x.kinematics.right.th_x}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_x}))
val vtx_y_L_vs_th_y_L = resultRDD.aggregate(TwoDimensionallyHistogram(100, -600E-6, +600E-6, {x: NtupleSimplified => x.kinematics.left.th_y}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.left.vtx_y}))
val vtx_y_R_vs_th_y_R = resultRDD.aggregate(TwoDimensionallyHistogram(100, -600E-6, +600E-6, {x: NtupleSimplified => x.kinematics.right.th_y}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_y}))

val vtx_x_diffLR = resultRDD.aggregate(Histogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_x - x.kinematics.left.vtx_x}))
val vtx_y_diffLR = resultRDD.aggregate(Histogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_y - x.kinematics.left.vtx_y}))

val vtx_x_diffLR_vs_th_x = resultRDD.aggregate(TwoDimensionallyHistogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.double.th_x}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_x - x.kinematics.left.vtx_x}))
val vtx_y_diffLR_vs_th_y = resultRDD.aggregate(TwoDimensionallyHistogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.double.th_y}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_y - x.kinematics.left.vtx_y}))

val vtx_x_diffLR_vs_vtx_x_R = resultRDD.aggregate(TwoDimensionallyHistogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_x}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_x - x.kinematics.left.vtx_x}))
val vtx_y_diffLR_vs_vtx_y_R = resultRDD.aggregate(TwoDimensionallyHistogram(100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_y}, 100, -0.5, +0.5, {x: NtupleSimplified => x.kinematics.right.vtx_y - x.kinematics.left.vtx_y}))

//CALCULATE ACCEPTANCE DIV CORRECTION
val glyph_one = timestamp.bokeh(lineColor=Color.Blue)
val glyph_two = timestamp_cut1.bokeh(lineColor=Color.Red)

//add legend, axis decsription etc
val legend = List("timestamp" -> List(glyph_one),"selected timestamp" -> List(glyph_two))
val plots = plot(glyph_one,glyph_two)
val leg = new Legend().plot(plots).legends(legend)
plots.renderers <<= (leg :: _)
save(plots,"rate_cmp.html")
