package cuts

class CutParameters(a: Double, b: Double, c: Double, si: Double)
case class Cut(cqa: Double, cqb: Double, params: CutParameters)

// cut structure:
//      | a*qa + b*qb + c| < n_si * si
//params valid for DS1 45b_56t
val cut = Map(1 -> CutParameters(1.0, 1.0, +0.0000005, 0.0000095), 2 -> CutParameters(1.0, 1.0, -0.00000021, 0.000000028), 7 -> CutParameters(181.0, 1.0, 0.0, 0.012))


val createCut = udf((right: Double, left: Double, a:Double, b:Double, c:Double, si:Double) => Cut(right, left, CutParameters(a, b, c, si)))

case class CutParameters(a: Double, b: Double, c: Double, si: Double)
case class Cut(cqa: Double, cqb: Double, params: CutParameters)

val n_si = 4.0

//udf on Cut case class not working ... : (
val evaluateCut = udf((a: Double, cqa: Double, b: Double, cqb: Double, c: Double, si: Double) => (math.abs(a * cqa + b * cqb + c) <= n_si * si))

//withCuts.printSchema()


