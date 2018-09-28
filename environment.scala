package environment

case class UnitOptics(v_x: Double, L_x: Double, v_y: Double, L_y: Double)
case class Optics(L_2_F: UnitOptics, L_2_N: UnitOptics, L_1_F: UnitOptics, R_1_F: UnitOptics, R_2_N: UnitOptics, R_2_F: UnitOptics)

// optics: v_x and v_y [1], L_x and L_y [mm]
// sent by Frici on 23 Oct 2015

val L_2_F = UnitOptics(-1.8975238180785, 0.10624114216534E3, -0.000000003186328, 261.86107319594E3)
val L_2_N = UnitOptics(-2.1876926248587, 2.95354551535812E3, 0.020514691932280, 236.73917844622E3)
val L_1_F = UnitOptics(-2.2756291135852, 3.81642926806849E3, 0.026731729097787, 229.12591622497E3)
val R_1_F = UnitOptics(-2.2582096378676, 3.76173451557219E3, 0.026620752547344, 229.83172867404E3)
val R_2_N = UnitOptics(-2.1682134167719, 2.89089335973313E3, 0.020429520698897, 237.53468452721E3)
val R_2_F = UnitOptics(-1.8712479992497, 0.01733151135160E3, -0.000000023213780, 262.95254622452E3)

val optics = Optics(L_2_F, L_2_N, L_1_F, R_1_F, R_2_N, R_2_F)

case class Momentum(p: Int, p_L: Int, p_R: Int)

// beam momentum (GeV)
val p   = 6500
val p_L = 6500
val p_R = 6500

val momentum = Momentum(p, p_L, p_R)

case class Environment(momentum: Momentum, optics: Optics)

val environment = Environment(momentum, optics)

