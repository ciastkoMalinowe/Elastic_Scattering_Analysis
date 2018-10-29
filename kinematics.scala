case class ArmReconstruction(th_x: Double, th_y: Double, vtx_x: Double, vtx_y: Double)
case class ThetaReconstruction(th: Double, phi: Double)
case class TReconstruction(x: Double, y: Double, t: Double)
case class Kinematics(leftArm: ArmReconstruction, rightArm: ArmReconstruction, doubleArm: ArmReconstruction, theta: ThetaReconstruction, t: TReconstruction)
case class Hit(x: Double, y: Double)

val doReconstruction = udf((L_2_N_x: Double, L_2_N_y: Double, L_2_F_x:Double, L_2_F_y: Double, R_2_N_x:Double, R_2_N_y:Double, R_2_F_x:Double, R_2_F_y:Double) => {

  val L_2_N = Hit(L_2_N_x, L_2_N_y)
  val L_2_F = Hit(L_2_F_x, L_2_F_y)
  val R_2_N = Hit(R_2_N_x, R_2_N_y)
  val R_2_F = Hit(R_2_F_x, R_2_F_y)
  // single-arm kinematics reconstruction
  // th_x: linear regression
  // th_y: from hit positions
  // vtx_x: linear regression

  val D_x_L = - environment.optics.L_2_N.v_x * environment.optics.L_2_F.L_x + environment.optics.L_2_F.v_x * environment.optics.L_2_N.L_x
  val th_x_L  = (environment.optics.L_2_N.v_x * L_2_F.x - environment.optics.L_2_F.v_x * L_2_N.x) / D_x_L
  val vtx_x_L = (- L_2_N.x * environment.optics.L_2_F.L_x + L_2_F.x * environment.optics.L_2_N.L_x) / D_x_L

  val th_y_L_2_N = - L_2_N.y / environment.optics.L_2_N.L_y
  val th_y_L_2_F = - L_2_F.y / environment.optics.L_2_F.L_y
  val th_y_L = (th_y_L_2_N + th_y_L_2_F) / 2.0

  val D_y_L = - environment.optics.L_2_N.v_y * environment.optics.L_2_F.L_y + environment.optics.L_2_F.v_y * environment.optics.L_2_N.L_y
  val vtx_y_L = (- L_2_N.y * environment.optics.L_2_F.L_y + L_2_F.y * environment.optics.L_2_N.L_y) / D_y_L

  val leftArm = ArmReconstruction(th_x_L, th_y_L, vtx_x_L, vtx_y_L)

  val D_x_R = + environment.optics.R_2_N.v_x * environment.optics.R_2_F.L_x - environment.optics.R_2_F.v_x * environment.optics.R_2_N.L_x
  val th_x_R = (environment.optics.R_2_N.v_x * R_2_F.x - environment.optics.R_2_F.v_x * R_2_N.x) / D_x_R
  val vtx_x_R = (+ R_2_N.x * environment.optics.R_2_F.L_x - R_2_F.x * environment.optics.R_2_N.L_x) / D_x_R

  val th_y_R_2_N = + R_2_N.y / environment.optics.R_2_N.L_y
  val th_y_R_2_F = + R_2_F.y / environment.optics.R_2_F.L_y
  val th_y_R = (th_y_R_2_N + th_y_R_2_F) / 2.0

  val D_y_R = + environment.optics.R_2_N.v_y * environment.optics.R_2_F.L_y - environment.optics.R_2_F.v_y * environment.optics.R_2_N.L_y
  val vtx_y_R = (+ R_2_N.y * environment.optics.R_2_F.L_y - R_2_F.y * environment.optics.R_2_N.L_y) / D_y_R

  val rightArm = ArmReconstruction(th_x_R, th_y_R, vtx_x_R, vtx_y_R)

  // double-arm kinematics reconstruction
  // th_x: from hit positions, L-R average
  // th_y: from hit positions, L-R average
  // vtx_x: from hit positions, L-R average

  val th_x = (th_x_L + th_x_R) / 2.0
  val th_y = (th_y_L + th_y_R) / 2.0
   val vtx_x = (vtx_x_L + vtx_x_R) / 2.0
  val vtx_y = (vtx_y_L + vtx_y_R) / 2.0

  val doubleArm = ArmReconstruction(th_x, th_y, vtx_x, vtx_y)

  // theta reconstruction
  val th_sq = th_x*th_x + th_y*th_y
  val th = math.sqrt(th_sq)
  val phi = math.atan2(th_y, th_x)

  val theta = ThetaReconstruction(th, phi)

  // t reconstruction
  val t_x = environment.momentum.p*environment.momentum.p * th_x * th_x
  val t_y = environment.momentum.p*environment.momentum.p * th_y * th_y
  val t_ = t_x + t_y

  val t = TReconstruction(t_x, t_y, t_)

  Kinematics(leftArm, rightArm, doubleArm, theta, t)

})

