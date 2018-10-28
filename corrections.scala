import org.apache.commons.math3.special.Erf
import scala.math._

case class Correction(phi_corr = 0.0: Double, div_corr = 0.0: Double, accepted: Boolean)

val calculateAcceptanceCorrections = udf((kinematics: Kinematics) => {
  
  //valid for 45b_56t
  val th_y_sign = 1
  val th_y_lcut_L = 30E-6; val th_y_lcut_R = 33.5E-6; val th_y_lcut = 34.5E-6
  val th_y_hcut_L = 102E-6; val th_y_hcut_R = 102E-6; val th_y_hcut = 100E-6
  // approximate (time independent) resolutions
  val si_th_y_1arm = 3.1E-6 / sqrt(2.0)

  if ((th_y_sign * kinematics.leftArm.th_y < th_y_lcut_L) || (th_y_sign * kinematics.rightArm.th_y < th_y_lcut_R) || (th_y_sign * kinematics.leftArm.th_y > th_y_hcut_L) || (th_y_sign * kinematics.rightArm.th_y > th_y_hcut_R)) return Correction(false)


  val F_x = 1.0

  val th_y_abs = th_y_sign * kinematics.doubleArm.th_y

  val UB_y = min(th_y_hcut_R - th_y_abs, th_y_abs - th_y_lcut_L)
  val LB_y = max(th_y_lcut_R - th_y_abs, th_y_abs - th_y_hcut_L)
  val F_y = if (UB_y > LB_y) then ( Erf.erf(UB_y / si_th_y_1arm) - Erf.erf(LB_y / si_th_y_1arm) ) / 2.0 else 0.0
  val div_corr = 1.0/(F_x * F_y);

    
  if (kinematics.doubleArm.th_x <= th_x_lcut || kinematics.doubleArm.th_x >= th_x_hcut || th_y_abs <= th_y_lcut || th_y_abs >= th_y_hcut) return Correction(false)

  // get all intersections
  val phis = collection.mutable.List[Double]()

  if (kinematics.theta.th > th_y_lcut){
    val phi = math.asin(th_y_lcut / k.th)
    val ta_x = kinematics.theta.th * cos(phi)
    if (th_x_lcut < ta_x && ta_x < th_x_hcut) phis += phi
    if (th_x_lcut < -ta_x && -ta_x < th_x_hcut) phis += (Pi - phi)
  }

  if (kinematics.theta.th > th_y_hcut){
    val phi = asin(th_y_hcut / k.th);
    val ta_x = kinematics.theta.th * cos(phi);
    if (th_x_lcut < ta_x && ta_x < th_x_hcut) phis += phi
    if (th_x_lcut < -ta_x && -ta_x < th_x_hcut) phis += (Pi - phi)
  }

  if (kinematics.theta.th > fabs(th_x_hcut)){
    val phi = acos(fabs(th_x_hcut) / k.th);
    val ta_y = kinematics.theta.th * sin(phi)
    if (th_y_lcut < ta_y && ta_y < th_y_hcut) phis += phi
  }

  if (kinematics.theta.th > fabs(th_x_lcut)){
    val phi = acos(fabs(th_x_lcut) / k.th);
    val ta_y = kinematics.theta.th * sin(phi);
    if (th_y_lcut < ta_y && ta_y < th_y_hcut) phis += (Pi - phi)
  }

  // no intersection => no acceptances
  if (phis.size == 0) Correction(0.0, div_corr, false)

  // calculate arc-length in within acceptance
  val phiSum = 0.0
  val phiSum = phis.foldLeft(0.0,-1)(((acc, mul), x) => (acc + mul*x, mul * -1)).first // add even sub odd

  val phi_corr = 2.0 * Pi / phiSum

  return Correction(phi_corr, div_corr, true)
}
