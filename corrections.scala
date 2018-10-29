import org.apache.commons.math3.special.Erf
import scala.math._

case class Correction(phi_corr: Double = 0.0, div_corr:Double = 0.0, accepted: Boolean = false)

val calculateAcceptanceCorrections = udf((kinematics_th_y_R: Double, kinematics_th_y_L: Double, kinematics_th_y: Double, kinematics_th_x: Double, kinematics_theta_th: Double) => {

  //valid for 45b_56t
  val th_y_sign = 1
  val th_y_lcut_L = 30E-6; val th_y_lcut_R = 33.5E-6; val th_y_lcut = 34.5E-6
  val th_y_hcut_L = 102E-6; val th_y_hcut_R = 102E-6; val th_y_hcut = 100E-6
  // approximate (time independent) resolutions
  val si_th_y_1arm = 3.1E-6 / sqrt(2.0)

  if ((th_y_sign * kinematics_th_y_L < th_y_lcut_L) || (th_y_sign * kinematics_th_y_R < th_y_lcut_R) || (th_y_sign * kinematics_th_y_L > th_y_hcut_L) || (th_y_sign * kinematics_th_y_R > th_y_hcut_R)) Correction()

  else {
    val F_x = 1.0

    val th_y_abs = th_y_sign * kinematics_th_y

    val UB_y = min(th_y_hcut_R - th_y_abs, th_y_abs - th_y_lcut_L)
    val LB_y = max(th_y_lcut_R - th_y_abs, th_y_abs - th_y_hcut_L)
    val F_y = if (UB_y > LB_y) ( Erf.erf(UB_y / si_th_y_1arm) - Erf.erf(LB_y / si_th_y_1arm) ) / 2.0 else 0.0
    val div_corr = 1.0/(F_x * F_y);

    val th_x_lcut = -1.0	
    val th_x_hcut = +1.0
    if (kinematics_th_x <= th_x_lcut || kinematics_th_x >= th_x_hcut || th_y_abs <= th_y_lcut || th_y_abs >= th_y_hcut) Correction()
    
    else {

      // get all intersections
      val phis = collection.mutable.Buffer[Double]()

      if (kinematics_theta_th > th_y_lcut){
        val phi = math.asin(th_y_lcut / kinematics_theta_th)
        val ta_x = kinematics_theta_th * cos(phi)
        if (th_x_lcut < ta_x && ta_x < th_x_hcut) phis += phi
        if (th_x_lcut < -ta_x && -ta_x < th_x_hcut) phis += (Pi - phi)
      }

      if (kinematics_theta_th > th_y_hcut){
        val phi = asin(th_y_hcut / kinematics_theta_th);
        val ta_x = kinematics_theta_th * cos(phi);
        if (th_x_lcut < ta_x && ta_x < th_x_hcut) phis += phi
        if (th_x_lcut < -ta_x && -ta_x < th_x_hcut) phis += (Pi - phi)
      }

      if (kinematics_theta_th > abs(th_x_hcut)){
        val phi = acos(abs(th_x_hcut) / kinematics_theta_th);
        val ta_y = kinematics_theta_th * sin(phi)
        if (th_y_lcut < ta_y && ta_y < th_y_hcut) phis += phi
      }

      if (kinematics_theta_th > abs(th_x_lcut)){
        val phi = acos(abs(th_x_lcut) / kinematics_theta_th);
        val ta_y = kinematics_theta_th * sin(phi);
        if (th_y_lcut < ta_y && ta_y < th_y_hcut) phis += (Pi - phi)
      }

      // no intersection => no acceptances
      if (phis.size == 0) Correction(0.0, div_corr, false)
      
      else {
        // calculate arc-length in within acceptance
        //val phiSum = phis.foldLeft(0.0,-1)((acc, x) => (acc._1 + acc._2*x, acc._2 * -1))._1 // add even sub odd
        val phiSum = 1.0

        val phi_corr = 2.0 * Pi / phiSum

        Correction(phi_corr, div_corr, true)
      }
    }
  }
})
