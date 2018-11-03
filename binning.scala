// binnings: "ub", "ob-1-10-0.2", "ob-1-30-0.2"

val t_max_full = 4.0
val step = 2E-3

val N_bins_ub = (t_max_full / step).toInt
val bin_lower_ub = 0.0
val bin_upper_ub = N_bins_ub * step

//TODO binning -> open generatrs.root etc

val N_bins_ob_1_10_02 = (t_max_full / step).toInt
val bin_lower_ob_1_10_02 = 0.0
val bin_upper_ob_1_10_02 = N_bins_ub * step

val N_bins_ob_1_30_02 = (t_max_full / step).toInt
val bin_lower_ob_1_30_02 = 0.0 
val bin_upper_ob_1_30_02 = N_bins_ub * step
