import iotbx.merging_statistics

def get_merging_statistics(scaled_unmerged_mtz):
  i_obs = iotbx.merging_statistics.select_data(scaled_unmerged_mtz, data_labels=None)
  i_obs = i_obs.customized_copy(anomalous_flag=True, info=i_obs.info())
  result = iotbx.merging_statistics.dataset_statistics(
    i_obs=i_obs,
    n_bins=20,
    anomalous=False,
    use_internal_variance=False,
    eliminate_sys_absent=False,
    assert_is_not_unique_set_under_symmetry=False)
  return result

