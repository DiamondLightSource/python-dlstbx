import mock
import ispyb
import pytest
import dlstbx.services.ispybsvc_em as em


def parameters(arg):
    return "parameters:" + arg


@pytest.mark.skip("EM methods are not yet implemented")
def test_do_insert_ctf_handles_ispyb_error():
    dls = em.EM_Mixin()
    dls.ispyb = mock.Mock()
    dls.ispyb.em_acquisition.insert_ctf.side_effect = ispyb.ISPyBException()
    dls.log = mock.Mock()

    assert dls.do_insert_ctf(parameters) is False
    assert dls.log.error.called


@pytest.mark.skip("EM methods are not yet implemented")
def test_insert_ctf_is_called_with_parameters():
    dls = em.EM_Mixin()
    dls.ispyb = mock.Mock()
    dls.ispyb.em_acquisition.insert_ctf = mock.Mock()
    return_test = dls.do_insert_ctf(parameters)
    assert return_test == {
        "success": True,
        "return_value": dls.ispyb.em_acquisition.insert_ctf.return_value,
    }
    dls.ispyb.em_acquisition.insert_ctf.assert_called_once_with(
        ctf_id=parameters("ctf_id"),
        motion_correction_id=1234,
        auto_proc_program_id=parameters("auto_proc_program_id"),
        box_size_x=parameters("box_size_x"),
        box_size_y=parameters("box_size_y"),
        min_resolution=parameters("min_resolution"),
        max_resolution=parameters("max_resolution"),
        min_defocus=parameters("min_defocus"),
        max_defocus=parameters("max_defocus"),
        astigmatism=parameters("astigmatism"),
        defocus_step_size=parameters("defocus_step_size"),
        astigmatism_angle=parameters("astigmatism_angle"),
        estimated_resolution=parameters("estimated_resolution"),
        estimated_defocus=parameters("estimated_defocus"),
        amplitude_contrast=parameters("amplitude_contrast"),
        cc_value=parameters("cc_value"),
        fft_theoretical_full_path=parameters("fft_theoretical_full_path"),
        comments=parameters("comments"),
    )
