import math
import sys


def pilatus_settings_check(filename):
    with open(filename) as fh:
        header_text = fh.read(1000)
    assert header_text.startswith("###CBF")

    wavelength = 0
    threshold = 0

    wavelength_to_energy = 12398.0

    for record in header_text.split("\n"):
        if record.startswith("# Wavelength"):
            wavelength = float(record.split()[-2])
        if record.startswith("# Threshold_setting"):
            threshold = float(record.split()[-2])

    assert wavelength > 0, wavelength
    assert threshold > 0, threshold

    threshold_ratio = threshold / (wavelength_to_energy / wavelength)

    if math.fabs(threshold_ratio - 0.5) > 0.05:
        sys.exit(
            "Threshold incorrect: is %.1f should be %.1f"
            % (threshold, 0.5 * (wavelength_to_energy / wavelength))
        )


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        pilatus_settings_check(arg)
