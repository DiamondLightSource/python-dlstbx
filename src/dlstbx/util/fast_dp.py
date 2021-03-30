class Reader:
    def __init__(self, filename):

        fields = [
            ("Low resolution", "low_resolution", 3),
            ("High resolution", "high_resolution", 3),
            ("Rmerge", "rmerge", 3),
            ("I/sigma", "i_over_sigma", 3),
            ("Completeness", "completeness", 3),
            ("Multiplicity", "multiplicity", 3),
            ("CC 1/2", "cc_half", 3),
            ("Anom. Completeness", "anom_completeness", 3),
            ("Anom. Multiplicity", "anom_multiplicity", 3),
            ("Anom. Correlation", "anom_correlation", 3),
            ("Nrefl", "nrefl", 3),
            ("Nunique", "nunique", 3),
            ("Mid-slope", "mid_slope", 1),
            ("dF/F", "df_f", 1),
            ("dI/sig(dI)", "di_sig", 1),
        ]

        with open(filename) as infile:
            for line in infile.readlines():
                line = line.strip()
                for f in fields:
                    if line.startswith(f[0]):
                        tokens = line.split()
                        tokens = tokens[-f[2] :]
                        assert len(tokens) == f[2]
                        name = f[1]
                        if f[2] == 1:
                            value = float(tokens[0])
                        else:
                            value = map(float, tokens)
                        setattr(self, name, value)
