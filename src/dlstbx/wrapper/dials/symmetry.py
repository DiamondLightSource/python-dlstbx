from . import DialsWrapper


class SymmetryWrapper(DialsWrapper):

    executable = "dials.symmetry"

    def construct_commandline(self, params):
        return [self.executable] + [
            f"{param}={value}" for param, value in params["symmetry"].items()
        ]
