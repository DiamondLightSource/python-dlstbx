from . import DialsWrapper


class RefineWrapper(DialsWrapper):

    executable = "dials.refine"

    def construct_commandline(self, params):
        return [self.executable] + [
            f"{param}={value}" for param, value in params["refine"].items()
        ]
