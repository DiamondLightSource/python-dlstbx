from . import DialsWrapper


class IntegrateWrapper(DialsWrapper):

    executable = "dials.integrate"

    def construct_commandline(self, params):
        return [self.executable] + [
            f"{param}={value}" for param, value in params["integrate"].items()
        ]
