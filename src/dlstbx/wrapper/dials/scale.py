from . import DialsWrapper


class ScaleWrapper(DialsWrapper):

    executable = "dials.scale"

    def construct_commandline(self, params):
        return [self.executable] + [
            f"{param}={value}" for param, value in params["scale"].items()
        ]
