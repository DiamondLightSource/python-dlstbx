from . import DialsWrapper


class IndexWrapper(DialsWrapper):

    executable = "dials.index"

    def construct_commandline(self, params):
        return [self.executable] + [
            f"{param}={value}" for param, value in params["index"].items()
        ]
