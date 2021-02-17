from . import DialsWrapper


class FindSpotsWrapper(DialsWrapper):

    executable = "dials.find_spots"

    def construct_commandline(self, params):
        return [self.executable] + [
            f"{param}={value}" for param, value in params["find_spots"].items()
        ]
