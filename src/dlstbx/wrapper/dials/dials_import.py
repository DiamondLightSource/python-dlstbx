from . import DialsWrapper


class ImportWrapper(DialsWrapper):

    executable = "dials.import"

    def construct_commandline(self, params):
        command = [self.executable]
        for param, value in params["import"].items():
            if param == "images":
                if not isinstance(value, (list, tuple)):
                    value = [value]
                command.extend(value)
            else:
                command.append(f"{param}={value}")
        return command
