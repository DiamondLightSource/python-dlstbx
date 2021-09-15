import time
from datetime import datetime
from typing import Optional, Union

import workflows
from sqlalchemy.dialects.mysql import insert
from workflows.services.common_service import CommonService

from dlstbx.prometheus_interface_tools import PrometheusInterface, get_sessionmaker


class DLSPromInterface(CommonService):
    """A service that receives information to be written to the Prometheus interface table."""

    # Human readable service name
    _service_name = "DLS Prometheus interface"

    # Logger name
    _logger_name = "dlstbx.services.prometheus_interface"

    def initializing(self) -> None:
        self.log.info("Prometheus interface starting")
        self._sessionmaker = get_sessionmaker()
        workflows.recipe.wrap_subscribe(
            self._transport,
            "prom_interface",
            self.receive_msg,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def receive_msg(
        self, rw: workflows.recipe.wrapper.RecipeWrapper, header: dict, message: dict
    ):
        if not rw:
            # Incoming message is not a recipe message. Simple messages can be valid
            if (
                not isinstance(message, dict)
                or not message.get("parameters")
                or not message.get("content")
            ):
                self.log.error("Rejected invalid simple message")
                self._transport.nack(header)
                return
            self.log.debug("Received a simple message")

            # Create a wrapper-like object that can be passed to functions
            # as if a recipe wrapper was present.
            class RW_mock:
                def dummy(self, *args, **kwargs):
                    pass

            rw = RW_mock()
            rw.transport = self._transport
            rw.recipe_step = {"parameters": message["parameters"]}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        params = {**rw.recipe_step["parameters"], **message}

        command = params.get("command")
        if command not in ["update", "reset"]:
            self.log.error(
                f"Received message does not contain a valid command, options are update or reset: {command}"
            )
            rw.transport.nack(header)
            return None
        metric_type = params.get("metric_type")
        if metric_type not in ["counter", "gauge", "histogram"]:
            self.log.error(f"Received metric type is not valid: {metric_type}")
            rw.transport.nack(header)
            return None
        validated = self._validate(metric_type, command, params)
        if not validated:
            self.log.error("Validation of message failed")
            rw.transport.nack(header)
            return None
        if command == "reset":
            self._reset(
                metric_type,
                params["metric_name"],
                self._parse_labels(params.get("metric_labels", {})),
                params["value"],
                params.get("timestamp"),
                params.get("metric_finished"),
            )
        elif command == "update":
            self._update(
                metric_type,
                params,
            )

    def _reset(
        self,
        mtype: str,
        name: str,
        labels: str,
        value: Union[float, int],
        timestamp: float,
        finished: bool,
    ) -> None:
        if finished is None:
            cluster_end_timestamp = None
        else:
            cluster_end_timestamp = datetime.fromtimestamp(time.time())
        insert_cmd = insert(PrometheusInterface).values(
            metric=name,
            metric_type=mtype,
            metric_labels=labels,
            metric_value=value,
            timestamp=datetime.fromtimestamp(timestamp),
            cluster_end_timestamp=cluster_end_timestamp,
        )
        update = insert_cmd.on_duplicate_key_update(
            metric_type=mtype,
            metric_value=value,
            timestamp=datetime.fromtimestamp(timestamp),
            cluster_end_timestamp=cluster_end_timestamp,
        )
        with self._sessionmaker() as session:
            session.execute(update)
            session.commit()

    def _update(self, mtype: str, params: dict) -> None:
        tstamp = params.get("timestamp") or datetime.fromtimestamp(time.time())
        if params.get("metric_finished"):
            endtstamp = datetime.fromtimestamp(time.time())
        else:
            endtstamp = None
        if mtype in ["counter", "gauge"]:
            self._update_simple(
                mtype,
                params["metric_name"],
                self._parse_labels(params.get("metric_labels", {})),
                params["value"],
                tstamp,
                endtstamp,
            )
        elif mtype == "histogram":
            self._update_hist(
                params["metric_name"],
                self._parse_labels(params.get("metric_labels", {})),
                params["value"],
                tstamp,
                endtstamp,
                params["bins"],
            )

    def _update_simple(
        self,
        mtype: str,
        name: str,
        labels: str,
        value: Union[float, int],
        timestamp: int,
        finished: Optional[bool],
    ) -> None:
        if finished is None:
            cluster_end_timestamp = None
        else:
            cluster_end_timestamp = datetime.fromtimestamp(time.time())
        insert_cmd = insert(PrometheusInterface).values(
            metric=name,
            metric_type=mtype,
            metric_labels=labels,
            metric_value=value,
            timestamp=datetime.fromtimestamp(timestamp),
            cluster_end_timestamp=cluster_end_timestamp,
        )
        update = insert_cmd.on_duplicate_key_update(
            metric_type=mtype,
            metric_value=PrometheusInterface.metric_value + value,
            timestamp=datetime.fromtimestamp(timestamp),
            cluster_end_timestamp=cluster_end_timestamp,
        )
        with self._sessionmaker() as session:
            session.execute(update)
            session.commit()

    def _update_hist(
        self,
        name: str,
        labels: dict,
        value: Union[float, int],
        timestamp: float,
        finished: Optional[bool],
        bins: list,
    ) -> None:
        captured = False
        for ul in bins:
            if value < ul and not captured:
                value_for_sum = value
                bin_value = 1
                captured = True
            else:
                bin_value = 0
            labelstring = self._labelstring({**labels, "le": ul})
            self._update_simple(
                "histogram",
                name + "_bucket",
                labelstring,
                bin_value,
                timestamp,
                finished,
            )
        if captured:
            bin_value = 0
        else:
            bin_value = 1
            value_for_sum = value
        self._update_simple(
            "histogram",
            name + "_bucket",
            self._parse_labels({**labels, "le": "+Inf"}),
            bin_value,
            timestamp,
            finished,
        )
        self._update_simple(
            "histogram",
            name + "_count",
            self._parse_labels(labels),
            1,
            timestamp,
            finished,
        )
        self._update_simple(
            "histogram",
            name + "_sum",
            self._parse_labels(labels),
            value_for_sum,
            timestamp,
            finished,
        )

    def _validate(self, mtype: str, command: str, params: dict) -> bool:
        validators = {
            "counter": self._val_counter,
            "gauge": self._val_gauge,
            "histogram": self._val_hist,
        }
        return validators[mtype](command, params)

    def _parameter_validation(self, params: dict) -> bool:
        if params.get("value") is None:
            self.log.error(
                "A value must be given in the parameters for a prometheus metric"
            )
            return False
        if params.get("metric_name") is None:
            self.log.error(
                "A value must be given in the parameters for a prometheus metric"
            )
            return False
        if not isinstance(params["value"], (int, float)):
            self.log.error(
                f"A value must be numeric for a prometheus metric: {params['value']}"
            )
            return False
        return True

    def _val_counter(self, command: str, params: dict) -> bool:
        if not self._parameter_validation(params):
            return False
        if command == "reset":
            if params["value"] != 0:
                self.log.error("A counter can only be reset to 0")
                return False
        else:
            if params["value"] < 0:
                self.log.error(
                    f"A counter metric must increase monotonically unless it is reset to 0: adding {params['value']} does not allow for this"
                )
                return False
        return True

    def _val_gauge(self, command: str, params: dict) -> bool:
        return self._parameter_validation(params)

    def _val_hist(self, command: str, params: dict) -> bool:
        if not self._parameter_validation(params):
            return False
        if command == "reset":
            self.log.error("reset command is not supported for metric type histogram")
            return False
        if not params["bins"]:
            self.log.error(
                "Bin upper values must be specified for metric type histogram"
            )
            return False
        return True

    @staticmethod
    def _parse_labels(labels: dict) -> str:
        as_str = ""
        for l in labels.keys():
            if labels.get(l) is not None:
                as_str += f'{l}="{labels[l]}",'
        return as_str[:-1]
