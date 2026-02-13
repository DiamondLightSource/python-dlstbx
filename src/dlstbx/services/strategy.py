from __future__ import annotations

import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement


def apply_limit(parameter: float, limits: tuple[float, float]) -> float:
    return max(limits[0], min(limits[1], parameter))


def scale_parameter(value: float, scale_factor: float, limits) -> tuple[float, float]:
    ref_value = value * scale_factor
    scaled_value = apply_limit(ref_value, limits)
    if scaled_value == 0:
        raise ValueError("Scaled value cannot be zero")
    inverse_scale_factor = ref_value / scaled_value
    return (scaled_value, inverse_scale_factor)


def get_resolution_scale(resolution: float) -> float:
    return resolution**2 - 0.4 * resolution + 0.5


def get_wavelength_scale(wavelength: float, default_wavelength: float) -> float:
    return (default_wavelength / wavelength) ** 2


class DLSStrategy(CommonService):
    """Service for creating data collection strategies."""

    # Human readable service name
    _service_name = "Strategy"

    # Logger name
    _logger_name = "dlstbx.services.strategy"

    def initializing(self):
        """Subscribe to channel."""
        self.log.info("Strategy service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "strategy",
            self.generate_strategy,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def generate_strategy(
        self, rw: workflows.recipe.RecipeWrapper, header: dict, message: dict
    ):
        """Generate a strategy from the results of an upstream pipeline"""
        self.log.info("Received strategy request, generating strategy")
        parameters = ChainMapWithReplacement(
            message.get("parameters", {}) if isinstance(message, dict) else {},
            rw.recipe_step.get("parameters", {}),
            substitutions=rw.environment,
        )
        self.log.info(f"Received parameters for strategy generation:\n{parameters}")
        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)

        wavelength = parameters["wavelength"]
        default_wavelength = parameters["default_wavelength"]
        resolution = max(parameters["resolution"] - 0.5, 0.9)
        scale = 1.0
        scale *= get_wavelength_scale(wavelength, default_wavelength)
        self.log.info(f"Scale factor from wavelength: {scale:.3f}")
        scale *= get_resolution_scale(resolution)
        self.log.info(f"Scale factor from resolution: {scale:.3f}")

        tranmission_limits = (0.0, 1.0)
        exposure_time_limits = (0.01, 1.0)
        transmission = 0.1
        exposure_time = 0.1

        # Runs twice to ensure that limits are applied correctly to both parameters, as they are interdependent - is this necessary?
        for _ in range(2):
            if scale > 1.0:
                transmission, scale = scale_parameter(
                    transmission, scale, tranmission_limits
                )
                exposure_time, scale = scale_parameter(
                    exposure_time, scale, exposure_time_limits
                )
            else:
                exposure_time, scale = scale_parameter(
                    exposure_time, scale, exposure_time_limits
                )
                transmission, scale = scale_parameter(
                    transmission, scale, tranmission_limits
                )
            self.log.info(
                f"Exposure time scaled to {exposure_time:.3f} s, transmission scaled to {transmission:.3f}, scale factor now {scale:.3f}"
            )

        ispyb_command_list = []

        # Step 1: Store screeningOutput results, linked to the screeningId
        #         Keep the screeningOutputId
        d = {
            "program": "udc-strategy",
            "strategysuccess": 1,
            "ispyb_command": "insert_screening_output",
            "screening_id": "$ispyb_screening_id",
            "store_result": "ispyb_screening_output_id",
        }
        ispyb_command_list.append(d)

        # Step 2: Store screeningStrategy results, linked to the screeningOutputId
        #         Keep the screeningStrategyId
        d = {
            "program": "udc-strategy",
            "ispyb_command": "insert_screening_strategy",
            "screening_output_id": "$ispyb_screening_output_id",
            "store_result": "ispyb_screening_strategy_id",
        }
        ispyb_command_list.append(d)

        # Step 3: Store screeningStrategyWedge results, linked to the screeningStrategyId
        #         Keep the screeningStrategyWedgeId
        d = {
            "wedgenumber": 1,
            "resolution": resolution,
            "phi": 0.0,
            "chi": 0.0,
            "ispyb_command": "insert_screening_strategy_wedge",
            "screening_strategy_id": "$ispyb_screening_strategy_id",
            "store_result": "ispyb_screening_strategy_wedge_id",
        }
        ispyb_command_list.append(d)

        # Step 4: Store second screeningStrategySubWedge results, linked to the screeningStrategyWedgeId
        #         Keep the screeningStrategyWedgeId
        d = {
            "subwedgenumber": 1,
            "rotationaxis": "Omega",
            "axisstart": 0.0,
            "axisend": 360.0,
            "exposuretime": exposure_time,
            "transmission": transmission,
            "oscillationrange": 0.1,
            "numberOfImages": 3600,
            "resolution": resolution,
            "chi": 0.0,
            "ispyb_command": "insert_screening_strategy_sub_wedge",
            "screening_strategy_wedge_id": "$ispyb_screening_strategy_wedge_id",
            "store_result": "ispyb_screening_strategy_sub_wedge_id",
        }
        ispyb_command_list.append(d)

        d = {
            "ispyb_command": "update_processing_status",
            "program_id": "$ispyb_autoprocprogram_id",
            "message": "Processing successful",
            "status": "success",
        }
        ispyb_command_list.append(d)

        # Send results onwards
        rw.set_default_channel("ispyb")
        rw.send_to("ispyb", {"ispyb_command_list": ispyb_command_list}, transaction=txn)
        self.log.info(f"Sent {len(ispyb_command_list)} commands to ISPyB")
        self.log.debug(f"Commands sent to ISPyB:\n{ispyb_command_list}")

        # Commit transaction
        self._transport.transaction_commit(txn)
        self.log.info("Strategy generation complete")
