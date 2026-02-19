from __future__ import annotations

from pathlib import Path

import workflows.recipe
import yaml
from pydantic import BaseModel, Field, ValidationError
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement


def apply_limit(parameter: float, limits: tuple[float, float]) -> float:
    return max(limits[0], min(limits[1], parameter))


def scale_parameter(
    value: float, scale_factor: float, limits: tuple[float, float] | None = None
) -> tuple[float, float]:
    ref_value = value * scale_factor
    scaled_value = apply_limit(ref_value, limits) if limits else ref_value
    if scaled_value == 0:
        raise ValueError("Scaled value cannot be zero")
    inverse_scale_factor = ref_value / scaled_value
    return (scaled_value, inverse_scale_factor)


def get_resolution_scale(resolution: float) -> float:
    return resolution**2 - 0.4 * resolution + 0.5


def get_wavelength_scale(wavelength: float, default_wavelength: float) -> float:
    return (default_wavelength / wavelength) ** 2


def parse_agamemnon_recipe(recipe_path: Path) -> list[dict]:
    with open(recipe_path, "r") as f:
        recipe = yaml.safe_load(f)
    return recipe


class AgamemnonParameters(BaseModel):
    chi: float
    comment: str
    exposure_time: float = Field(gt=0)
    dose: float = Field(gt=0)
    kappa: float
    number_of_images: int = Field(gt=0)
    omega_increment: float = Field(gt=0)
    omega_overlap: float
    omega_start: float
    phi_increment: float
    phi_overlap: float
    phi_start: float
    scan_axis: str
    transmission: float = Field(gt=0)
    two_theta: float
    wavelength: float = Field(gt=0)


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
            rw.recipe_step["parameters"].get("ispyb_parameters", {}),
            substitutions=rw.environment,
        )
        self.log.info(f"Received parameters for strategy generation:\n{parameters}")
        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)

        beamline = (
            parameters["beamline"][0]
            if isinstance(parameters["beamline"], list)
            else parameters["beamline"]
        )
        wavelength = (
            float(parameters["wavelength"][0])
            if isinstance(parameters["wavelength"], list)
            else float(parameters["wavelength"])
        )
        resolution_estimate = (
            float(parameters["resolution"][0])
            if isinstance(parameters["resolution"], list)
            else float(parameters["resolution"])
        )
        resolution = max((resolution_estimate) - 0.5, 0.9)
        # TODO get limits from filesystem
        tranmission_limits = (0.0001, 1.0)
        exposure_time_limits = (0.001, 1.0)

        recipes = ("OSC.yaml", "Ligand binding.yaml")
        recipe_aliases = {"OSC.yaml": "OSC", "Ligand binding.yaml": "Ligand"}

        for recipe in recipes:
            ispyb_command_list = []
            recipe_path = Path(f"/dls_sw/{beamline}/etc/agamemnon-recipes/{recipe}")
            recipe_steps = parse_agamemnon_recipe(recipe_path)

            # Step 1: Create screeningOutput record for recipe, linked to the screeningId
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
                "program": f"udc-strategy: {recipe_aliases[recipe]}",
                "ispyb_command": "insert_screening_strategy",
                "screening_output_id": "$ispyb_screening_output_id",
                "store_result": "ispyb_screening_strategy_id",
            }
            ispyb_command_list.append(d)

            for n_step, recipe_step in enumerate(recipe_steps, start=1):
                try:
                    recipe_step = AgamemnonParameters(**recipe_step)
                except ValidationError as e:
                    self.log.error(f"Invalid recipe step in {recipe_path}: {e}")
                    # TODO handle this error - Send a message to ISPyB to log the failure and skip the rest of the recipe steps.
                    break
                scale = 1.0
                default_wavelength = recipe_step.wavelength
                scale *= get_wavelength_scale(wavelength, default_wavelength)
                scale *= get_resolution_scale(resolution)

                dose, _ = scale_parameter(recipe_step.dose, scale)

                rotation_axis = recipe_step.scan_axis
                rotation_start = recipe_step.__getattribute__(f"{rotation_axis}_start")
                rotation_increment = recipe_step.__getattribute__(
                    f"{rotation_axis}_increment"
                )
                transmission = recipe_step.transmission
                exposure_time = recipe_step.exposure_time

                # Runs twice to ensure that limits are applied correctly to both parameters, as they are interdependent - is this necessary?
                for _ in range(2):
                    if scale > 1.0:
                        transmission, scale = scale_parameter(
                            transmission, scale, limits=tranmission_limits
                        )
                        exposure_time, scale = scale_parameter(
                            exposure_time, scale, limits=exposure_time_limits
                        )
                    else:
                        exposure_time, scale = scale_parameter(
                            exposure_time, scale, exposure_time_limits
                        )
                        transmission, scale = scale_parameter(
                            transmission, scale, limits=tranmission_limits
                        )
                    self.log.debug(
                        f"Exposure time scaled to {exposure_time:.3f} s, transmission scaled to {transmission:.3f}, scale factor now {scale:.3f}"
                    )

                    # Step 3: Store screeningStrategyWedge results, linked to the screeningStrategyId
                    #         Keep the screeningStrategyWedgeId
                    d = {
                        "wedgenumber": n_step,
                        "resolution": resolution,
                        "phi": recipe_step.phi_start,
                        "chi": recipe_step.chi,
                        "kappa": recipe_step.kappa,
                        "wavelength": wavelength,
                        "dosetotal": dose,
                        "ispyb_command": "insert_screening_strategy_wedge",
                        "screening_strategy_id": "$ispyb_screening_strategy_id",
                        "store_result": f"ispyb_screening_strategy_wedge_id_{n_step}",
                    }
                    ispyb_command_list.append(d)

                    # Step 4: Store second screeningStrategySubWedge results, linked to the screeningStrategyWedgeId
                    #         Keep the screeningStrategyWedgeId
                    d = {
                        "subwedgenumber": 1,
                        "rotationaxis": recipe_step.scan_axis,
                        "axisstart": rotation_start,
                        "axisend": rotation_start
                        + rotation_increment * recipe_step.number_of_images,
                        "exposuretime": exposure_time,
                        "transmission": transmission,
                        "oscillationrange": rotation_increment,
                        "numberOfImages": recipe_step.number_of_images,
                        "resolution": resolution,
                        "ispyb_command": "insert_screening_strategy_sub_wedge",
                        "screening_strategy_wedge_id": f"$ispyb_screening_strategy_wedge_id_{n_step}",
                        "store_result": f"ispyb_screening_strategy_sub_wedge_id_{n_step}",
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
