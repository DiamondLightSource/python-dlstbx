import copy
import errno
import json
import os
import re
import time
import timeit
import uuid

import workflows.recipe
from workflows.services.common_service import CommonService

# DCID(4983612).group.experiment_type == "SAD"


class DLSDispatcher(CommonService):
    """
    Single point of contact service that takes in job meta-information
    (say, a data collection ID), a processing recipe, a list of recipes,
    or pointers to recipes stored elsewhere, and mangles these into something
    that can be processed by downstream services.
    """

    # Human readable service name
    _service_name = "DLS Dispatcher"

    # Logger name
    _logger_name = "dlstbx.services.dispatcher"

    # Store a copy of all dispatch messages in this location
    _logbook = "/dls/tmp/zocalo/dispatcher"

    def initializing(self):
        """Subscribe to the processing_recipe queue. Received messages must be acknowledged."""
        # self._environment.get('live') can be used to distinguish live/test mode
        self.log.info("Dispatcher starting")
        self.recipe_basepath = "/dls_sw/apps/zocalo/live/recipes"

        if self._environment.get("live"):
            try:
                os.makedirs(self._logbook, 0o775)
            except OSError:
                pass  # Ignore if exists
            if not os.access(self._logbook, os.R_OK | os.W_OK | os.X_OK):
                self.log.error("Logbook disabled: Can not write to location")
                self._logbook = None
        else:
            self.log.info("Logbook disabled: Not running in live mode")
            self._logbook = None

        workflows.recipe.wrap_subscribe(
            self._transport,
            "processing_recipe",
            self.process,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def record_to_logbook(self, guid, header, original_message, message, recipewrap):
        basepath = os.path.join(self._logbook, time.strftime("%Y-%m"))
        clean_guid = re.sub(r"[^a-z0-9A-Z\-]+", "", guid, re.UNICODE)
        if not clean_guid or len(clean_guid) < 3:
            self.log.warning(
                "Message with non-conforming guid %s not written to logbook", guid
            )
            return
        try:
            os.makedirs(os.path.join(basepath, clean_guid[:2]))
        except OSError:
            pass  # Ignore if exists

        def neat_json_to_file(obj, fh, **kwargs):
            def _fix(item):
                if isinstance(item, list):
                    return [_fix(i) for i in item]
                if isinstance(item, dict):
                    return {str(key): _fix(value) for key, value in item.items()}
                return item

            return json.dump(
                _fix(obj),
                fh,
                sort_keys=True,
                skipkeys=True,
                default=str,
                indent=2,
                separators=(",", ": "),
                **kwargs
            )

        try:
            log_entry = os.path.join(basepath, clean_guid[:2], clean_guid[2:])
            with open(log_entry, "w") as fh:
                fh.write("Incoming message header:\n")
                neat_json_to_file(header, fh)
                fh.write("\n\nIncoming message body:\n")
                neat_json_to_file(original_message, fh)
                fh.write("\n\nParsed message body:\n")
                neat_json_to_file(message, fh)
                fh.write("\n\nRecipe object:\n")
                neat_json_to_file(
                    recipewrap.recipe.recipe,
                    fh,
                )
                fh.write("\n")
            self.log.debug("Message saved in logbook at %s", log_entry)
        except Exception:
            self.log.warning("Could not write message to logbook", exc_info=True)

    def process(self, rw, header, message):
        """Process an incoming processing request."""
        # Time execution
        start_time = timeit.default_timer()

        # Load processing parameters
        parameters = message.get("parameters", {})
        if not isinstance(parameters, dict):
            # malformed message
            self.log.error(
                "Dispatcher rejected malformed message: parameters not given as dictionary"
            )
            self._transport.nack(header)
            return

        # Unless 'guid' is already defined then generate a unique recipe IDs for
        # this request, which is attached to all downstream log records and can
        # be used to determine unique file paths.
        recipe_id = parameters.get("guid") or str(uuid.uuid4())
        parameters["guid"] = recipe_id

        if rw:
            # If we received a recipe wrapper then we already have a recipe_ID
            # attached to logs. Make a note of the downstream recipe ID so that
            # we can track execution beyond recipe boundaries.
            self.log.info(
                "Processing request with new recipe ID %s:\n%s", recipe_id, str(message)
            )

        # If we are fully logging requests then make a copy of the original message
        if self._logbook:
            original_message = copy.deepcopy(message)

        # From here on add the global ID to all log messages
        with self.extend_log("recipe_ID", recipe_id):
            self.log.debug("Received processing request:\n" + str(message))
            self.log.debug("Received processing parameters:\n" + str(parameters))

            # At this point external helper functions should be called,
            # eg. ISPyB database lookups
            import dlstbx.ispybtbx

            # Step 1: Check that parsing the message can proceed
            if not dlstbx.ispybtbx.ready_for_processing(message, parameters):
                # Message not yet cleared for processing
                if "dispatcher_expiration" not in parameters:
                    parameters["dispatcher_expiration"] = time.time() + int(
                        parameters.get("dispatcher_timeout", 120)
                    )
                if parameters["dispatcher_expiration"] > time.time():
                    # Wait for 2 seconds
                    txn = self._transport.transaction_begin()
                    self._transport.ack(header, transaction=txn)
                    self._transport.send(
                        "processing_recipe", message, transaction=txn, delay=2
                    )
                    self.log.info("Message not yet ready for processing")
                    self._transport.transaction_commit(txn)
                    return
                elif parameters.get("dispatcher_error_queue"):
                    # Drop message into error queue
                    txn = self._transport.transaction_begin()
                    self._transport.ack(header, transaction=txn)
                    self._transport.send(
                        parameters["dispatcher_error_queue"], message, transaction=txn
                    )
                    self.log.info(
                        "Message rejected to specified error queue as still not ready for processing"
                    )
                    self._transport.transaction_commit(txn)
                    return
                else:
                    # Unhandled error, send message to DLQ
                    self.log.error(
                        "Message rejected as still not ready for processing",
                    )
                    self._transport.nack(header)
                    return

            try:
                message, parameters = dlstbx.ispybtbx.ispyb_filter(message, parameters)
            except Exception as e:
                self.log.error(
                    "Rejected message due to ISPyB filter error: %s",
                    str(e),
                    exc_info=True,
                )
                self._transport.nack(header)
                return
            self.log.debug("Mangled processing request:\n" + str(message))
            self.log.debug("Mangled processing parameters:\n" + str(parameters))

            # Process message
            recipes = []
            if message.get("custom_recipe"):
                try:
                    recipes.append(
                        workflows.recipe.Recipe(
                            recipe=json.dumps(message["custom_recipe"])
                        )
                    )
                    self.log.info(
                        "Received message containing a custom recipe: %s",
                        message["custom_recipe"],
                    )
                except Exception as e:
                    self.log.error(
                        "Rejected message containing a custom recipe that caused parsing errors: %s",
                        str(e),
                        exc_info=True,
                    )
                    self._transport.nack(header)
                    return
            if message.get("recipes"):
                for recipefile in message["recipes"]:
                    try:
                        with open(
                            os.path.join(self.recipe_basepath, recipefile + ".json"),
                        ) as rcp:
                            recipes.append(workflows.recipe.Recipe(recipe=rcp.read()))
                    except ValueError as e:
                        self.log.error(
                            "Error reading recipe '%s': %s", recipefile, str(e)
                        )
                        self._transport.nack(header)
                        return
                    except OSError as e:
                        if e.errno == errno.ENOENT:
                            self.log.error(
                                "Message references non-existing recipe '%s'",
                                recipefile,
                            )
                            self._transport.nack(header)
                            return
                        raise

            if not recipes:
                self.log.error(
                    "Message contains no valid recipes or pointers to recipes"
                )
                self._transport.nack(header)
                return

            full_recipe = workflows.recipe.Recipe()
            for recipe in recipes:
                try:
                    recipe.validate()
                except workflows.Error as e:
                    self.log.error(
                        "Recipe failed validation. %s", str(e), exc_info=True
                    )
                    self._transport.nack(header)
                    return
                try:
                    recipe.apply_parameters(parameters)
                except Exception as e:
                    self.log.error(
                        "Failed to apply_parameters to recipe: %s",
                        str(e),
                        exc_info=True,
                    )
                    self._transport.nack(header)
                    return
                full_recipe = full_recipe.merge(recipe)

            # Conditionally acknowledge receipt of the message
            txn = self._transport.transaction_begin()
            self._transport.ack(header, transaction=txn)

            rw = workflows.recipe.RecipeWrapper(
                recipe=full_recipe, transport=self._transport
            )
            rw.environment = {
                "ID": recipe_id
            }  # FIXME: This should go into the constructor, but workflows can't do that yet
            rw.start(transaction=txn)

            # Write information to logbook if applicable
            if self._logbook:
                self.record_to_logbook(recipe_id, header, original_message, message, rw)

            # Commit transaction
            self._transport.transaction_commit(txn)
            self.log.info(
                "Processed incoming message in %.4f seconds",
                timeit.default_timer() - start_time,
            )
