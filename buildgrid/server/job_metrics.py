# Copyright (C) 2021 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, Dict

from buildgrid._enums import LeaseState, OperationStage
from buildgrid.server.metrics_utils import publish_counter_metric
from buildgrid.server.metrics_names import (JOB_CHANGES_COUNTER_METRIC_NAME,
                                            LEASE_CHANGES_COUNTER_METRIC_NAME)


class JobMetrics():
    @staticmethod
    def publish_metrics_on_job_updates(initial_values_of_interest: Dict,
                                       changes: Dict,
                                       instance_name: Optional[str]):
        if "stage" in changes:
            new_stage = OperationStage(changes["stage"])
            old_stage = None
            if "stage" in initial_values_of_interest:
                old_stage = OperationStage(initial_values_of_interest["stage"])

            JobMetrics._publish_job_stage_transition_metric_nowait(
                new_stage=new_stage,
                old_stage=old_stage,
                instance_name=instance_name)

    @staticmethod
    def publish_metrics_on_lease_updates(initial_values_of_interest: Dict,
                                         changes: Dict,
                                         instance_name: Optional[str]):
        if "state" in changes:
            new_state = LeaseState(changes["state"])
            old_state = None
            if "state" in initial_values_of_interest:
                old_state = LeaseState(initial_values_of_interest["state"])

            JobMetrics._publish_lease_state_transition_metric_nowait(
                new_state=new_state,
                old_state=old_state,
                instance_name=instance_name)

    @staticmethod
    def _publish_job_stage_transition_metric_nowait(
            new_stage: OperationStage,
            old_stage: Optional[OperationStage] = None,
            instance_name: Optional[str] = None):
        if not instance_name:
            instance_name = ""
        # Publish record for new stage
        publish_counter_metric(
            JOB_CHANGES_COUNTER_METRIC_NAME, 1, {
                "instance-name": instance_name,
                "operation-stage": new_stage.name,
                "statsd-bucket": new_stage.name
            })
        # If we're given prior stage, indicate that transition too
        if old_stage and old_stage != new_stage:
            publish_counter_metric(
                JOB_CHANGES_COUNTER_METRIC_NAME, -1, {
                    "instance-name": instance_name,
                    "operation-stage": old_stage.name,
                    "statsd-bucket": old_stage.name
                })

    @staticmethod
    def _publish_lease_state_transition_metric_nowait(
            new_state: LeaseState,
            old_state: Optional[LeaseState] = None,
            instance_name: Optional[str] = None):
        if not instance_name:
            instance_name = ""
        # Publish record for new state
        publish_counter_metric(
            LEASE_CHANGES_COUNTER_METRIC_NAME, 1, {
                "instance-name": instance_name,
                "operation-stage": new_state.name,
                "statsd-bucket": new_state.name
            })
        # If we're given prior state, indicate that transition too
        if old_state and old_state != new_state:
            publish_counter_metric(
                LEASE_CHANGES_COUNTER_METRIC_NAME, -1, {
                    "instance-name": instance_name,
                    "operation-stage": old_state.name,
                    "statsd-bucket": old_state.name
                })
