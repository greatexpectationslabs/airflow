#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
import great_expectations as ge

log = logging.getLogger(__name__)


class GreatExpectationsOperator(BaseOperator):
    """
        This is the base operator for all Great Expectations operators.

        TODO: Update doc string
    """


    @apply_defaults
    def __init__(self,
                 *,
                 run_id=None,
                 data_context_root_dir=None,
                 data_context=None,
                 expectation_suite_name=None,
                 batch_kwargs=None,
                 assets_to_validate=None,
                 checkpoint_name=None,
                 fail_task=True,
                 validation_operator_name="action_list_operator",
                 **kwargs
        ):
        super().__init__(**kwargs)

        self.run_id = run_id

        if data_context_root_dir and data_context:
            raise ValueError("Only one of data_context_root_dir or data_context should be used.")

        if data_context:
            self.data_context = data_context
        elif data_context_root_dir:
            self.data_context = ge.DataContext(data_context_root_dir)
        else:
            self.data_context = ge.DataContext()

        # TODO: check that only one of the following is passed
        self.expectation_suite_name = expectation_suite_name
        self.batch_kwargs = batch_kwargs
        self.assets_to_validate = assets_to_validate
        self.checkpoint_name = checkpoint_name

        self.fail_task = fail_task

        self.validation_operator_name = validation_operator_name

    def execute(self, context):
        log.info("Running validation with Great Expectations")

        batches_to_validate = []
        validation_operator_name = self.validation_operator_name

        if self.batch_kwargs and self.expectation_suite_name:
            batch = self.data_context.get_batch(self.batch_kwargs, self.expectation_suite_name)
            batches_to_validate.append(batch)

        elif self.checkpoint_name:
            checkpoint = self.data_context.get_checkpoint(self.checkpoint_name)
            validation_operator_name = checkpoint["validation_operator_name"]

            for batch in checkpoint["batches"]:
                batch_kwargs = batch["batch_kwargs"]
                for suite_name in batch["expectation_suite_names"]:
                    suite = self.data_context.get_expectation_suite(suite_name)
                    batch = self.data_context.get_batch(batch_kwargs, suite)
                    batches_to_validate.append(batch)

        elif self.assets_to_validate:
            for asset in self.assets_to_validate:
                batch = self.data_context.get_batch(
                    asset["batch_kwargs"],
                    asset["expectation_suite_name"]
                )
                batches_to_validate.append(batch)

        results = self.data_context.run_validation_operator(
            validation_operator_name,
            assets_to_validate=batches_to_validate,
            run_id=self.run_id
        )

        if not results["success"]:
            if self.fail_task:
                raise AirflowException('Great Expectations validation failed')
            else:
                log.warning("Validation with Great Expectation failed.")
