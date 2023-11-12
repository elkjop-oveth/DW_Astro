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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from include.matillion.hooks.MatillionHook import MatillionHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

class MatillionTriggerSyncOperator(BaseOperator):
    """
    Submits a job to an Matillion server to run a integration process between your source and destination.

    :param wait_seconds: Optional. Number of seconds between checks.
        Defaults to 10 seconds.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Defaults to 14400 seconds (or 1 hour).
    """

    #template_fields: Sequence[str] = ("connection_id",)

    def __init__(
        self,
        task_id: str,
        group_name: str,
        project_name: str,
        job_name: str,
        environment_name: str,
        matillion_conn_id: str = "Matillion",
        wait_seconds: float = 3,
        timeout: float = 3600,
        **kwargs,
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.matillion_conn_id = matillion_conn_id
        self.group_name = group_name
        self.project_name = project_name
        self.job_name = job_name
        self.environment_name = environment_name
        self.timeout = timeout
        self.wait_seconds = wait_seconds

    def execute(self, context: Context) -> None:
        """Create Matillion Job and wait to finish."""
        self.hook = MatillionHook(group_name=self.group_name, project_name=self.project_name, job_name=self.job_name, environment_Name=self.environment_name, matillion_conn_id=self.matillion_conn_id)
        job_object = self.hook.submit_sync_connection()
        self.job_id = job_object.json()["id"]

        self.log.info("Job %s was submitted to Matillion Server", self.job_id)
        self.log.info("Waiting for job %s to complete", self.job_id)
        self.hook.wait_for_job(job_id=self.job_id, wait_seconds=self.wait_seconds, timeout=self.timeout)
        self.log.info("Job %s completed successfully", self.job_id)

        return self.job_id

    def on_kill(self):
        """Cancel the job if task is cancelled."""
        if self.job_id:
            self.log.info("on_kill: cancel the airbyte Job %s", self.job_id)
            self.hook.cancel_job(self.job_id)