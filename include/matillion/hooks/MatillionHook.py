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

import time
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
# from tenacity import tenacity


class MatillionHook(HttpHook):

    conn_name_attr = "matillion"
    #default_conn_name = "airbyte_default"
    #conn_type = "airbyte"
    #hook_name = "Airbyte"

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    QUEUED = "QUEUED"
    FAILED = "FAILED"
    #ERROR = "error"
    #INCOMPLETE = "incomplete"

    def __init__(self, group_name: str, project_name: str, job_name: str, environment_Name: str, matillion_conn_id: str = "Matillion") -> None:
        super().__init__(http_conn_id=matillion_conn_id)
        
        self.group_name: str = group_name
        self.project_name: str = project_name
        self.job_name: str = job_name
        self.environment_Name: str = environment_Name
        self.extra_options={"verify": False,"timeout": 120}

    def submit_sync_connection(self) -> Any:
        """
        Submit a job to a Matillion server.

        :param connection_id: Required. The ConnectionId of the Matillion Connection.
        """
        self.method = "POST"
        enpoint = f"rest/v1/group/name/" + self.group_name + "/project/name/" + self.project_name + "/version/name/default/job/name/" + self.job_name + "/run?environmentName=" + self.environment_Name
        self.log.info("Submiting sync connection:" + enpoint)
        job = self.run(
            endpoint=enpoint,
            headers={
                "accept": "application/json",
                'X-Requested-By': 'airflow',
                'Content-Type': 'application/json'                     
            },
            extra_options=self.extra_options,
        )

        self.log.info("submit_sync_connection result: " + str(job.json()))

        return job

    def wait_for_job(self, job_id: str | int, wait_seconds: float = 3, timeout: float | None = 14400) -> None:
        """
        Poll a job to check if it finishes.

        :param job_id: Required. Id of the Matillion job
        :param wait_seconds: Optional. Number of seconds between checks.
        :param timeout: Optional. How many seconds wait for job to be ready.
            Used only if ``asynchronous`` is False.
        """
        state = None
        start = time.monotonic()
        while True:
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(f"Timeout: Matillion job {job_id} is not ready after {timeout}s")
            time.sleep(wait_seconds)
            try:
                job = self.get_job(job_id=(int(job_id)))
                state = job.json()["state"]
                self.log.info("polling job " + str(job_id) + " for status: " + state)
            except AirflowException as err:
                self.log.info("Retrying. Matillion API returned server error when waiting for job: %s", err)
                continue

            if state in (self.RUNNING, self.QUEUED):
                continue
            if state == self.SUCCESS:
                break
            if state in (self.CANCELLING, self.CANCELLED):
                raise AirflowException(f"Matillion Job has been Canceled:\n{job}")    
            if(state == self.FAILED):
                raise AirflowException(f"Matillion Job has Failed:\n{job}")    
            else:
                raise AirflowException(f"Job failed:\n{job}")

    def get_job(self, job_id: int) -> Any:
        """
        Get the resource representation for a job in Matillion.

        :param job_id: Required. Id of the Airbyte job
        """
        self.method = "GET"
        job = self.run(
            endpoint=f"rest/v1/group/name/" + self.group_name + "/project/name/" + self.project_name + "/task/id/" + str(job_id),
            headers={
                "accept": "application/json",
                'X-Requested-By': 'airflow',
                'Content-Type': 'application/json'                     
            },
            extra_options=self.extra_options,
        )
    
        self.log.info("job: " + str(job.json()))
        return job

    def cancel_job(self, job_id: int) -> Any:
        """
        Cancel the job when task is cancelled.

        :param job_id: Required. Id of the Airbyte job
        """
        self.method = "POST"
        job = self.run(
            endpoint=f"rest/v1/group/name/" + self.group_name + "/project/name/" + self.project_name + "/task/id/" + str(job_id) + "/cancel",
            headers={
                "accept": "application/json",
                'X-Requested-By': 'airflow',
                'Content-Type': 'application/json'                     
            },
            extra_options=self.extra_options,
        )

        self.log.info("job canceled in Airflow, canceling in Matillion: " + str(job.json()))

        return job