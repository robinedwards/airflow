# -*- coding: utf-8 -*-
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

# pylint: disable=missing-docstring
import importlib

from airflow.configuration import conf
from airflow.task.task_runner.standard_task_runner import StandardTaskRunner


def get_task_runner(local_task_job):
    """
    Get the task runner that can be used to run the given job.

    :param local_task_job: The LocalTaskJob associated with the TaskInstance
        that needs to be executed.
    :type local_task_job: airflow.jobs.LocalTaskJob
    :return: The task runner to use to run the task.
    :rtype: airflow.task.task_runner.base_task_runner.BaseTaskRunner
    """
    _task_runner = conf.get('core', 'TASK_RUNNER')

    if _task_runner == "StandardTaskRunner":
        return StandardTaskRunner(local_task_job)
    elif _task_runner == "CgroupTaskRunner":
        from airflow.task.task_runner.cgroup_task_runner import CgroupTaskRunner
        return CgroupTaskRunner(local_task_job)
    else:
        path, cls_name = _task_runner.rsplit('.', 1)
        module = importlib.import_module(path)
        runner_cls = getattr(module, cls_name)
        return runner_cls(local_task_job)
