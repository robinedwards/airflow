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
import unittest
from unittest import mock

from airflow.task.task_runner import get_task_runner
from airflow.configuration import conf


class CustomTaskRunner:
    def __init__(self, local_task_job):
        self.local_task_job = local_task_job


class TestCustomTaskRunner(unittest.TestCase):

    def test_custom_task_runner_returned(self):
        """
        This test ensures that specifying a custom runner in the config is returned by get_task_runner
        """
        local_task_job = mock.Mock()
        conf.set('core', 'task_runner', 'tests.task.task_runner.test_custom_task_runner.CustomTaskRunner')
        runner = get_task_runner(local_task_job)
        self.assertTrue(isinstance(runner, CustomTaskRunner))
        self.assertTrue(local_task_job is runner.local_task_job)
