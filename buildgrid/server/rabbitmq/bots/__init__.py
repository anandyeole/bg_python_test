# Copyright (C) 2021 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License' is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Bots service
============

The Bots service is responsible for assigning work to the various worker
(sometimes "bot") machines connected to the grid. These workers communicate
with the Bots service using the `Remote Workers API`_.

The Bots service gRPC servicer is :class:`.service.BotsService`, which
handles routing requests to the correct :class:`.instance.BotsInstance`
based on the instance name specified by the worker.

.. _Remote Workers API: https://github.com/googleapis/googleapis/tree/master/google/devtools/remoteworkers/v1test2

"""
