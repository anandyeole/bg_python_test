# Copyright (C) 2018 Bloomberg LP
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


import asyncio
import logging


class Bot:
    """Creates a local BotSession."""

    def __init__(self, bot_session):
        self.__logger = logging.getLogger(__name__)

        self.__bot_session = bot_session

        self.__loop = None

    def session(self):
        """Will create a session and periodically call the server."""

        self.__loop = asyncio.get_event_loop()

        try:
            task = asyncio.ensure_future(self.__bot_session.run())
            self.__loop.run_until_complete(task)
        except KeyboardInterrupt:
            pass

        self.__kill_everyone()
        self.__logger.info("Bot shutdown.")

    def __kill_everyone(self):
        """Cancels and waits for them to stop."""
        self.__logger.info("Cancelling remaining tasks...")
        if self.__loop:
            for task in asyncio.all_tasks(loop=self.__loop):
                task.cancel()
                self.__loop.run_until_complete(task)
