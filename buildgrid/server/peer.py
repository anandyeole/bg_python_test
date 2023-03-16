# Copyright (C) 2019 Bloomberg LP
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

import logging
from threading import Lock


class Peer:
    """Represents a client during a session."""

    # We will keep a global list of Peers indexed by their `Peer.uid`.
    __peers_by_uid = {}  # type: ignore
    __peers_by_uid_lock = Lock()

    @classmethod
    def register_peer(cls, uid, context, token=None):
        """Registers a new peer from a given context.

        Args:
            uid (str): a unique identifier
            token (str): an authentication token (optional)
            context (grpc.ServicerContext): context in which the peer is
                being registered

        Returns:
            Peer: an existing or newly created Peer object
        """

        with cls.__peers_by_uid_lock:
            # If the peer exists, we just increment the counter on the existing
            # instance:
            if uid in cls.__peers_by_uid:
                existing_peer = cls.__peers_by_uid[uid]
                logging.getLogger(__name__).debug('Registering another instance '
                                                  f'of Peer with uid {uid} ')
                existing_peer.__instance_count += 1
                return existing_peer
            else:
                # Otherwise we add ourselves to the list of Peers:
                new_peer = Peer(uid=uid, token=token)
                cls.__peers_by_uid[uid] = new_peer
                return cls.__peers_by_uid[uid]

    def __init__(self, uid, token=None, tool_name=None, tool_version=None):
        """Creates a new Peer object.

        Args:
            uid (str): a unique identifier
            token (str): an authentication token (optional)
        """
        self._uid = uid  # This uniquely identifies a client
        self._token = token

        # Each Peer object contains the number of instances of itself:
        self.__instance_count = 1

    @classmethod
    def find_peer(cls, uid):
        return cls.__peers_by_uid.get(uid, None)

    def __eq__(self, other):
        if not isinstance(other, Peer):
            return False

        return self.uid == other.uid and self.token == other.token

    def __hash__(self):
        return hash(self.uid)  # This string is unique for each peer

    def __str__(self):
        return f'Peer: uid: {self._uid}'

    @property
    def uid(self):
        return self._uid

    @property
    def token(self):
        return self._token

    @classmethod
    def deregister_peer(cls, peer_uid):
        """Deregisters a Peer from the list of peers present.
        If the Peer deregistered has a single instance, we delete it
        from the dictionary.
        """
        with cls.__peers_by_uid_lock:
            cls.__peers_by_uid[peer_uid].__instance_count -= 1

            if cls.__peers_by_uid[peer_uid].__instance_count < 1:
                del cls.__peers_by_uid[peer_uid]
