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


from contextlib import contextmanager
from functools import partial
from io import BytesIO
from itertools import combinations
from urllib.parse import urljoin
from operator import attrgetter
from typing import AnyStr, BinaryIO, Dict, Iterable, IO, List, Mapping, Optional, Sequence, Set, Tuple, TypeVar, Union
import hashlib
import json
import os
import socket
import threading

from buildgrid.settings import HASH, HASH_LENGTH, BROWSER_URL_FORMAT, DEFAULT_LOCK_ACQUIRE_TIMEOUT
from buildgrid._enums import JobEventType
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2


T = TypeVar('T')


secure_uri_schemes = ["https", "grpcs"]
insecure_uri_schemes = ["http", "grpc"]


class BrowserURL:

    __url_markers = (
        '%(instance)s',
        '%(type)s',
        '%(hash)s',
        '%(sizebytes)s',
    )

    def __init__(self, base_url, instance_name=None):
        """Begins browser URL helper initialization."""
        self.__base_url = base_url
        self.__initialized = False
        self.__url_spec = {
            '%(instance)s': instance_name or '',
        }

    def for_message(self, message_type, message_digest):
        """Completes browser URL initialization for a protobuf message."""
        if self.__initialized:
            return False

        self.__url_spec['%(type)s'] = message_type
        self.__url_spec['%(hash)s'] = message_digest.hash
        self.__url_spec['%(sizebytes)s'] = str(message_digest.size_bytes)

        self.__initialized = True
        return True

    def generate(self):
        """Generates a browser URL string."""
        if not self.__base_url or not self.__initialized:
            return None

        url_tail = BROWSER_URL_FORMAT

        for url_marker in self.__url_markers:
            if url_marker not in self.__url_spec:
                return None
            if url_marker not in url_tail:
                continue
            url_tail = url_tail.replace(url_marker, self.__url_spec[url_marker])

        return urljoin(self.__base_url, url_tail)


class TypedEvent:

    """Wrapper around a ``threading.Event`` to support event 'types'"""

    def __init__(self):
        self.event = threading.Event()
        self.history = []

    def set(self, event_type=None):
        self.history.append(event_type)
        self.event.set()

    def clear(self):
        self.event.clear()

    def notify_change(self):
        self.set(event_type=JobEventType.CHANGE)
        self.clear()

    def notify_stop(self):
        self.set(event_type=JobEventType.STOP)
        self.clear()

    def wait(self, last_received=None, timeout=None):
        if last_received is not None:
            next_index = last_received + 1
            if next_index < len(self.history):
                return next_index, self.history[next_index]
        if self.event.wait(timeout=timeout):
            return len(self.history) - 1, self.history[-1]
        else:
            # Timeout: no event and `last_received` is unchanged
            return last_received, None


class JobState:

    def __init__(self, job):
        self.cancelled = job.cancelled
        self.operation_stage = job.operation_stage

    def __eq__(self, other):
        return (self.cancelled == other.cancelled and
                self.operation_stage == other.operation_stage)


class JobWatchSpec:

    """Structure to track what operations are being watched for a given job.

    This also contains the event used for notifying watchers of changes, and the
    state that the job was in after a change was last detected.

    """

    def __init__(self, job):
        """Instantiate a new JobWatchSpec.

        Args:
            job (buildgrid.server.job.Job): The job that this spec tracks the
                watchers and state for.

        """
        self.event = TypedEvent()
        self.last_state = JobState(job)
        self.operations = {}
        self.operations_lock = threading.Lock()

    @property
    def peers(self):
        with self.operations_lock:
            return [peer for op in self.operations.values()
                    for peer in op["peers"]]

    def peers_for_operation(self, operation_name):
        """Returns a copy of the list of peers for the given operation.

        If the operation is not being watched, or for some reason has no "peers"
        key, the empty list is returned.

        Args:
            operation_name (string): The name of the operation to get the list
                of peers for.

        """
        try:
            return self.operations[operation_name]["peers"].copy()
        except KeyError:
            return []

    def add_peer(self, operation_name, peer):
        """Add a peer to the set of peers watching the job this spec is for.

        Takes an operation name and a peer and tracks that the peer is watching
        the given operation.

        Args:
            operation_name (string): The name of the operation that the peer
                is watching for updates on.
            peer (string): The peer that is starting to watch for updates.

        """
        with self.operations_lock:
            if operation_name in self.operations:
                self.operations[operation_name]["peers"].append(peer)
            else:
                self.operations[operation_name] = {
                    "peers": [peer]
                }

    def remove_peer(self, operation_name, peer):
        """Remove a peer from the list watching an operation for this job.

        The inverse of ``add_peer``. Takes an operation name and a peer and
        removes that peer from the list of peers watching that operation.
        If this leaves the operation with no peers watching it, the operation
        is removed from the ``JobWatchSpec``.

        Args:
            operation_name (string): The name of the operation that is
                no longer being watched by the peer.
            peer (string): The name of the peer that is stopping watching.

        """
        with self.operations_lock:
            if operation_name in self.operations:
                self.operations[operation_name]["peers"].remove(peer)
                if not self.operations[operation_name]["peers"]:
                    self.operations.pop(operation_name)


@contextmanager
def acquire_lock_or_timeout(lock, timeout=DEFAULT_LOCK_ACQUIRE_TIMEOUT):
    result = lock.acquire(timeout=timeout)
    if result:
        try:
            yield result
        finally:
            lock.release()
    else:
        raise TimeoutError(f'Could not acquire lock with timeout=[{timeout}]')


def get_hostname():
    """Returns the hostname of the machine executing that function.

    Returns:
        str: Hostname for the current machine.
    """
    return socket.gethostname()


def get_hash_type():
    """Returns the hash type."""
    hash_name = HASH().name
    if hash_name == "sha256":
        return remote_execution_pb2.DigestFunction.SHA256
    return remote_execution_pb2.DigestFunction.UNKNOWN


def create_digest(bytes_to_digest):
    """Computes the :obj:`Digest` of a piece of data.

    The :obj:`Digest` of a data is a function of its hash **and** size.

    Args:
        bytes_to_digest (bytes): byte data to digest.

    Returns:
        :obj:`Digest`: The :obj:`Digest` for the given byte data.
    """
    return remote_execution_pb2.Digest(hash=HASH(bytes_to_digest).hexdigest(),
                                       size_bytes=len(bytes_to_digest))


def create_digest_from_file(file_obj: BinaryIO) -> remote_execution_pb2.Digest:
    """Computed the :obj:`Digest` of a file-like object.

    The :obj:`Digest` contains a hash of the file's contents and the size of
    those contents. This function only reads the content in chunks for hashing,
    so is safe to use on large files.

    Args:
        file_obj (BinaryIO): A file-like object of some kind.

    Returns:
        :obj:`Digest`: The :obj:`Digest` for the given file object.
    """
    digest = remote_execution_pb2.Digest()

    # Make sure we're hashing from the start of the file
    file_obj.seek(0)

    # Generate the file hash and keep track of the file size
    hasher = HASH()
    digest.size_bytes = 0
    for block in iter(partial(file_obj.read, 8192), b''):
        hasher.update(block)
        digest.size_bytes += len(block)
    digest.hash = hasher.hexdigest()

    # Return to the start of the file ready for future reads
    file_obj.seek(0)
    return digest


def parse_digest(digest_string):
    """Creates a :obj:`Digest` from a digest string.

    A digest string should alway be: ``{hash}/{size_bytes}``.

    Args:
        digest_string (str): the digest string.

    Returns:
        :obj:`Digest`: The :obj:`Digest` read from the string or None if
            `digest_string` is not a valid digest string.
    """
    digest_hash, digest_size = digest_string.split('/')

    if len(digest_hash) == HASH_LENGTH and digest_size.isdigit():
        return remote_execution_pb2.Digest(hash=digest_hash,
                                           size_bytes=int(digest_size))

    return None


def validate_digest_data(digest: remote_execution_pb2.Digest, data: bytes):
    """ Validate that the given digest corresponds to the given data. """
    return len(data) == digest.size_bytes and HASH(data).hexdigest() == digest.hash


def read_file(file_path):
    """Loads raw file content in memory.

    Args:
        file_path (str): path to the target file.

    Returns:
        bytes: Raw file's content until EOF.

    Raises:
        OSError: If `file_path` does not exist or is not readable.
    """
    with open(file_path, 'rb') as byte_file:
        return byte_file.read()


def write_file(file_path, content):
    """Dumps raw memory content to a file.

    Args:
        file_path (str): path to the target file.
        content (bytes): raw file's content.

    Raises:
        OSError: If `file_path` does not exist or is not writable.
    """
    with open(file_path, 'wb') as byte_file:
        byte_file.write(content)
        byte_file.flush()


def read_and_rewind(read_head: IO) -> Optional[AnyStr]:
    """Reads from an IO object and returns the data found there
    after rewinding the object to the beginning.

    Args:
        read_head (IO): readable IO head

    Returns:
        AnyStr: readable content from `read_head`.
    """
    if not read_head:
        return None

    data = read_head.read()
    read_head.seek(0)
    return data


def merkle_tree_maker(directory_path):
    """Walks a local folder tree, generating :obj:`FileNode` and
    :obj:`DirectoryNode`.

    Args:
        directory_path (str): absolute or relative path to a local directory.

    Yields:
        :obj:`Message`, bytes, str: a tutple of either a :obj:`FileNode` or
        :obj:`DirectoryNode` message, the corresponding blob and the
        corresponding node path.
    """
    directory_name = os.path.basename(directory_path)

    # Actual generator, yields recursively FileNodes and DirectoryNodes:
    def __merkle_tree_maker(directory_path, directory_name):
        if not os.path.isabs(directory_path):
            directory_path = os.path.abspath(directory_path)

        directory = remote_execution_pb2.Directory()

        files, directories, symlinks = [], [], []
        for directory_entry in os.scandir(directory_path):
            node_name, node_path = directory_entry.name, directory_entry.path

            if directory_entry.is_file(follow_symlinks=False):
                with open(directory_entry.path, 'rb') as node_blob:
                    node_digest = create_digest_from_file(node_blob)

                    node = remote_execution_pb2.FileNode()
                    node.name = node_name
                    node.digest.CopyFrom(node_digest)
                    node.is_executable = os.access(node_path, os.X_OK)

                    files.append(node)

                    yield node, node_blob, node_path

            elif directory_entry.is_dir(follow_symlinks=False):
                node, node_blob, _ = yield from __merkle_tree_maker(node_path, node_name)

                directories.append(node)

                yield node, node_blob, node_path

            # Create a SymlinkNode;
            elif os.path.islink(directory_entry.path):
                node_target = os.readlink(directory_entry.path)

                node = remote_execution_pb2.SymlinkNode()
                node.name = directory_entry.name
                node.target = node_target

                symlinks.append(node)

        files.sort(key=attrgetter('name'))
        directories.sort(key=attrgetter('name'))
        symlinks.sort(key=attrgetter('name'))

        directory.files.extend(files)
        directory.directories.extend(directories)
        directory.symlinks.extend(symlinks)

        node_blob = directory.SerializeToString()
        node_digest = create_digest(node_blob)

        node = remote_execution_pb2.DirectoryNode()
        node.name = directory_name
        node.digest.CopyFrom(node_digest)

        return node, BytesIO(node_blob), directory_path

    node, node_blob, node_path = yield from __merkle_tree_maker(directory_path,
                                                                directory_name)

    yield node, node_blob, node_path


def output_file_maker(file_path, input_path, file_digest):
    """Creates an :obj:`OutputFile` from a local file and possibly upload it.

    Note:
        `file_path` **must** point inside or be relative to `input_path`.

    Args:
        file_path (str): absolute or relative path to a local file.
        input_path (str): absolute or relative path to the input root directory.
        file_digest (:obj:`Digest`): the underlying file's digest.

    Returns:
        :obj:`OutputFile`: a new :obj:`OutputFile` object for the file pointed
        by `file_path`.
    """
    if not os.path.isabs(file_path):
        file_path = os.path.abspath(file_path)
    if not os.path.isabs(input_path):
        input_path = os.path.abspath(input_path)

    output_file = remote_execution_pb2.OutputFile()
    output_file.digest.CopyFrom(file_digest)
    # OutputFile.path should be relative to the working directory
    output_file.path = os.path.relpath(file_path, start=input_path)
    output_file.is_executable = os.access(file_path, os.X_OK)

    return output_file


def output_directory_maker(directory_path, working_path, tree_digest):
    """Creates an :obj:`OutputDirectory` from a local directory.

    Note:
        `directory_path` **must** point inside or be relative to `input_path`.

    Args:
        directory_path (str): absolute or relative path to a local directory.
        working_path (str): absolute or relative path to the working directory.
        tree_digest (:obj:`Digest`): the underlying folder tree's digest.

    Returns:
        :obj:`OutputDirectory`: a new :obj:`OutputDirectory` for the directory
        pointed by `directory_path`.
    """
    if not os.path.isabs(directory_path):
        directory_path = os.path.abspath(directory_path)
    if not os.path.isabs(working_path):
        working_path = os.path.abspath(working_path)

    output_directory = remote_execution_pb2.OutputDirectory()
    output_directory.tree_digest.CopyFrom(tree_digest)
    output_directory.path = os.path.relpath(directory_path, start=working_path)

    return output_directory


def convert_values_to_sorted_lists(
    dictionary: Mapping[str, Union[str, Sequence[str], Set[str]]]
) -> Dict[str, List[str]]:
    """ Given a dictionary, do the following:

    1. Turn strings into singleton lists
    2. Turn all other sequence types into sorted lists with list()

    This returns the converted dictionary and does not change the dictionary
    that was passed in.

    """
    normalized: Dict[str, List[str]] = {}
    for key, value in dictionary.items():
        if isinstance(value, str):
            normalized[key] = [value]
        else:
            try:
                normalized[key] = sorted(list(value))
            except TypeError:
                raise ValueError(f"{value} cannot be sorted")
    return normalized


def hash_from_dict(dictionary: Mapping[str, List[str]]) -> str:
    """ Get the hash represntation of a dictionary """
    return hashlib.sha1(json.dumps(dictionary, sort_keys=True).encode()).hexdigest()


def get_unique_objects_by_attribute(objects: Sequence[T], attribute: str) -> Iterable[T]:
    """ Return a list of unique objects based on a hashable attribute or chained attributes.

    Note that this does not provide any sanitization, and any problematic elements will
    only raise exceptions when iterated on. """

    attrs_seen = set()

    for obj in objects:
        if obj:
            attr_value = attrgetter(attribute)(obj)
            if attr_value not in attrs_seen:
                attrs_seen.add(attr_value)
                yield obj


def retry_delay(retry_attempt: int, delay_base: int=1) -> float:
    attempt = min(5, retry_attempt)  # Limit the delay to ~10.5x the base time
    return round(delay_base * (1.6 ** attempt), 1)


def flatten_capabilities(capabilities: Mapping[str, Union[Set[str], List[str]]]) -> List[Tuple[str, str]]:
    """Flatten a capabilities dictionary.

    This method takes a capabilities dictionary and flattens it into a
    list of key/value tuples describing all the platform properties
    that the capabilities map to. To do this, it assumes that all of the
    dictionary's values are iterable.

    For example,

        ``{'OSFamily': {'Linux'}, 'ISA': {'x86-32', 'x86-64'}}``

    becomes

        ``[('OSFamily', 'Linux'), ('ISA', 'x86-32'), ('ISA', 'x86-64')]``

    Args:
        capabilities (dict): The capabilities dictionary to flatten.

    Returns:
        list containing the flattened dictionary key-value tuples.

    """
    return [
        (name, value) for name, value_list in capabilities.items()
        for value in value_list
    ]


def combinations_with_unique_keys(iterator: Sequence[Tuple[str, str]], size: int) -> Iterable[Iterable[Tuple]]:
    """Return an iterator of the unique combinations of the input without duplicated keys.

    The input ``iterator`` is a sequence of key-value tuples. This function behaves
    similarly to :func:`itertools.combinations`, except combinations containing
    more than one tuple with the same first element are not included in the result.

    The ``size`` argument specifies how many elements should be included in the
    resulting combinations.

    For example,

    .. code-block:: python

        >>> capabilities = [('OSFamily', 'linux'), ('ISA', 'x86-64'), ('ISA', 'x86-32')]
        >>> platforms = combinations_with_unique_keys(capabilities, 2)
        >>> for item in platforms:
        ...     print(item)
        ...
        (('OSFamily', 'linux'), ('ISA', 'x86-64'))
        (('OSFamily', 'linux'), ('ISA', 'x86-32'))

    Args:
        iterator (list): The list of key-value tuples to return combinations of.
        size (int): How many elements to include in each combination.

    Returns:
        An iterator of the combinations of the input in which each key appears
        at most once.

    """
    def _validate_combination(combination):
        seen = set()
        return not any(key in seen or seen.add(key) for key, _ in combination)
    yield from filter(_validate_combination, combinations(iterator, size))
