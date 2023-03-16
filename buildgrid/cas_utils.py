from buildgrid.client.cas import upload, download
import os
import sys
from buildgrid.client.channel import setup_channel
from buildgrid.utils import create_digest, parse_digest, merkle_tree_maker, read_file
from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2


class Context:

    def __init__(self):
        self.verbose = False
        self.channel = None
        self.cas_channel = None
        self.operations_channel = None
        self.cache_channel = None
        self.logstream_channel = None
        self.instance_name = ''

        self.user_home = os.getcwd()


cas_server = 'http://localhost:50052'
instance_name = 'buildgrid'
context = Context
context.channel, _ = setup_channel(remote_url=cas_server,
                                   auth_token=None, client_key=None,
                                   client_cert=None, server_cert=None,
                                   action_id=None, tool_invocation_id=None,
                                   correlated_invocations_id=None, asynchronous=False,
                                   timeout=None)
context.instance_name = instance_name


def upload_file(context, file_path: list, verify=True):
    sent_digests = []
    sent_details = []
    try:
        with upload(context.channel, instance=context.instance_name) as uploader:
            for path in file_path:
                if not os.path.isabs(path):
                    path = os.path.relpath(path)

                print(f"Queueing path=[{path}]")

                file_digest = uploader.upload_file(path, queue=True)

                sent_digests.append((file_digest, path))
    except Exception as e:
        print(f'Error: Uploading file: {e}')
        sys.exit(-1)

    for sent_file_digest, sent_file_path in sent_digests:
        if verify and sent_file_digest.size_bytes != os.stat(sent_file_path).st_size:
            print(f"Error: Failed to verify '{sent_file_path}'")
        elif sent_file_digest.ByteSize():
            print(f"Success: Pushed path=[{sent_file_path}] with digest="
                  f"[{sent_file_digest.hash}/{sent_file_digest.size_bytes}]")
        else:
            print(
                f"Error: Failed pushing path=[{sent_file_path}]")
    for digest, path in sent_digests:
        if os.path.isdir(path):
            details = {'type': 'directory_path',
                       'digest': digest.hash + '/' + str(digest.size_bytes), 'path': path}
            sent_details.append(details)
        elif os.path.isfile(path):
            details = {'type': 'file_path',
                       'digest': digest.hash + '/' + str(digest.size_bytes), 'path': path}
            sent_details.append(details)
    return sent_details[0]
    print(sent_details)
    print(sent_digests)


def upload_directory(context, directory_path: str, verify=True):
    sent_digests = []
    sent_details = []
    try:
        with upload(context.channel, instance=context.instance_name) as uploader:
            for node, blob, path in merkle_tree_maker(directory_path):
                if not os.path.isabs(directory_path):
                    path = os.path.relpath(path)
                print(f"Queueing path=[{path}]")

                node_digest = uploader.put_blob(
                    blob, digest=node.digest, queue=True)
                sent_digests.append((node_digest, path))
    except Exception as e:
        print(f'Error: Uploading directory: {e}')
        sys.exit(-1)

    for node_digest, node_path in sent_digests:
        if verify and (os.path.isfile(node_path) and
                       node_digest.size_bytes != os.stat(node_path).st_size):
            print(f"Error: Failed to verify path=[{node_path}]")
        elif node_digest.ByteSize():
            print(f"Success: Pushed path=[{node_path}] with digest="
                  f"{node_digest.hash}/{node_digest.size_bytes}")
        else:
            print(f"Error: Failed pushing path=[{node_path}]")
    for digest, path in sent_digests:
        if os.path.isdir(path):
            details = {'type': 'directory_path',
                       'digest': digest.hash + '/' + str(digest.size_bytes), 'path': path}
            sent_details.append(details)
        elif os.path.isfile(path):
            details = {'type': 'file_path',
                       'digest': digest.hash + '/' + str(digest.size_bytes), 'path': path}
            sent_details.append(details)
    print(sent_details)
    # print(sent_digests)


def download_file(context, file_digest: str, file_path: str, verify=True, overwrite=False):
    # Downloading files:
    downloaded_files = {}
    digest_path_list = [file_digest, file_path]
    try:
        with download(context.channel, instance=context.instance_name) as downloader:
            for (digest_string, file_path) in zip(digest_path_list[0::2],
                                                  digest_path_list[1::2]):
                if os.path.exists(file_path):
                    if overwrite:
                        print(f"path=[{file_path}] already exists. Overwriting file...")
                    else:
                        print("Error: Invalid value for " + f"path=[{file_path}] already exists.")
                        continue

                digest = parse_digest(digest_string)

                downloader.download_file(digest, file_path)
                downloaded_files[file_path] = digest
    except NotFoundError:
        print('Error: Blob not found in CAS')
        sys.exit(-1)
    except Exception as e:
        print(f'Error: Downloading file: {e}')
        sys.exit(-1)

    # Verifying:
    for (file_path, digest) in downloaded_files.items():
        if verify:
            downloaded_file_digest = create_digest(read_file(file_path))
            if downloaded_file_digest != digest:
                print(
                    f"Error: Failed to verify path=[{file_path}]")
                continue

        if os.path.isfile(file_path):
            print(f"Success: Pulled path=[{file_path}] from digest="
                  f"[{digest.hash}/{digest.size_bytes}]")
        else:
            print(f'Error: Failed pulling "{file_path}"')


def download_directory(context, digest_string, directory_path, verify=True):
    if os.path.exists(directory_path):
        if not os.path.isdir(directory_path) or os.listdir(directory_path):
            print("Error: Invalid value, " +
                  f"path=[{directory_path}] already exists.")
            return

    digest = parse_digest(digest_string)

    try:
        with download(context.channel, instance=context.instance_name) as downloader:
            downloader.download_directory(digest, directory_path)
    except Exception as e:
        print(f'Error: Downloading directory: {e}')
        sys.exit(-1)

    if verify:
        last_directory_node = None
        for node, _, _ in merkle_tree_maker(directory_path):
            if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                last_directory_node = node
        if last_directory_node.digest != digest:
            print(
                f"Error: Failed to verify path=[{directory_path}]")
            return

    if os.path.isdir(directory_path):
        print(f"Success: Pulled path=[{directory_path}] from digest="
              f"[{digest.hash}/{digest.size_bytes}]")
    else:
        print(f"Error: Failed pulling path=[{directory_path}]")


# upload_directory(context, '/work/Cisco/cas_utilities/buildgrid/_app/')
#upload_file(context,['/usr/include/stdc-predef.h/usr/include/stdio.h'])
# download_file(context,'kafasd708524985hajhdsf','/newfile.tar')
# download_directory(context,'ajsdhf8034asdf7987asdf6ag5sad')
