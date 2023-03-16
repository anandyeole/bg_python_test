from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
from buildgrid.cas_utils import Context, upload_file, download_file
import os


def dask_run(args):
    cmd, deps, gen_files = args
    curr_dir = os.getcwd()
    folder_name = 'tmp.' + str(os.getpid())
    while os.path.exists(folder_name):
        folder_name = folder_name + '1'
    os.makedirs(folder_name, exist_ok=True)
    os.chdir(folder_name)

    for dep in deps.values():
        digest = dep['digest']
        file_path = dep['file_path']
        if file_path.startswith('/'):
            file_path = folder_name + file_path
        download_file(Context, digest, file_path, verify=True)

    os.system(cmd)
    print('done with cmd : ' + cmd)

    out_files = []
    for file in gen_files:
        out = upload_file(Context, [file], verify=True)
        out_files.append(out)

    os.chdir(curr_dir)
    os.system('rm -rf ' + folder_name)

    return out_files


def upload_dep(deps):
    data = {}
    for dep in deps:
        file = upload_file(Context, [dep], verify=True)
        data[dep] = {'digest': file['digest'], 'file_path': file['path']}
    # print(data)
    return data


def upload_dep_and_run_dask(cmd, files, out_files):
    deps = upload_dep(files)
    cmd = (cmd, deps, out_files)
    file = dask_run(cmd)
    return file


def get_files(file):
    with open(file) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    return content


if __name__ == '__main__':

    cmd_list = []
    print("running")
    with ProcessPoolExecutor() as executor:
        futures = []
        for i in range(1, 201):
            files = get_files(str(i) + ".txt")
            cmd = "gcc " + str(i) + ".c -o " + str(i) + ".o"
            out_files = [str(i) + ".o"]
            future = executor.submit(upload_dep_and_run_dask, cmd, files, out_files)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            try:
                out_files = future.result()
                download_file(Context, out_files[0]['digest'], out_files[0]['path'], verify=True, overwrite=True)
            except Exception as e:
                print(e)
