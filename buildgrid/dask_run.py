from buildgrid.cas_utils import Context, upload_file, download_file
import os

# run the dask command


def dask_run(cmd, deps, gen_files):
    # print(type(deps))
    # print(deps)
    # create a unique tmp folder
    curr_dir = os.getcwd()
    folder_name = 'tmp.' + str(os.getpid())
    # check if folder exists if not create it else create new folder name
    while os.path.exists(folder_name):
        folder_name = folder_name + '1'
    # print(folder_name)
    os.makedirs(folder_name, exist_ok=True)
    os.chdir(folder_name)

    for dep in deps.values():
        digest = dep['digest']
        file_path = dep['file_path']
        # print(digest)
        # print(file_path)
        # print(temp_path)
        if file_path.startswith('/'):
            file_path = folder_name + file_path
        else:
            pass
        print(file_path)
        download_file(Context, digest, file_path, verify=True)  # , overwrite=True)
    # run cmd on terminal
    os.system(cmd)
    print(os.getcwd())
    print('done with cmd : ' + cmd)
    # upload the generated files
    out_files = []
    for file in gen_files:
        print(file)
        out = upload_file(Context, [file], verify=True)
        out_files.append(out)
    os.chdir(curr_dir)
    # remove tmp folder
    os.system('rm -rf ' + folder_name)
    return out_files


def upload_dep(deps):
    data = {}
    for dep in deps:
        file = upload_file(Context, [dep], verify=True)
        data[dep] = {'digest': file['digest'], 'file_path': file['path']}
    # print(data)
    return data


# get each file from the txt file
def get_files(file):
    with open(file) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    return content


files = get_files("1.txt")
# print(files)
deps = upload_dep(files)
# print(deps)
curr_dir = os.getcwd()
file = dask_run("gcc 1.c -o 1.o", deps, ['1.o'])
print(file)
os.chdir(curr_dir)
download_file(Context, file[0]['digest'], file[0]['path'], verify=True, overwrite=True)
