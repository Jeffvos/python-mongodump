import os
import json
from datetime import date
import minio_client

with open('config.json') as config:
    config_file = json.load(config)


class MongoDump:
    def __init__(self):
        self.today = date.today().strftime('%b-%d-%Y')
        self.tempdir = config_file['temp_dir_path']
        self.bucket = config_file['bucket']
        self.create_temp_dir()

    def create_temp_dir(self):
        create_dir = os.system("mkdir -p {}".format(self.tempdir))
        return self.create_dump()

    def create_dump(self):
        command = f"mongodump -o={self.tempdir}"
        dumpdb = os.system(command)
        return self.compress_folder()

    def compress_folder(self):
        command = f"tar -czvf {self.tempdir}/mongoDump-{self.today}.tar.gz {self.tempdir}"
        compressdir = os.system(command)
        return self.transfer_dump()

    def transfer_dump(self):
        storage = minio_client.Storage()
        filename = f"mongoDump-{self.today}.tar.gz"
        path_to_tar = f"{self.tempdir}/{filename}"
        storage.upload_to_bucket(self.bucket, filename, path_to_tar)
        return self.destroy_dump()

    def destroy_dump(self):
        command = f"rm -rf {self.tempdir}"
        remove_dir = os.system(command)
        return remove_dir


if __name__ == "__main__":
    mongodump = MongoDump()
