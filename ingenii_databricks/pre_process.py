import csv
from os.path import exists
import json


class PreProcess:
    def __init__(self, data_provider: str, table: str, file_name: str,
                 development: bool = False):
        self.data_provider = data_provider
        self.table = table
        self.file_name = file_name

        self.development = development

        if not exists(self.get_file_path()):
            raise Exception(
                f"Unable to find file at {self.get_file_path()}!"
            )

    def get_file_path(self):
        if self.development:
            return self.file_name
        return "/" + "/".join(["dbfs", "mnt", "raw",
                               self.data_provider, self.table, self.file_name])

    def get_filename_no_extension(self):
        return ".".join(self.file_name.split(".")[:-1])

    def get_raw_file(self):
        with open(self.get_file_path(), "r") as raw_file:
            return raw_file.read()

    def get_raw_file_by_line(self):
        with open(self.get_file_path(), "r") as raw_file:
            for line in raw_file.readlines():
                yield line

    def get_file_as_json(self):
        with open(self.get_file_path(), "r") as jsonfile:
            return json.load(jsonfile)

    def write_json_to_csv(self, new_file_name, json_to_write,
                          write_header=True, **kwargs):
        with open(new_file_name, "w") as result:

            fieldnames = list(json_to_write[0].keys())
            writer = csv.DictWriter(result, fieldnames=fieldnames, **kwargs)

            if write_header:
                writer.writeheader()

            for entry in json_to_write:
                writer.writerow(entry)
