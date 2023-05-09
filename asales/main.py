import pandas as pd
import os
import sys
from asales.constants import *


def exit_system(message):
    print(f"[Error]: {message}")
    sys.exit()


class DataFolderEmptyException(ValueError):
    """ DataFolderEmptyException occurs if the data folder provided doesn't contain any files """

    def __init__(self, folder_path, *args):
        super().__init__(args)
        self._folder_path = folder_path

    def __str__(self):
        return f"Folder {self._folder_path} doesn't contain any data files!"


class DataLoader:
    """ DataLoader class is responsible for loading data from the data folder, and converting it into one
        single Pandas Data Frame
    """

    def __init__(self, data_folder=DATA_DIR):
        self._folder_path = data_folder
        self._file_list = self._get_file_names()
        self._data_frame = None

    def _get_file_names(self):
        return [os.path.join(self._folder_path, f) for f in os.listdir(self._folder_path)
                if os.path.isfile(os.path.join(self._folder_path, f))]

    def init(self):
        try:
            if len(self._file_list) == 0:
                raise DataFolderEmptyException
        except DataFolderEmptyException as exp:
            exit_system(str(exp))
        else:
            self._data_frame = DataLoader.load_data_frames_from_list(self._file_list)

    @staticmethod
    def load_data_frames_from_list(data_file_list):
        return pd.concat([pd.read_csv(data_file) for data_file in data_file_list])

    @property
    def data_frame(self):
        return self._data_frame

    @data_frame.setter
    def data_frame(self, df):
        self._data_frame = df

    @data_frame.deleter
    def data_frame(self):
        del self._data_frame

    @property
    def file_list(self):
        return self._file_list
