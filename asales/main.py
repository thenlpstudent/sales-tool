import pandas as pd
import os
import sys
from enum import Enum, auto
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


class ByType(Enum):
    BY_HOUR = auto()
    BY_DAY_OF_WEEK = auto()
    BY_MONTH = auto()
    BY_YEAR = auto()
    BY_CITY = auto()
    BY_STATE = auto()


class DataClean:
    """
        Handles cleaning data of the dataframe, removes NaN and
        configured unnecessary values.
    """
    def __init__(self, df, invalid_values=[]):
        self._df = df
        self._invalid_values = invalid_values


    @property
    def invalid_values(self):
        return self._invalid_values

    @invalid_values.setter
    def invalid_values(self, invalid_values):
        self._invalid_values = invalid_values

    @property
    def data_frame(self):
        return self._df

    @data_frame.setter
    def data_frame(self, df):
        self._df = df

    def remove_nan_vals(self):
        pass

    def remove_invalid_vals(self):
        pass


class DataValues:
    """
       Manages obtaining meaningful data values from the sales dataframe.
       Use the ByType for different filters
    """
    def __init__(self, df, do_clean=DO_CLEAN_BY_DEFAULT):
        self._df = df

    @property
    def data_frame(self):
        return self._df

    @data_frame.setter
    def data_frame(self, df):
        self._df = df

    def get_sold_count_avg_by(self, by_type: ByType):
        pass

    def get_best_products_by(self, by_type: ByType):
        pass

    def get_best_product_n_pairs_by(self, by_type: ByType):
        pass


class GeoPlotUS:
    """
        Handles plotting geo plot for US with states/cities
    """
    pass


if __name__ == "__main__":
    dl = DataLoader("../data")
    dl.init()
    print(dl.data_frame.head(3))
