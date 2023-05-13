import pandas as pd
import os
import sys
from _datafilter import *

pd.options.mode.chained_assignment = None  # Raise an exception, warn, or no action if trying to use chained assignment


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

    def __init__(self, data_folder=DATA_DIR, size_cap=FILE_SIZE_CAP):
        self._folder_path = data_folder
        self._data_frame = None
        self._size_cap = size_cap
        self._file_list = self._get_file_names()

    def _get_file_names(self):
        files = os.listdir(self._folder_path)
        if self._size_cap == -1:
            self._size_cap = len(files)
        file_list = files[0:self._size_cap]
        return [os.path.join(self._folder_path, f) for f in file_list
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


class DataClean:
    """
        Handles cleaning data of the dataframe, removes NaN and
        configured unnecessary values.
    """

    def __init__(self, df, invalid_col, invalid_values=[]):
        self._df = df
        self._invalid_col = invalid_col
        self._invalid_values = invalid_values

        self._df = self.remove_nan_vals()
        self._df = self.remove_invalid_vals()
        self._df = self._df.astype({PRICE_EACH_LABEL: "float", QTY_ORDERED_LABEL: "int"})

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
        return self._df.dropna()

    @staticmethod
    def remove_invalid_val(df, col, value):
        if col not in df.columns:
            return df
        return df.loc[df[col] != value]

    def remove_invalid_vals(self):
        df = self._df
        for value in self._invalid_values:
            df = DataClean.remove_invalid_val(df, self._invalid_col, value)
        return df


class DataValues:
    """
       Manages obtaining meaningful data values from the sales dataframe.
       Use the ByType for different filters
    """

    def __init__(self, df, do_clean=DO_CLEAN_BY_DEFAULT, invalid_col=PRODUCT_LABEL, invalid_values=INVALID_VALUES):
        self._df = df
        if do_clean:
            self._df = DataClean(self._df, invalid_col, invalid_values).data_frame

    @property
    def data_frame(self):
        return self._df

    @data_frame.setter
    def data_frame(self, df):
        self._df = df

    def group_by_type(self, by_type):
        data_filter = DataFilterFactory(self._df, by_type).create_data_filter_from_type()
        data_filter.add_type_col()
        data_filter.groupby()
        return data_filter.data_frame

    def filter_by_type(self, by_type, value):
        data_filter = DataFilterFactory(self._df, by_type).create_data_filter_from_type()
        data_filter.add_type_col()
        data_filter.filter(value)
        return data_filter.data_frame

    def get_sold_count_avg_by(self, by_type: ByType):
        self._df[COUNT_LABEL] = 1
        self._df[TOTAL_PRICE_LABEL] = self._df[QTY_ORDERED_LABEL] * self._df[PRICE_EACH_LABEL]
        df = self.group_by_type(by_type)

        col_name = ByType.get_type_lbl(by_type)
        sold_count_df = df.sum()[[col_name, COUNT_LABEL, TOTAL_PRICE_LABEL]]
        total = sold_count_df[COUNT_LABEL].sum()
        totalSales = sold_count_df[TOTAL_PRICE_LABEL].sum()

        sold_count_df[COUNT_LABEL] = sold_count_df[COUNT_LABEL].apply(lambda x: x / total * 100)
        sold_count_df[TOTAL_PRICE_LABEL] = sold_count_df[TOTAL_PRICE_LABEL].apply(lambda x: x / totalSales * 100)

        return sold_count_df

    def get_sold_count_by(self, by_type: ByType):
        self._df[COUNT_LABEL] = 1
        self._df[TOTAL_PRICE_LABEL] = self._df[QTY_ORDERED_LABEL] * self._df[PRICE_EACH_LABEL]
        df = self.group_by_type(by_type)
        col_name = ByType.get_type_lbl(by_type)
        sold_count_df = df.sum()[[col_name, COUNT_LABEL, TOTAL_PRICE_LABEL]]
        return sold_count_df

    def get_best_products_by(self, by_type: ByType, value):
        self._df[COUNT_LABEL] = 1
        self._df[TOTAL_PRICE_LABEL] = self._df[QTY_ORDERED_LABEL] * self._df[PRICE_EACH_LABEL]
        self._df = self.filter_by_type(by_type, value)
        self._df = self._df.groupby(PRODUCT_LABEL, as_index=False)
        return self._df.sum()[[PRODUCT_LABEL, COUNT_LABEL, TOTAL_PRICE_LABEL]]

    def get_best_product_pairs_by(self, by_type: ByType, value, n=-1):
        self._df[COUNT_LABEL] = 1
        self._df[TOTAL_PRICE_LABEL] = self._df[QTY_ORDERED_LABEL] * self._df[PRICE_EACH_LABEL]
        self._df = self.filter_by_type(by_type, value)
        self._df = self._df.loc[self._df.duplicated(subset=[ORDER_ID_LABEL], keep=False)]
        self._df[GROUP_PAIR_LABEL] = self._df.groupby(ORDER_ID_LABEL, as_index=False)[PRODUCT_LABEL]. \
            transform(lambda x: GROUP_PAIR_DELIM.join(x))
        self._df.drop_duplicates(subset=[ORDER_ID_LABEL, GROUP_PAIR_LABEL], keep="first",
                                 ignore_index=True, inplace=True)
        return self._df

class GeoPlotUS:
    """
        Handles plotting geo plot for US with states/cities
    """
    pass


if __name__ == "__main__":
    dl = DataLoader("../data")
    dl.init()
    dv = DataValues(dl.data_frame)
    print(dv.get_best_products_by(ByType.BY_HOUR, 3))
    dv = DataValues(dl.data_frame)
    print(dv.get_best_products_by(ByType.BY_DAY_OF_WEEK, 3))
