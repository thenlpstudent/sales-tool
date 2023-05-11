import pandas as pd

from constants import *
from abc import ABC, abstractmethod
from enum import Enum, auto


class ByType(Enum):
    BY_HOUR = auto()
    BY_DAY_OF_WEEK = auto()
    BY_MONTH = auto()
    BY_YEAR = auto()
    BY_CITY = auto()
    BY_STATE = auto()

    @staticmethod
    def get_type_str(type):
        if type is ByType.BY_CITY:
            return "By City"
        if type is ByType.BY_HOUR:
            return "By Hour"
        if type is ByType.BY_YEAR:
            return "By Year"
        if type is ByType.BY_STATE:
            return "By State"
        if type is ByType.BY_DAY_OF_WEEK:
            return "By Day of Week"
        if type is ByType.BY_MONTH:
            return "By Month"

    @staticmethod
    def get_type_lbl(by_type):
        if by_type is ByType.BY_CITY:
            return CITY_LABEL
        if by_type is ByType.BY_HOUR:
            return HOUR_LABEL
        if by_type is ByType.BY_YEAR:
            return YEAR_LABEL
        if by_type is ByType.BY_STATE:
            return STATE_LABEL
        if by_type is ByType.BY_DAY_OF_WEEK:
            return DAY_OF_WEEK_LABEL
        if by_type is ByType.BY_MONTH:
            return MONTH_LABEL


class AbstractDataFilter(ABC):
    def __init__(self, df):
        self._df = df

    @abstractmethod
    def groupby(self):
        pass

    @abstractmethod
    def filter(self, value):
        pass

    @abstractmethod
    def add_type_col(self):
        pass

    @abstractmethod
    def get_col_name(self):
        pass


class DataFilterFactory:
    def __init__(self, df, by_type: ByType):
        self._df = df
        self._type = by_type

    def create_data_filter_from_type(self):
        if self._type is ByType.BY_CITY:
            return DataFilterCity(self._df)
        elif self._type is ByType.BY_STATE:
            return DataFilterState(self._df)
        elif self._type is ByType.BY_YEAR or self._type is ByType.BY_MONTH or self._type is ByType.BY_DAY_OF_WEEK \
                or self._type is ByType.BY_HOUR:
            return DataFilterDate(df=self._df, by_type=self._type)
        else:
            raise NotImplementedError(f"Type '{ByType.get_type_str(self._type)}' is not yet implemented!")

    @property
    def data_frame(self):
        return self._df


class DataFilterCity(AbstractDataFilter):
    def __init__(self, df):
        super(DataFilterCity, self).__init__(df)

    def groupby(self):
        self._df = self._df.groupby(CITY_LABEL, as_index=False)

    def filter(self, city):
        self._df.loc[self._df[CITY_LABEL].str.contains(city)]

    @staticmethod
    def _format_city(value):
        value = value.split(",")
        return f"{value[1].strip()} {value[2].strip().split(' ')[0]}"

    def add_type_col(self):
        self._df[CITY_LABEL] = self._df[PURCHASE_ADDRESS_LABEL].apply(DataFilterCity._format_city)

    def get_col_name(self):
        return CITY_LABEL

    @property
    def data_frame(self):
        return self._df


class DataFilterState(AbstractDataFilter):
    def __init__(self, df):
        super(DataFilterState, self).__init__(df)

    def groupby(self):
        self._df = self._df.groupby(STATE_LABEL, as_index=False)

    def filter(self, state):
        state = state.strip()

        if state not in abbrev_to_us_state.keys():
            if state not in us_state_to_abbrev.keys():
                raise ValueError(f"State {state} is invalid!")
            state = us_state_to_abbrev[state]

        self._df.loc[self._df[STATE_LABEL].str.contains(state)]

    def add_type_col(self):
        self._df[STATE_LABEL] = self._df[PURCHASE_ADDRESS_LABEL].apply(DataFilterState._format_state)

    def get_col_name(self):
        return STATE_LABEL

    @property
    def data_frame(self):
        return self._df

    @staticmethod
    def _format_state(value):
        value = value.split(",")
        state = abbrev_to_us_state[value[2].strip().split(' ')[0].strip()]
        return f"{state}"


class DataFilterDate(AbstractDataFilter):
    def __init__(self, by_type, df):
        super(DataFilterDate, self).__init__(df)
        self._type = by_type

    def groupby(self):
        col_lbl = self.get_col_name()
        self._df = self._df[[col_lbl, COUNT_LABEL, TOTAL_PRICE_LABEL]]
        self._df = self._df.groupby(col_lbl, as_index=False)

    def filter(self, value):
        col_lbl = self.get_col_name()
        self._df = self._df[[col_lbl, PRODUCT_LABEL, COUNT_LABEL, TOTAL_PRICE_LABEL]]
        self._df = self._df.loc[self._df[col_lbl] == value]

    def add_type_col(self):
        self._df[DATETIME_LABEL] = pd.to_datetime(self._df[ORDER_DATE_LABEL], format=DATE_FORMAT, errors="ignore")
        if self._type is ByType.BY_YEAR:
            self._df[YEAR_LABEL] = self._df[DATETIME_LABEL].dt.year
        elif self._type is ByType.BY_MONTH:
            self._df[MONTH_LABEL] = self._df[DATETIME_LABEL].dt.month
        elif self._type is ByType.BY_DAY_OF_WEEK:
            self._df[DAY_OF_WEEK_LABEL] = self._df[DATETIME_LABEL].dt.dayofweek
        elif self._type is ByType.BY_HOUR:
            self._df[HOUR_LABEL] = self._df[DATETIME_LABEL].dt.hour

    def get_col_name(self):
        return ByType.get_type_lbl(self._type)

    @property
    def data_frame(self):
        return self._df
