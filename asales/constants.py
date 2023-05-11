DATA_DIR = "data"
OUTPUT_FILE_NAME = "report.pdf"
DO_CLEAN_BY_DEFAULT = True

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
}
abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))

['Order ID', 'Product', 'Quantity Ordered', 'Price Each', 'Order Date',
 'Purchase Address']
CITY_LABEL = "City"
STATE_LABEL = "State"
ORDER_ID_LABEL = "Order ID"
PRODUCT_LABEL = "Product"
QTY_ORDERED_LABEL = "Quantity Ordered"
PRICE_EACH_LABEL = "Price Each"
ORDER_DATE_LABEL = "Order Date"
PURCHASE_ADDRESS_LABEL = "Purchase Address"
DAY_OF_WEEK_LABEL = "DayWeek"
MONTH_LABEL = "Month"
YEAR_LABEL = "Year"
HOUR_LABEL = "Hour"
COUNT_LABEL = "Count"
DATETIME_LABEL = "Datetime"
INVALID_VALUES = ["Product"]

FILE_SIZE_CAP = -1
DATE_FORMAT = "%m/%d/%y %H:%M"
