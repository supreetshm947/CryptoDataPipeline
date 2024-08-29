#Load Coin Metadata
from coin_utils import load_coin_metadata, load_coin_historic_data
# load_coin_metadata()

#loading historic data for all coins in database
from datetime import datetime, timedelta

start_date = str((datetime.now() - timedelta(days=364)).date())
load_coin_historic_data(start_date)