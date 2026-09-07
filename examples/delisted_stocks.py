from firstrate_data.domain.enums import DelistedArchive, EquitiesAdjustment, Timeframe
from firstrate_data.download.client import Client

client = Client.from_env()

for archive in tuple(DelistedArchive):
    client.download_delisted_bars(archive, Timeframe.DAY_1, EquitiesAdjustment.SPLIT)
