"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass, asdict

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: str
    blue: str
    green: str


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("com.udacity.details.stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1, value_type=TransformedStation)
# TODO: Define a Faust Table
table = app.Table(
   "org.chicago.cta.stations.table.v1",
   partitions=1,
   changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def process(stream):
    async for data in stream:
        logger.info(f"data: {asdict(data)}")
        if data.red:
            line = "red"
        elif data.green:
            line = "green"
        elif data.blue:
            line = "blue"
        else:
            line = None
            logger.error("Missing color field")

        table[data.station_id] = TransformedStation(
            station_id=data.station_id,
            station_name=data.station_name,
            order=data.order,
            line=line
        )

        logger.info(f"Aggregated data {asdict(table[data.station_id])}")


if __name__ == "__main__":
    app.main()
