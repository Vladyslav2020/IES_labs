from csv import reader
from datetime import datetime

import config
from domain.accelerometer import Accelerometer
from domain.aggregated_data import AggregatedData
from domain.gps import Gps
from domain.parking import Parking


class FileDatasource:
    def __init__(
            self,
            accelerometer_filename: str,
            gps_filename: str,
            parking_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        next(self.accelerometer_file)
        self.gps_file = open(self.gps_filename, 'r')
        next(self.gps_file)
        self.parking_file = open(self.parking_filename, 'r')
        next(self.parking_file)

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        if self.accelerometer_file:
            self.accelerometer_file.close()
        if self.gps_file:
            self.gps_file.close()
        if self.parking_file:
            self.parking_file.close()

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        accelerometer_data = self._read_accelerometer_data()
        gps_data = self._read_gps_data()
        parking_data = self._read_parking_data()

        return AggregatedData(
            accelerometer=accelerometer_data,
            gps=gps_data,
            parking=parking_data,
            timestamp=datetime.now(),
            user_id=config.USER_ID,
        )

    def _read_accelerometer_data(self) -> Accelerometer:
        csv_reader = reader(self.accelerometer_file)
        try:
            row = next(csv_reader)
        except StopIteration:
            self.accelerometer_file.seek(0)
            next(self.accelerometer_file)
            row = next(csv_reader)

        x, y, z = map(int, row)
        return Accelerometer(x, y, z)

    def _read_gps_data(self) -> Gps:
        csv_reader = reader(self.gps_file)
        try:
            row = next(csv_reader)
        except StopIteration:
            self.gps_file.seek(0)
            next(self.gps_file)
            row = next(csv_reader)

        longitude, latitude = map(float, row)
        return Gps(longitude, latitude)

    def _read_parking_data(self) -> Parking:
        csv_reader = reader(self.parking_file)
        try:
            row = next(csv_reader)
        except StopIteration:
            self.parking_file.seek(0)
            next(self.parking_file)
            row = next(csv_reader)

        empty_count, longitude, latitude = map(float, row)
        return Parking(int(empty_count), Gps(longitude, latitude))
