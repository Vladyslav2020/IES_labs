from marshmallow import Schema, fields

from schema.gps_schema import GpsSchema


class ParkingSchema(Schema):
    empty_count = fields.Number()
    gps = fields.Nested(GpsSchema)
