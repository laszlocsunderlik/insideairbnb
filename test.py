from typing import List, Tuple
from pydantic import BaseModel

class GeometryNeighbourhoods(BaseModel):
    type: str
    geometry: List[List[Tuple[float, float]]]

# Example input data
input_data = "SRID=4326;MULTIPOLYGON(((4.991669 52.324436,4.991755 52.324289,4.991828 52.324175,4.991894 52.324077,4.991952 52.323996,4.992036 52.32387,4.992109 52.323767,4.99217 52.323706,4.992597 6)))"

# Extract the WKT string from the input data
wkt_string = input_data.split(";")[1]

# Split the WKT string to get the type and coordinates
wkt_parts = wkt_string.split("(")
geometry_type = wkt_parts[0].strip()
coordinates_str = wkt_parts[1].split(")")[0].strip()

# Convert the coordinates to a list of lists of tuples
coordinates_list = [
    [tuple(map(float, coord.split())) for coord in polygon.split(",")]
    for polygon in coordinates_str.split("),(")
]

# Create an instance of GeometryNeighbourhoods
geometry_neighbourhood = GeometryNeighbourhoods(type=geometry_type, geometry=coordinates_list)

print(geometry_neighbourhood)
