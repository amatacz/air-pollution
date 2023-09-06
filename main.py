from src.data_ingestion import cities_data_retrieve, cities_data_to_json, \
                                coord_extraction, air_pollution_retrieve, \
                                air_pollution_data_to_json


cities_data = cities_data_retrieve()
cities_data_to_json(cities_data)
coordinates_list = coord_extraction('cities_coordinates.json')
air_data = air_pollution_retrieve(coordinates_list)
air_pollution_data_to_json(air_data)
