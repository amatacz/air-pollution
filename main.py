from src.data_ingestion import cities_data_retrieve, cities_data_to_json, \
                                coord_extraction, air_pollution_retrieve, \
                                air_pollution_data_to_json, \
                                upload_data_to_bucket, \
                                historical_air_pollution_data_retrieve, \
                                historical_air_pollution_data_to_json



# TU NIE WIEM, JAK MĄDRZE ZROBIĆ,
# ŻEBY TE FUNKCJE URUCHAMIAŁY SIĘ JEDNA PO DRUGIEJ

cities_data = cities_data_retrieve()
cities_data_to_json(cities_data)
coordinates_list = coord_extraction('code/data/cities_data.json')
air_data = air_pollution_retrieve(coordinates_list)
air_pollution_data_to_json(air_data)
historical_air_data = historical_air_pollution_data_retrieve("05-09-2023 00:00:00", "09-09-2023 00:00:00", coordinates_list)
historical_air_pollution_data_to_json(historical_air_data)
upload_data_to_bucket()
