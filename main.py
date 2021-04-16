"""
Data collection from different source and import it from non-relation database to relational database.
Find useful insights from data processing
"""
from scripts import flights

if __name__ == '__main__':
    flight_obj = flights.Flights()
    # flight_obj.insert_cities_countries()
    flight_obj.collect_data()
    attration()
    prjawal()


    Rohit
    Utkarsh
    Prajwal



    # flight_obj.flight_status()
    # flight_obj.get_mongo_flight_details()
    # flight_obj.mongo_insert_details()
