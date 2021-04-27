# Script Created by Rohit Yadav, Utkarsh
"""
Data collection from different source and import it from non-relation database to relational database.
Find useful insights from data processing
"""
from scripts import flights, Attractions

if __name__ == '__main__':
    # Utkarsh
    attraction_obj = Attractions.Attractions()
    attraction_obj.data_cleaning()

    # Rohit
    flight_obj = flights.Flights()

    flight_obj.collect_data()
    flight_obj.schedule_graph()
    flight_obj.arrival_delay()
    flight_obj.flight_status()
    flight_obj.common_insight()

    attraction_obj.flight_landing()
