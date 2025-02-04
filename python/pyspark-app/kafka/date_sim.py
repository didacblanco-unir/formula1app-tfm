
import time
from datetime import datetime, timedelta, timezone
from producers import produce_position, produce_car_data

if __name__ == '__main__':
    start_time_str = "2023-03-05T15:00:00+00:00"
    simulated_time = datetime.fromisoformat(start_time_str)
    
    while True:
        print("Hora simulada:", simulated_time.isoformat())
        
        produce_position(simulated_time)
        produce_car_data(simulated_time)

        time.sleep(2)

        simulated_time += timedelta(seconds=30)
