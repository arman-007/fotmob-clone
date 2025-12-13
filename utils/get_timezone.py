from datetime import datetime
from tzlocal import get_localzone

def get_local_time_zone():
    local_timezone = get_localzone()

    local_time = datetime.now(local_timezone)

    return local_time.tzinfo