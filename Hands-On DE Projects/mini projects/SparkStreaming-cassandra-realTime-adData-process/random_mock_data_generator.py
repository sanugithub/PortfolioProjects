import json
import random
import datetime

# Create a list of random data
data = []
for i in range(1500):
    # Create an ISO string
    iso_string = "2023-08-23T12:01:05Z"

    # Convert the ISO string to a datetime object
    datetime_object = datetime.datetime.fromisoformat(iso_string)
    #print(datetime_object)

    # Create a timedelta object representing few seconds
    timedelta_object = datetime.timedelta(seconds=i)

    # Add the timedelta object to the datetime object
    datetime_object += timedelta_object

    # Convert the datetime object back to an ISO string
    new_iso_string = datetime_object.isoformat().replace('+00:00', 'Z')

    # Print the new ISO string
    #print(datetime_object)
    print(new_iso_string)

    ad_ids = [12345, 54321, 13245, 54231, 14235, 53241, 15234, 43251, 67891, 19876, 68791, 19786, 69781, 18796]

    data.append({
        "ad_id": str(random.choice(ad_ids)),
        "timestamp": new_iso_string,
        "clicks": random.randint(2, 52),
        "views": random.randint(15, 167),
        "cost": round(random.uniform(50.75, 112.66), 2)
    })

# Write the data to a JSON file
with open("mock_ads_data.json", "w") as f:
    json.dump(data, f)