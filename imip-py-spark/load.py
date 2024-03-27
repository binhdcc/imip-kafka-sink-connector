import json

# Example JSON string
json_string = '{"ID": 1 }'

# Parse JSON string to a dictionary
data = json.loads(json_string)

# Extract keys
keys = data.keys()

# Extract values
values = data.values()

print("Keys:", keys)
print("Values:", values)