"""
This file contains `faststream docs` commands.
These commands are referenced in guide file.
These commands are also imported and used in tests under tests/ directory.
"""


gen_asyncapi_json_cmd = """
faststream docs gen basic:app
"""

gen_asyncapi_yaml_cmd = """
faststream docs gen --yaml basic:app
"""

asyncapi_serve_cmd = """
faststream docs serve basic:app
"""

asyncapi_serve_json_cmd = """
faststream docs serve asyncapi.json
"""

asyncapi_serve_yaml_cmd = """
faststream docs serve asyncapi.yaml
"""
