SPARKPLUG_TO_SPARK = {
    1:  {"name": "Int8",     "cast_to": "byte"},
    2:  {"name": "Int16",    "cast_to": "short"},
    3:  {"name": "Int32",    "cast_to": "int"},
    4:  {"name": "Int64",    "cast_to": "long"},
    5:  {"name": "UInt8",    "cast_to": "short"},
    6:  {"name": "UInt16",   "cast_to": "int"},
    7:  {"name": "UInt32",   "cast_to": "long"},
    8:  {"name": "UInt64",   "cast_to": "decimal(20,0)"},
    9:  {"name": "Float",    "cast_to": "float"},
    10: {"name": "Double",   "cast_to": "double"},
    11: {"name": "Boolean",  "cast_to": "boolean"},
    12: {"name": "String",   "cast_to": "string"},
}

def cast_datatype(datatype: int):
    return SPARKPLUG_TO_SPARK[datatype]['cast_to']
