from sqlalchemy.types import NVARCHAR, INT, BIGINT, TEXT, VARCHAR, DATE, DATETIME, DECIMAL, SMALLINT
def cor_datatype(key, *args):
    if len(args)==2:
        length, scale = args
    elif len(args)==1:
        length = args[0]
    
    data_types = {
        "NVARCHAR": NVARCHAR,
        "INT": INT,
        "BIGINT": BIGINT,
        "TEXT":TEXT,
        "DATE": DATE,
        "DATETIME": DATETIME,
        "DECIMAL": DECIMAL,
    }
    if key in ['NVARCHAR']:
        return data_types[key](int(length))
    elif key in ["DECIMAL"]:
        return data_types[key](int(length), int(scale))
    else:
        return data_types[key]