import pyarrow.parquet as pq
import pyarrow as pq
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

def ex_one():
    a_array = pa.array([42])
    b_array = pa.array(["ahmed"])

    fields = [
        pa.array(['foo']), pa.array(['bar'])
    ]
    c_struct = pa.StructArray.from_arrays(fields, ['a', 'b'])

    # List of Struct
    values = c_struct
    offsets = pa.array([0,1])
    d_list = pa.ListArray.from_arrays(offsets, values)
    
    # map_data = [[('x', 1), ('y', 0), ('z', 2)]]
    # ty = pa.map_(pa.string(), pa.int64())
    # map_array = pa.array(map_data, type=ty)
    #
    # Map with key: string, value: Struct
    map_array = pa.MapArray.from_arrays([0, 1], ['x'], c_struct)
    
    names = ['a', 'b', 'c', 'd', 'e']
    table = pa.Table.from_arrays([a_array, b_array, c_struct, d_list, map_array], names = names)
    print(table.to_pandas())
    
    table_to_parquet(table)

def ex_two():    
    df = pd.DataFrame(
        {
            'one': [-1, 0.0, 2.5],
            'two': ['foo', 'bar', 'baz'],
            'three': [{'ten': '10'}, {'twenty': '20'}, {'thirty': '30'}]
            
        },
        index = list('abc')
    )
    print(df)
    table = pa.Table.from_pandas(df)

def table_to_parquet(table):
    pq.write_table(table, "/tmp/arrow_parquet.parquet")    

if __name__ == "__main__":
    ex_one()
