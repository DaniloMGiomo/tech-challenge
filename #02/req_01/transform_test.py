from scraper import Scraper
import pandas as pd

def get_data() -> pd.DataFrame:
    """
    Retrieve data from web scraper.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the scraped data.
    """
    scraper = Scraper()
    df = scraper.get()
    return df

def clean_none_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns with no data from the DataFrame.

    Parameters:
        df (pd.DataFrame): DataFrame to be cleaned.

    Returns:
        pd.DataFrame: DataFrame with columns of no data removed.
    """
    to_drop = list()
    for col in df.columns:
        if len(df[col].value_counts()) < 1:
            to_drop.append(col)
    # 
    df.drop(to_drop, axis=1, inplace=True)
    return df

def normalize_string_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize string data in a DataFrame by replacing
    dots and commas with empty strings, and replacing
    multiple spaces with a single space. Also, upper
    case all string values.

    Parameters:
        df (pd.DataFrame): DataFrame to be normalized.

    Returns:
        pd.DataFrame: DataFrame with normalized string data.
    """
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x).replace('.', '').replace(',', '.').replace('   ', ' ').replace('  ', ' ').upper())
    return df

def normalize_numeric_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize numeric data in a DataFrame by converting
    all numeric values to either float or int.

    Parameters:
        df (pd.DataFrame): DataFrame to be normalized.

    Returns:
        pd.DataFrame: DataFrame with normalized numeric data.
    """
    for col in df.columns:
        if df[col].values[0][0].isnumeric():
            df[col] = df[col].apply(lambda x: float(x) if '.' in str(x) else int(x))
    return df

def reduce_string_schema(df: pd.DataFrame) -> dict:
    """
    Reduce string data in a DataFrame by creating a dictionary of
    schemas where each key is a column name and each value is a list
    of dictionaries where each dictionary has two keys: 'Id{column name}'
    and '{column name}'. The value for 'Id{column name}' is an incremented
    integer for each unique value in the column and the value for
    '{column name}' is the actual value from the column.

    Parameters:
        df (pd.DataFrame): DataFrame to be reduced.

    Returns:
        dict: A dictionary of schemas for the string columns in the DataFrame.
    """
    columns_schema = dict()
    for col, dtype in df.dtypes.astype(str).to_dict().items():
        if dtype == 'object':
            ID_name = f'Id{col.capitalize()}'
            columns_schema[col] = list()
            for i, val in enumerate(df[col].unique()):
                columns_schema[col].append({
                    ID_name : i + 1,
                    col : val
                        })
    return columns_schema

def reduce_string_data(df: pd.DataFrame, columns_schema: dict) -> pd.DataFrame:
    """
    Reduce string data in a DataFrame by merging it with the schema dictionary provided.
    
    Parameters:
        df (pd.DataFrame): DataFrame to be reduced.
        columns_schema (dict): A dictionary of schemas for the string columns in the DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with reduced string data.
    """
    for col, schema in columns_schema.items():
        df_schema = pd.DataFrame(schema)
        df = df.merge(df_schema, how='left', on=col)
        df.drop(columns=[col], axis=1, inplace=True)
        df_schema.to_parquet(f'schema_{col}.parquet')
    return df

def optimize_numeric_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize numeric data types in a DataFrame by finding the smallest type that can
    represent all values in a column.

    Parameters:
        df (pd.DataFrame): DataFrame to be optimized.

    Returns:
        pd.DataFrame: DataFrame with optimized numeric data types.

    Notes:
        This function iterates over each column of a DataFrame and tries to convert
        it to the smallest possible numeric data type. If the column is an integer
        type, it will be converted to one of ['int8', 'int16', 'int32', 'int64'] in
        order of increasing size. If the column is a floating point type, it will
        be converted to one of ['float16', 'float32', 'float64'] in order of increasing
        size. If the column cannot be converted to any of the above types, it will
        be left unchanged. The function also prints the initial and final memory
        usage of the DataFrame, as well as the percentage memory usage reduction.
    """
    int_types = ['int8', 'int16', 'int32', 'int64']
    float_types = ['float16', 'float32', 'float64']
    # get initial dataframe memory size in bytes
    initial_memory = df.memory_usage().sum()    
    for col, dtype in df.dtypes.astype(str).to_dict().items():
        if 'int' in dtype:
            for int_type in int_types:
                df['type_test'] = df[col].astype(int_type)
                if sum(df[col] - df['type_test']) == 0:
                    print(f'{col} data type changed from {dtype} to {int_type}')
                    df[col] = df[col].astype(int_type)
                    break
        elif 'float' in dtype:
            for float_type in float_types:
                df['type_test'] = df[col].astype(float_type)
                if sum(df[col] - df['type_test']) == 0:
                    print(f'{col} data type changed from {dtype} to {float_type}')
                    df[col] = df[col].astype(float_type)
                    break
    df.drop(columns=['type_test'], axis=1, inplace=True)
    # get final memory size in bytes
    final_memory = df.memory_usage().sum()    
    print(f'Initial memory usage: {initial_memory} bytes')
    print(f'Final memory usage: {final_memory} bytes')
    percentual_memory_reduction = ((initial_memory - final_memory) / initial_memory) * 100
    print(f'Memory usage reduction: {percentual_memory_reduction}%')
    return df

def save_parquet(df: pd.DataFrame):
    df.to_parquet('data.parquet', compression='gzip', index=False, engine='pyarrow')

df = get_data()
df = clean_none_data(df)
df = normalize_string_data(df)
df = normalize_numeric_data(df)
columns_schema = reduce_string_schema(df)
df = reduce_string_data(df, columns_schema)
df = optimize_numeric_datatypes(df)
save_parquet(df)