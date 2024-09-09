import pandas as pd
import logging

from datetime import datetime

from Load import Load

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

class Transform:
    def __init__(self, df):
        self.df = df
        self.schema_frames = list()
        self.int_types = ['int8', 'int16', 'int32', 'int64']
        self.float_types = ['float16', 'float32', 'float64']
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.Load = Load()
    
    def clean_none_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
        self.logger.info("transform.clean_none_data: columns of no data removed")
        return df
        
    def normalize_string_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
        self.logger.info("transform.normalize_string_data: string data normalized")
        return df
        
    def normalize_numeric_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
        self.logger.info("transform.normalize_numeric_data: numeric data normalized")
        return df
    
    def reduce_string_schema(self, df: pd.DataFrame) -> dict:
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
        self.logger.info("transform.reduce_string_schema: schema created")
        return columns_schema
    
    def reduce_string_data(self, df: pd.DataFrame, columns_schema: dict) -> pd.DataFrame:
        """
        Reduce string data in a DataFrame by merging it with the schema dictionary provided.
        
        Parameters:
            df (pd.DataFrame): DataFrame to be reduced.
            columns_schema (dict): A dictionary of schemas for the string columns in the DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame with reduced string data.
        """
        for col, schema in columns_schema.items():
            name = f"silver/schema/schema_{col}.parquet"
            df_schema_base = self.Load.download_parquet(name)
            df_schema = pd.DataFrame(schema)
            if df_schema_base != None:
                col_names = df_schema_base['col'].unique()
                df_schema.query('col not in @col_names', inplace=True)
                if len(df_schema) > 0:
                    last_id = df_schema_base[df_schema_base.columns[0]].max()
                    df_schema.reset_index(drop=True, inplace=True)
                    df_schema.index += (last_id + 1)
                    df_schema = pd.concat([df_schema_base, df_schema], ignore_index=True)
                else:
                    df_schema = df_schema_base.copy()
            df = df.merge(df_schema, how='left', on=col)
            df.drop(columns=[col], axis=1, inplace=True)
            self.schema_frames.append((col, df_schema))
        self.logger.info("transform.reduce_string_data: string data reduced")
        return df
    
    def optimize_numeric_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
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
        self.schema_frames.append(('target', df))
        #
        for schema in self.schema_frames:
            key, df = schema
            self.logger.info(f"transform.optimize_numeric_datatypes: data type optimization started for target: {key}")
            # get initial dataframe memory size in bytes
            initial_memory = df.memory_usage().sum()    
            for col, dtype in df.dtypes.astype(str).to_dict().items():
                if 'int' in dtype:
                    for int_type in self.int_types:
                        df['type_test'] = df[col].astype(int_type)
                        if sum(df[col] - df['type_test']) == 0:
                            print(f'{col} data type changed from {dtype} to {int_type}')
                            df[col] = df[col].astype(int_type)
                            break
                elif 'float' in dtype:
                    for float_type in self.float_types:
                        df['type_test'] = df[col].astype(float_type)
                        if sum(df[col] - df['type_test']) == 0:
                            print(f'{col} data type changed from {dtype} to {float_type}')
                            df[col] = df[col].astype(float_type)
                            break
            df.drop(columns=['type_test'], axis=1, inplace=True)
            # get final memory size in bytes
            final_memory = df.memory_usage().sum()    
            percentual_memory_reduction = ((initial_memory - final_memory) / initial_memory) * 100
            self.logger.info(f"transform.optimize_numeric_datatypes: data type optimization completed for target: {key} with memory usage reduction: {percentual_memory_reduction}%")
            if key != 'target':
                name = f"silver/schema/schema_{col}.parquet"
                self.Load.upload_parquet(df, name)
            else:
                self.logger.info(f"transform.optimize_numeric_datatypes: completed data type optimization")
                return df
    
    def save_parquet(self, df: pd.DataFrame):
        """
        Save the transformed DataFrame to a parquet file.

        Parameters:
            df (pd.DataFrame): DataFrame to be saved.

        Notes:
            The file name will be the current date and time in the format
            'YYYYMMDDHHMMSS_transformed_data.parquet'.
        """
        name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_transformed_data"
        df.to_parquet(f'{name}.parquet', compression='gzip', index=False, engine='pyarrow')
        self.logger.info(f"transform.save_parquet: completed data type optimization")
    
    def process(self) -> pd.DataFrame:
        """
        Process the DataFrame by performing the following operations in order:
        1. remove columns with no data
        2. normalize string data
        3. normalize numeric data
        4. reduce string data
        5. optimize numeric datatypes
        6. save the transformed DataFrame to a parquet file.
        """
        
        df = self.clean_none_data(self.df)
        df = self.normalize_string_data(df)
        df = self.normalize_numeric_data(df)
        columns_schema = self.reduce_string_schema(df) # pensar na arquitetura disso la no S3
        df = self.reduce_string_data(df, columns_schema) # pensar na arquitetura disso la no S3
        df = self.optimize_numeric_datatypes(df)
        # self.save_parquet(df)
        return df

if __name__ == '__main__':
    # # get raw data
    # from Scraper import Scraper
    # scraper = Scraper()
    # df = scraper.get()
    df = pd.DataFrame()
    # transform data
    transform = Transform(df=df)
    transform.process()