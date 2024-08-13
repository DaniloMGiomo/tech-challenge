import pandas as pd

class UpdateDatabase():
    def __init__(self):
        """
        Initializes the UpdateDatabase object by setting the local and ImportExport file paths, 
        and then reads the database into the object's dataframes.
        
        Parameters:
            None
        
        Returns:
            None
        """
        self.local_file = 'L_data.parquet'
        self.ImportExport_file = 'IE_data.parquet'
        self.read_database()
        
    def read_database(self):
        """
        Reads the local and ImportExport databases from their respective parquet files.

        Attempts to read the databases using  read_parquet function with the pyarrow engine and multi-threading.
        If the read operation fails, initializes the corresponding DataFrame as empty.

        Parameters:
            None

        Returns:
            None
        """
        try:
            self.df_local = pd.read_parquet(self.local_file, engine='pyarrow', use_threads=True)
        except:
            self.df_local = pd.DataFrame()
        try:
            self.df_ImportExport = pd.read_parquet(self.ImportExport_file, engine='pyarrow', use_threads=True)
        except:
            self.df_ImportExport = pd.DataFrame()
    
    def update_database_L(self, df):
        """
        Updates the local database by concatenating the provided DataFrame, sorting the data, 
        and then writing the updated data to the local parquet file.

        Parameters:
            df (pandas.DataFrame): The DataFrame to be concatenated with the local database.

        Returns:
            pandas.DataFrame: The updated local database.
        """
        self.df_local = pd.concat([self.df_local, df], ignore_index=True)
        self.df_local.sort_values(['opt', 'subopt', 'ano'], ascending=[True, True, True], inplace=True)
        self.df_local.to_parquet(self.local_file, engine='pyarrow', index=False, compression='gzip')
        return self.df_local
    
    def update_database_IE(self, df):
        """
        Updates the ImportExport database by concatenating the provided DataFrame, sorting the data, 
        and then writing the updated data to the ImportExport parquet file.

        Parameters:
            df (pandas.DataFrame): The DataFrame to be concatenated with the ImportExport database.

        Returns:
            pandas.DataFrame: The updated ImportExport database.
        """
        self.df_ImportExport = pd.concat([self.df_ImportExport, df], ignore_index=True)
        self.df_ImportExport.sort_values(['opt', 'Produto', 'ano'], ascending=[True, True, True], inplace=True)
        self.df_ImportExport.to_parquet(self.ImportExport_file, engine='pyarrow', index=False, compression='gzip')
        return self.df_ImportExport