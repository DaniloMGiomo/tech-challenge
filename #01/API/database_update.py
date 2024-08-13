import pandas as pd

class UpdateDatabase():
    def __init__(self):
        self.local_file = 'L_data.parquet'
        self.ImportExport_file = 'IE_data.parquet'
        self.read_database()
        
    def read_database(self):
        try:
            self.df_local = pd.read_parquet(self.local_file, engine='pyarrow', use_threads=True)
        except:
            self.df_local = pd.DataFrame()
        try:
            self.df_ImportExport = pd.read_parquet(self.ImportExport_file, engine='pyarrow', use_threads=True)
        except:
            self.df_ImportExport = pd.DataFrame()
    
    def update_database_L(self, df):
        self.df_local = pd.concat([self.df_local, df], ignore_index=True)
        self.df_local.sort_values(['opt', 'subopt', 'ano'], ascending=[True, True, True], inplace=True)
        self.df_local.to_parquet(self.local_file, engine='pyarrow', index=False, compression='gzip')
        return self.df_local
    
    def update_database_IE(self, df):
        self.df_ImportExport = pd.concat([self.df_ImportExport, df], ignore_index=True)
        self.df_ImportExport.sort_values(['opt', 'Produto', 'ano'], ascending=[True, True, True], inplace=True)
        self.df_ImportExport.to_parquet(self.ImportExport_file, engine='pyarrow', index=False, compression='gzip')
        return self.df_ImportExport