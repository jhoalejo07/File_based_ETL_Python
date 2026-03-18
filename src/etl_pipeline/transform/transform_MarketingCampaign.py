import pandas as pd
from src.etl_pipeline.utils.SQL_with_Dataframes import SQl_df


class Transform:
    def __init__(self, raw_data: dict):
        """
        Transformation layer for Marketing Campaign.
        Receives raw data dictionary and executes SQL-like transformations.
        """

        self.dataframes = raw_data
        self.sql_df = SQl_df()  # SQL abstraction layer
        self.data = self._transform()  # Execute transformation pipeline

    # =====================================================
    # Transformation Logic
    # =====================================================
    def _transform(self) -> pd.DataFrame:
        """
        Hospital billing transformation pipeline.
        Implements numeric conversion, filtering, categorization and rollup.
        """

        # Normalize column names
        for filename, df in self.dataframes.items():
            self.dataframes[filename] = self.sql_df.rename_columns(df)

        # Dynamic unpacking of input datasets
        # Convert the dictionary of DataFrames into a list.
        # self.dataframes is a dictionary where:- keys: identifiers - values: pandas DataFrame
        df1, df2, df3 = list(self.dataframes.values())[:3]

        df1 = self.sql_df.df_addcolumn(df1, 'Brand', 'Nykaa')
        df2 = self.sql_df.df_addcolumn(df2, 'Brand', 'Purplle')
        df3 = self.sql_df.df_addcolumn(df3, 'Brand', 'Tira')

        df = self.sql_df.df_unionall([df1, df2, df3])

        df = self.sql_df.df_ToDate(df, 'Date')

        # Extract Time Features
        df['Month'] = df['Date'].dt.month
        df['Year'] = df['Date'].dt.year

        # Ensure BillAmount is numeric
        df = self.sql_df.convert_to_numeric(df, 'Revenue')

        # Business threshold filter
        df = self.sql_df.apply_filters(df, 'Revenue', '>=', 1000)

        # Projection
        df = self.sql_df.df_select_columns(
            df,
            ['Brand', 'Campaign_Type', 'Language', 'Revenue', 'Date', 'Month', 'Year']
        )

        # Aggregate count per grouping dimensions
        df = self.sql_df.df_groupby(
            df,
            ['Brand', 'Year', 'Campaign_Type'],
            'SumRevenue',
            'Revenue',
            "sum"
        )

        df = self.sql_df.df_orderby(df, ['Brand', 'Year', 'Campaign_Type', 'SumRevenue'])
       
        return df
