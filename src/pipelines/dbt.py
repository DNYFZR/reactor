import os
from numpy import isin
from sqlalchemy import create_engine, text 
from sqlalchemy.engine import URL

class dbt:
    '''This class is for executing queies on databases. '''
    
    def __init__(self, 
        db:      str = 'ATP_tour_data', 
        user:    str = 'postgres', 
        pw:      str = os.getenv('PG_PW'), 
        server:  str = 'host.docker.internal', 
        port:    int =  5432, 
        db_type: str = 'postgres') -> None:

        # Select engine connection string
        if db_type == 'postgres':
            conn_str =  URL.create(drivername="postgresql+psycopg2", username=user, password=pw, host=server, database=db, port=port, )
    
        elif db_type == 'sqlserver':
            conn_str = URL.create(drivername="mssql+pyodbc", username=user, password=pw, host=server, database=db, port=port, 
                                    query={"driver": "ODBC Driver 17 for SQL Server", "authentication": "ActiveDirectoryIntegrated"}, )
        else:
            raise ValueError(f'''Unfortunatley {db_type} is not supported at the moment.''')
        
        # Build engine
        self.engine = create_engine(url=conn_str)


    def execute(self, query:str, return_output: bool = True):
        ''' Executes the provided SQL query on the database. 
            If return_output = True then the output will be returned as a JSON object - default = True.'''
        
        # Apply any restrictions to excecutions
        restricted_words = ['delete']
        for i in query.split(' '):
            if isin(i, restricted_words):
                raise ValueError(f'''Forbidden word in query : {i.capitalize()}''')

        # Run the query 
        with self.engine.connect() as cursor:
            raw_output = cursor.execute(text(query))
            
            if return_output:
                # Extract table column names & row data 
                column_names = [col for col in raw_output.keys()]
                output = [row for row in raw_output]

                # Return dictionary of lists  
                return {col : [i[n] for i in output] for n, col in enumerate(column_names)}
    
    @staticmethod
    def test(baseline_item, test_item):
        '''Test item matches baseline'''
        if test_item != baseline_item:
            raise AssertionError

if __name__ == '__main__':
    import pandas as pd
    
    query = '''
        WITH filtered_table as (
            SELECT *
            FROM matches
            WHERE (winner_name = 'Rafael Nadal' AND loser_name = 'Novak Djokovic') OR 
                (winner_name = 'Novak Djokovic' AND loser_name = 'Rafael Nadal')  
        )

        SELECT tourney_name, SUM(minutes) as total_mins
        FROM filtered_table
        GROUP BY tourney_name
        ORDER BY total_mins DESC
        LIMIT 10 ;
    '''
    print(pd.DataFrame(dbt(server='localhost').execute(query=query)))