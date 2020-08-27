"""

@author: kshock
"""


from sqlalchemy import create_engine


class DataBaseConn(object):
    def __init__(self, user, passworld, host, port, db, dataframe_to_load):
        '''
        

        Parameters
        ----------
        user : string
        passworld : string
        host : string
        port : string
        db : string
        dataframe_to_load : pd.DataFrame

        Returns
        -------
        Crates user database parameters.

        '''
        self.user = user
        self.passworld = passworld
        self.host = host
        self.port = port
        self.db = db
        self.dataframe_to_load = dataframe_to_load
        
    def load_to_sqlite(self, table_name):
        '''


        Parameters
        ----------
        table_name : string

        Returns
        -------
        Loads pandas DataFrame to sqlite database.

        '''
        engine = create_engine('sqlite:///{}:{}@{}:{}/{}'.format(self.user,
                                                                 self.passworld,
                                                                 self.host,
                                                                 self.port,
                                                                 self.db))
        self.dataframe_to_load.to_sql(table_name, con=engine, if_exists='replace')
        
    
                                
            