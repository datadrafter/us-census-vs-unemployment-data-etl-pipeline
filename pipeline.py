import sqlite3
import pandas as pd

class Pipeline(object):
    def __init__(self):
        self.population = None
        self.unemployment = None

    def extract(self):
        """
        Data source (based on data from data.gov)
        description:
            table1: https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2021/CBSA-EST2021-ALLDATA.pdf
            table2: https://www.ers.usda.gov/data-products/county-level-data-sets/download-data
        """
        #data for population estimation
        url_popul_est = 'data/cbsa-est2017-alldata.csv'
        #data for unemployment
        url_unemployment = 'data/unemployment.xls'

        self.population = pd.read_csv(url_popul_est, encoding='ISO-8859-1')
        self.unemployment = pd.read_excel(url_unemployment, skiprows=7)

    def transform(self):
        #formatting population datset

        #keep the columns only that are required for our end product i.e, the column that contains year-population-estimate and index names
        pop_idx = ['CBSA', 'MDIV', 'STCOU', 'NAME', 'LSAD']
        pop_cols = [c for c in self.population.columns if c.startswith('POPEST')]
        population = self.population[pop_idx + pop_cols].copy()

        #melt, "unpivot" the yearly rate value (from wide format 'columns' to long format 'rows')
        self.population = population.melt(id_vars=pop_idx,
                                          value_vars=pop_cols,
                                          var_name='YEAR',
                                          value_name='POPULATION_EST')

        #fix column values
        self.population['YEAR'] = self.population['YEAR'].apply(
        lambda x: x[-4:])   #eg: POPESTIMATE2010 -> 2010

        #formatting unemployment dataset
    
        #keep the columns only that are required for our end product i.e, the column that contains year-population-estimate and index names
        unemp_idx = ['FIPStxt', 'State', 'Area_name']
        unemp_cols = [c for c in self.unemployment.columns if c.startswith('Unemployment_rate')]
        unemployment = self.unemployment[unemp_idx + unemp_cols].copy()

        #melt, "unpivot" the yearly rate value (from wide format 'columns' to long format 'rows')
        self.unemployment = unemployment.melt(id_vars=unemp_idx,
                                                value_vars=unemp_cols,
                                                var_name='Year',
                                                value_name='Unemployment_rate')

        # fix columns values
        self.unemployment = self.unemployment.round(1) #set precision to .1
        self.unemployment['Year'] = self.unemployment['Year'].apply(
            lambda x: x[-4:]) #eg: remove prefix "Unemployment_rate_XXXX" -> XXXX

    def load(self):
        db = DB()
        self.population.to_sql('population', db.conn, if_exists='append',index=False)
        self.unemployment.to_sql('unemployment',db.conn, if_exists='append',index=False)

class DB(object):
    def __init__(self,db_file='db.sqlite'):
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()
        self.__init__db()

    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def __init__db(self):
        table1 = f"""CREATE TABLE IF NOT EXISTS population(
            CBSA INTEGER,
            MDIV REAL,
            STCOU INTEGER,
            NAME TEXT,
            LSAD TEXT,
            YEAR INTEGER,
            POPULATION_EST INTEGER
        );"""

        table2 = f"""CREATE TABLE IF NOT EXISTS unemployment(
            FIPStxt INTEGER,
            State TEXT,
            Area_name TEXT,
            Year INTEGER,
            unemployment_rate REAL
        );"""

        self.cur.execute(table1)
        self.cur.execute(table2)

if __name__ == '__main__':
    pipeline =Pipeline()
    print('Data Pipeline Created')
    print('\t extracting data from source.....')
    pipeline.extract()
    print('\t formatting and transforming data.....')
    pipeline.transform()
    print('\t loading into database.....')
    pipeline.load()

    print('\nDone. See: results in db.sqlite"')