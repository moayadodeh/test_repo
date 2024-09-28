from dar.config.database import connect_postgres
from dar.helpers.common import read_sql_query
from typing import Union
from datetime import datetime,time

from dar.scripts.extract_from_postgres.resources.paths import select_info_tables_sql,select_conditions_sql,get_first_scrape_date_sql

import logging

class ExtractFromPostgres:

    def __init__(self):
        self.fact_table = 'info_posts'
        self.dim_table = 'info_date'

    def extract(self, start_date:str= None,end_date:str=None):
        self.start_date = start_date
        self.end_date = end_date
        ## create instance of connection
        #conn = connect_postgres()

        #with conn.cursor() as cur:
        query_main = read_sql_query(select_info_tables_sql).format(
                fact_table = self.fact_table,
                dim_table = self.dim_table)

        if not(type(start_date) and type(end_date))  == str:
            query_condition = ''

        else:
            if not(type(end_date)) == str:
                end_date = str(datetime.now())

            elif not(type(start_date)) == str:
                query = read_sql_query(get_first_scrape_date_sql).format(
                    fact_table = self.fact_table,
                    dim_table = self.dim_table
                )
                cur.execute(query)
                start_date = cur.fetchone()
            ## dates formatting
            start_date_object = datetime.strptime(start_date, "%d/%m/%Y")
            end_date_object = datetime.strptime(end_date, "%d/%m/%Y")

            start_date_formatted = start_date_object.strftime("%d/%m/%Y")
            end_date_formatted = end_date_object.strftime("%d/%m/%Y")


        query_condition = read_sql_query(select_conditions_sql).format(
            start_date = f'{start_date_formatted}',
            end_date = f'{end_date_formatted}'
        )
        logging.info(query_main + query_condition)
        #cur.execute(query_main + query_condition)
        
