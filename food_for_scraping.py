import pandas as pd
import os
import sqlite3

from make_database import FinalDatabaze

#TODO

class FoodData(FinalDatabaze):
    def __init__(self):
        super().__init__()
        self.initiation_table = "iniciation_data"

    food_data_template = [
        ("Položka", "TEXT"), 
        ("Skutečné_jméno", "TEXT"), 
        ("Odkaz", "TEXT"), 
        ("Velikost_balení", "REAL"), 
        ("Druh_potraviny", "TEXT"), 
        ("Energetická_hodnota", "REAL"), 
        ("Bílkoviny", "REAL"),
        ("Sacharidy", "REAL"), 
        ("Cukry", "REAL"),
        ("Tuky", "REAL"), 
        ("Nasycené_mastné_kyseliny", "REAL"), 
        ("Trans_mastné_kyseliny", "REAL"), 
        ("Mononenasycené", "REAL"),
        ("Polynenasycené", "REAL"), 
        ("Vláknina", "REAL"), 
        ("Sůl", "REAL"), 
        ("Vápník", "REAL")
    ]

    initiation_data_template = [
        ("Položka", "TEXT"), 
        ("Odkaz", "TEXT"), 
        ("Velikost_balení", "REAL"), 
        ("Druh_potraviny", "TEXT"), 
    ]

    def create_database(self,project_db):

        if not os.path.isfile(project_db):
            conn = sqlite3.connect(project_db)
            conn.close()


    def create_table(self, db_name, template_name, table_name):
        columns_with_types = ", ".join([f"[{column_name}] {column_type}" for column_name, column_type in template_name])
        create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_with_types}, PRIMARY KEY("Položka")
        );
        '''

        with sqlite3.connect(db_name) as conn:
            conn.execute(create_table_sql)

    def get_items_for_scrape(self, project_db, initiation_table, food_table):
        query = f'''
        SELECT {initiation_table}.*
        FROM {initiation_table}
        LEFT JOIN {food_table} ON {initiation_table}.Položka = {food_table}.Položka
        WHERE {food_table}.Položka IS NULL
        '''
        
        with sqlite3.connect(project_db) as conn:
            df = pd.read_sql_query(query, conn)
        
        return df
    
    def edit_data(self,food_data_template,df):
        template_columns = [col[0] for col in food_data_template]
        new_df = pd.DataFrame(columns=template_columns)

        for col in df.columns:
            if col in new_df.columns:
                new_df[col] = df[col]

        return new_df

    def food_for_scraping(self):
        self.create_database(self.project_db)
        self.create_table(self.project_db,self.food_data_template,self.food_table)
        self.create_table(self.project_db,self.initiation_data_template,self.initiation_table)

        missing_items = self.get_items_for_scrape(self.project_db,self.initiation_table,self.food_table)
        format_missing_items = self.edit_data(self.food_data_template,missing_items)

        return format_missing_items

if __name__ == "__main__":
    show_data = FoodData().food_for_scraping()


