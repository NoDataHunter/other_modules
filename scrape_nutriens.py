import requests
from bs4 import BeautifulSoup
import json
import sqlite3

from make_database import FinalDatabaze

#TODO

class ScrappeNutriens(FinalDatabaze):
    def __init__(self,scraped_df):
        super().__init__()
        self.scraped_df = scraped_df

    def get_json_content(self,row):
        response = requests.get(row["Odkaz"])
        html_content = response.text

        soup = BeautifulSoup(html_content, "html.parser")
        script_tag = soup.find("script", type="application/ld+json", string=lambda text: text and "'@type': 'Dataset'" in text).string
        json_content = json.loads(script_tag)

        return json_content


    def Download_from_htlm(self,template_for_item):

        for position , row in template_for_item.iterrows():

            json_content = self.get_json_content(row)
            template_for_item.at[position, "Skutečné_jméno"] = json_content["name"]
            
            keywords = json_content.get("keywords", [])
            keywords = [item.replace("Nasycené mastné kyseliny", "Nasycené_mastné_kyseliny") for item in keywords]
            keywords = [item.replace("Energetická hodnota", "Energetická_hodnota") for item in keywords]

            for item in keywords:
                key, value = item.split(" : ")
                template_for_item.at[position, key] = value
                template_for_item.at[position, key] = float("".join([char for char in value if char.isdigit() or char == ","]).replace(",", "."))

        return template_for_item

    def data_to_db(self, download_nutriens):
        with sqlite3.connect(self.project_db) as conn:
            download_nutriens.to_sql("food_data", conn, if_exists="append", index=False)

    def execute_flow(self):
        download_nutriens = self.Download_from_htlm(self.scraped_df)
        self.data_to_db(download_nutriens) 

if __name__ == "__main__":
    execution = ScrappeNutriens().execute_flow()
