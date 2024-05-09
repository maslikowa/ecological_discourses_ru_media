import datetime
import time
from typing import Any, List, Literal
import requests
from requests.exceptions import ChunkedEncodingError
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import word_tokenize
import re
import os

import logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[
                            logging.FileHandler("Logs/tass_logger_2021.log"),
                            logging.StreamHandler()
                        ]
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 "
                  "Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8,af;q=0.7"
}

key_words = ["Экология", "Окружающая среда", "Природа", "Экосистема",
            "Экологическое состояние", "Природные ресурсы", "Биоразнообразие",
            "Экологическая политика", "Экологический кризис", "Сохранение природы",
            "Энвайронментализм", "Устойчивое развитие", "Зеленая экономика"]

article_counter = 262521

class tassConnector:

    def get_response(self, date_time) -> List[dict]:
        """
        Выполняем запрос по ссылке
        """
        date_time = date_time.strftime("%Y-%m-%dT%H:%M:%S.%f")
        url = f"https://tass.ru/tbp/api/v1/search?limit=20&last_es_updated_dt={date_time}&lang=ru&sort=-es_updated_dt"
        response = requests.get(url=url, timeout = 60)
        logging.info(f"Got new date: {date_time}")

        return self.check_response(response=response)
    

    def check_response(self, response: requests.Response) -> List[dict]:
        """
        Проверка статуса запроса
        """
        if response.status_code == 200:
            return response.json()['result']
        else:
            error_message = f"Your request returned {response.status_code} status code."
            if response.status_code == 404:
                error_message += " The requested resource wasn't found."
            elif response.status_code == 500:
                error_message += " The server encountered an internal error."

            logging.exception(f"Exception occured with status code: {0}: {1}".format(
                response.status_code, error_message
            ))
            raise Exception(error_message)
        
    def get_metadata(self, response: List, number: int) -> dict[str, Any] | Literal[False] | None | datetime.date | str:
        """
        Получение данных о статье
        """
        result = response[number]
        if bool(re.search(pattern=r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}", string=result['es_updated_dt'])) is False:
            return "continue"
        
        current_date = datetime.datetime.strptime(result['es_updated_dt'], "%Y-%m-%dT%H:%M:%S.%f")
        path = result["url"]

        if path.split('/')[1] == "nauka" or "http" in path:
            return "continue"

        if current_date < datetime.datetime(2022, 12, 31):
            text = NewsParser().get_link(path=path)

            if text == "continue":
                return "continue"

            if text:
                row = {
                    "id" : [result["id"]],
                    "date" : [current_date],
                    "path" : [path],
                    "type" : [result["type"]],
                    "text" : [text]
                }
                return row
            else:
                return current_date
        else:
            return False



class NewsParser:

    def keywords_stemmer(self, key_words: List[str], stemmer: SnowballStemmer) -> list[str]:
        """
        Стемминг ключевых слов для дальнейшего поиска сходств
        """
        new_words_list = []
        for words in key_words:
            word_list = words.split(" ")
            stemmed_words = [stemmer.stem(word) for word in word_list]
            sentence = ' '.join(stemmed_words)
            new_words_list.append(sentence)

        return new_words_list


    def get_link(self, path: str) -> str | None:
        """
        Создаем и возвращаем ссылку на статью
        """
        article = "https://tass.ru" + path
        try:
            response = requests.get(article, timeout=60)
        except ChunkedEncodingError:
            return None

        if len(response.history) > 1:
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        global article_counter
        article_counter += 1
        logging.info(f"Уже посчитано: {article_counter}")

        return self.get_article_text(soup=soup)
    
    def get_article_text(self, soup: BeautifulSoup) -> str | None:
        """
        Извлекаем текст с учетом нашего словаря
        """
        try:
            article = soup.find(name="article").find_all(string=True) # type: ignore
        except AttributeError:
            return "continue"

        # В статьях обычно содержиться блок "Читайте также", 
        # он нас не интересует, поэтому удаляем его из массива
        read_more = np.where(np.atleast_1d(article) == "Читайте также")
        if len(read_more[0]) > 0:
            for index in read_more[0]:
                index = int(index)
                del article[index:index + 1]

        pure_text = " ".join(article).lower()
        tokens = word_tokenize(pure_text, language="russian")
        stemmer = SnowballStemmer("russian")
        stemmed_words = [stemmer.stem(word) for word in tokens]

        stemmed_key_words = self.keywords_stemmer(key_words, stemmer)

        if any(key_word in stemmed_words for key_word in stemmed_key_words):
            logging.info("Нашел экологическую статью!")
            return pure_text
        else:
            return None        
    
    def process (self):
        """
        Основной блок программы
        """
        # start_date = datetime.datetime(2023,12,31,23,59,59)
        start_date =datetime.datetime(year=2021, month=2, day=6, hour=8, minute=43, second=17)

        if os.path.isfile("Data/tass_data_2021.csv"):
            main_df = pd.read_csv("Data/tass_data_2021.csv")
        else:
            table_init = {
                "id" : [1],
                "date" : [datetime.datetime(2025,1,1).date()],
                "path" : ["path"],
                "type" : ["type"],
                "text" : ["text"]
            }
            main_df = pd.DataFrame(table_init)

        flag = True
        
        try:
            while flag:
                responses = tassConnector().get_response(start_date)
                time.sleep(3)

                for n in range(len(responses)):
                    metadata = tassConnector().get_metadata(response = responses,number = n)
                    if isinstance(metadata, dict):
                        row = pd.DataFrame(metadata)
                        main_df = pd.concat([main_df, row], ignore_index=True)
                        start_date = metadata["date"][0]
                    elif isinstance(metadata, datetime.datetime):
                        start_date = metadata
                    elif isinstance(metadata, str):
                        continue
                    else:
                        flag = metadata
                        break

        except Exception as err:
            logging.error(f"An error occured: {err}" , exc_info = True)
        
        finally:
                main_df.to_csv(path_or_buf="Data/tass_data_2021.csv", index=False)
                logging.info(f"Программа завершилась, проанализировав {article_counter} статей, последняя дата обращения {start_date}")

if __name__ == "__main__":
    tass = NewsParser()
    tass.process()