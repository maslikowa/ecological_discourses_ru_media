import datetime
import time
from typing import Any, List, Literal
import requests
from bs4 import BeautifulSoup
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import word_tokenize
import locale
import logging
import pandas as pd
import re
import os

locale.setlocale(locale.LC_ALL, "ru_RU.UTF-8")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[
                            logging.FileHandler("Logs/ria_logger_2022.log"),
                            logging.StreamHandler()
                        ]
)

key_words = ["Экология", "Окружающая среда", "Природа", "Экосистема",
            "Экологическое состояние", "Природные ресурсы", "Биоразнообразие",
            "Экологическая политика", "Экологический кризис", "Сохранение природы",
            "Энвайронментализм", "Устойчивое развитие", "Зеленая экономика"]

article_counter = 106479

class riaConnector:

    def check_response(self, response: requests.Response) -> BeautifulSoup:
        """
        Проверка статуса запроса
        """
        if response.status_code == 200:
            return self.brewListSoup(response)
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
        
    def get_another_block(self, date_time, id) -> BeautifulSoup:
        """
        Получаю новый блок новостей на основе 
        даты и айдишника статьи из последнего запроса
        """
        date = date_time.strftime("%Y%m%d")
        date_time = date_time.strftime("%Y%m%dT%H%M%S")
        url = f"https://ria.ru/services/{date}/more.html?id={id}&date={date_time}"
        response = requests.get(url, timeout=60)
        logging.info(msg=f"Got new date: {date_time}")
        return self.check_response(response=response)
    
    def brewListSoup(self, response: requests.Response) -> BeautifulSoup:
        soup = BeautifulSoup(response.text, "html.parser")
        return soup

class riaParser:

    def get_metadata(self, list_item) -> dict[str, Any] | datetime.datetime | Literal[False] | str:
        href = list_item.find('a')['href']
        try:
            id = re.findall(pattern=r'\d+', string=href.split("/")[4])[0]
        except IndexError:
            logging.error("IndexError occured, continue")
            return "continue"
        date_str = list_item.find('div', {"class" : "list-item__date"}).text

        if bool(re.search(pattern=r"\d .* \d{4}, \d{2}:\d{2}", string=date_str)) is False:
            return "continue"
        
        date_time = datetime.datetime.strptime(date_str, "%d %B %Y, %H:%M")
        text = self.get_link(href)


        if text == "continue":
            return "continue"
        if date_time >= datetime.datetime(2022, 1, 1):
            if text:
                row = {
                    "id" : [id],
                    "date" : [date_time.strftime("%d-%m-%Y %H:%M")],
                    "url" : [href],
                    "text" : [text]
                }
                return row
            else:
                return date_time
        else:
            return False
    
    def get_id(self, item_list):
        href = item_list.find('a')['href']
        id = re.findall(pattern=r'\d+', string=href.split("/")[4])[0]
        return id

    def get_link(self, url: str) -> str | None:
        """
        Создаем и возвращаем ссылку на статью
        """
        response = requests.get(url, timeout=60)

        soup = BeautifulSoup(response.text, "html.parser")
        global article_counter
        article_counter += 1
        logging.info(f"Уже посчитано: {article_counter}")

        return self.get_article_text(soup)
    
    def get_article_text(self, soup: BeautifulSoup) -> str | None:
        """
        Извлекаем текст с учетом нашего словаря
        """
        try:
            article = soup.find_all("div", attrs={"class" : "article__body js-mediator-article mia-analytics"})
            text = article[0].text.lower()
        except IndexError:
            return "continue"
        
        tokens = word_tokenize(text, language="russian")
        stemmer = SnowballStemmer("russian")
        stemmed_words = [stemmer.stem(word) for word in tokens]

        stemmed_key_words = self.keywords_stemmer(key_words, stemmer)

        if any(key_word in stemmed_words for key_word in stemmed_key_words):
            logging.info("Нашел экологическую статью!")
            return text
        else:
            return None

    def keywords_stemmer(self, key_words: List[str], stemmer: SnowballStemmer) -> list[str]:
        """
        Стемминг ключевых слов для дальнейшего поиска сходств
        """
        stemmer = SnowballStemmer("russian")
        new_words_list = []
        for words in key_words:
            word_list = words.split(" ")
            stemmed_words = [stemmer.stem(word) for word in word_list]
            sentence = ' '.join(stemmed_words)
            new_words_list.append(sentence)

        return new_words_list

    def process(self):
        """
        Основной блок программы
        """
        start_date = datetime.datetime(year=2022, month=4, day=24, hour=23, minute=59, second=0)
        id = "0"

        if os.path.isfile("Data/ria_data_2022.csv"):
            main_df = pd.read_csv("Data/ria_data_2022.csv")
        else:
            table_init = {
                "id" : [1],
                "date" : [datetime.datetime(2025,1,1).date()],
                "url" : ["url"],
                "text" : ["text"]
            }
            main_df = pd.DataFrame(table_init)

        flag = True
        try:
            while flag:
                soup = riaConnector().get_another_block(date_time=start_date, id=id)
                item_list = soup.find_all(name="div", attrs={"class" : "list-item"})
                time.sleep(1)

                if len(item_list) <= 1:
                    start_date = start_date - datetime.timedelta(minutes=start_date.minute+1)
                    continue

                for n in range(len(item_list)):
                    metadata = self.get_metadata(item_list[n])
                    if n == 19:
                        id = self.get_id(item_list=item_list[n])
                        
                    if isinstance(metadata, dict):
                        row = pd.DataFrame(metadata)
                        main_df = pd.concat([main_df, row], ignore_index=True)
                        start_date = datetime.datetime.strptime(metadata["date"][0], "%d-%m-%Y %H:%M")
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
                main_df.to_csv(path_or_buf="Data/ria_data_2022.csv", index=False)
                logging.info(f"Программа завершилась, проанализировав {article_counter} статей, последняя дата обращения {start_date}")

if __name__ == "__main__":
    ria = riaParser()
    ria.process()


