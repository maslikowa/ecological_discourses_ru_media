import string
import nltk
from typing import List
from nltk import word_tokenize
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
import constants
import re


class textFormatter():

    def regex_remove_start(self, text: str) -> str:
        """
        Убираем начало статьи с датой и новостным агенством
        """
        re_pattern = re.compile(r"(.*,\s\d{1,2}\s\w{3,8}\.\s\/тасс\/\.)|(.*,\s\d{1,2}\s\w{3}\s\W\sриа\sновости\.)")
        text = re_pattern.sub("", text)
        return self.remove_punctuation_digits(text)


    def remove_punctuation_digits(self, text: str) -> str:
        """
        Убираем пунктуацию и числа из нашего текста
        """
        translator = str.maketrans('', '', string.punctuation + string.digits)
        return text.translate(translator)
    
    def stem_keywords(self, keywords_dict: dict[str, List[str]] = constants.KEY_WORDS, russian_stopwords: List[str] = constants.RUSSIAN_STOPWORDS) -> dict[str, List[str]]:
        """
        Стемминг ключевых слов для дальнейшего поиска сходств
        """  
        stemmed_keywords = {}

        for category, words_list in keywords_dict.items():
            stemmed_category_keywords = []
            for word in words_list:
                # Токенизация слова
                tokens = word_tokenize(word)
                # Применение SnowballStemmer к каждому токену
                stemmed_words = [constants.STEMMER.stem(token) for token in tokens if token not in russian_stopwords]
                # Сбор снова в строку
                stemmed_word = ' '.join(stemmed_words)
                stemmed_category_keywords.append(stemmed_word)
            stemmed_keywords[category] = stemmed_category_keywords
        return stemmed_keywords
    
    def get_article_text(self, article:str, stemmed_key_words:dict[str, List[str]], russian_stopwords: List[str] = constants.RUSSIAN_STOPWORDS) -> str:
        """
        Проверяем текст на принадлежность к категории.
        Возвращаем категорию текста
        """
        tokens = word_tokenize(text=article, language="russian")
        stemmed_words = [constants.STEMMER.stem(word) for word in tokens if word not in russian_stopwords]

        for category, words_list in stemmed_key_words.items():
            for key_word in words_list:
                if key_word in stemmed_words:
                    return category
        
        return "No category"
    
class Bubbles:

    def create_bubble(self, articles: pd.Series, russian_stopwords: List[str] = constants.RUSSIAN_STOPWORDS):
        """
        Предобрабатываем текст и рисуем пузырь слов
        """
        # Приводим весь столбец к строковому типу
        all_texts = articles.to_string()
        all_texts = textFormatter().remove_punctuation_digits(all_texts)
        tokens = word_tokenize(all_texts)
        words = nltk.Text(tokens)
        # Удаляем стоп-слова
        filtered_words = [word.strip() for word in words if word not in russian_stopwords]
        # Создаем облако слов
        wordcloud = WordCloud(width=800, height=400, background_color='white', min_word_length=3, stopwords= constants.BUBBLE_STOPWORDS, min_font_size=8).generate(' '.join(filtered_words))
        # Отображаем график
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.show()



        