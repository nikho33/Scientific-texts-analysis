import os
import pandas as pd
import findspark

findspark.init()

from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql import functions as F

from PyPDF2 import PdfFileReader
from tika import parser  ## https://stackoverflow.com/questions/34837707/how-to-extract-text-from-a-pdf-file

import nltk
from nltk.stem import PorterStemmer
stemming = PorterStemmer()
from nltk.corpus import stopwords

# local = "local[*]"
# appName = "Scientific analysis app"
# configLocale = SparkConf().setAppName(appName).setMaster(local). \
#    set("spark.executor.memory", "4G"). \
#    set("spark.driver.memory", "4G"). \
#    set("spark.sql.catalogImplementation", "in-memory")
#
# spark = SparkSession.builder.config(conf=configLocale).getOrCreate()
# sc = spark.sparkContext
# sc.setLogLevel("ERROR")
#
print("hello")
# print("Hello ", spark.sparkContext.appName)

path = "./Data/MiniMicroTAS/"
#path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2018/PDFs/Papers/"
files = [f for f in os.listdir(path) if f.endswith(('.pdf', '.PDF'))]
nb_files = len(files)
print("Number of files to convert : ", nb_files)


def get_info(path):
    with open(path, 'rb') as f:
        pdf = PdfFileReader(f)
        info = pdf.getDocumentInfo()
        number_of_pages = pdf.getNumPages()
    # print("metadata: ", info)
    # info.author, info.creator, nfo.producer, info.subject, info.title
    return info

def identify_tokens(text):
    tokens = nltk.word_tokenize(text)
    # taken only words (not punctuation)
    token_words = [w for w in tokens if w.isalpha()]
    return token_words


def stem_list(my_list):
    stemmed_list = [stemming.stem(word) for word in my_list]
    return stemmed_list


def remove_stops(my_list):
    meaningful_words = [w for w in my_list if w not in stops]
    return meaningful_words

def cut_sections(my_list):
    ind_abstract = 0
    for word in my_list:
        if word == "abstract":
            break
        ind_abstract += 1
    ind_keywords = ind_abstract
    for word in my_list[ind_abstract:]:
        if word == "keywords":
            break
        ind_keywords += 1
    ind_text = ind_keywords
    for word in my_list[ind_keywords:]:
        if word == "introduction":
            break
        ind_text += 1
    #print(ind_abstract, ind_keywords, ind_text)
    # special case when original file is not organized like expected (i.e. with section words abstract, keywords, introduction)
    if ind_abstract == len(my_list):
        print("warming : no abstract found ! ")
        return [my_list[:], [], my_list[:]]
    return [my_list[ind_abstract+1:ind_keywords], my_list[ind_keywords+1:ind_text], my_list[ind_text+1:]]


columns = ['filename', 'title', 'author', 'date', 'abstract', 'keywords', 'text']
dataframe = pd.DataFrame(columns=columns)

i= 0
for f in files:
    info = get_info(path + f)                       # get metadata (e.g. title, author, ...) of the pdf with PdfFileReader
    text = parser.from_file(path + f)               # parse the text of the pdf with Tika package function
    text = identify_tokens(text["content"].lower()) # convert raw text in a list of words
    stops = set(stopwords.words("english"))
    text = remove_stops(text)                   # remove stop words
    text = cut_sections(text)     # cut the raw text in abstract - keywords - text (in this order !)
    text[0] = stem_list(text[0])                # stemming of the abstract
    text[2] = stem_list(text[2])                # stemming of the text
    new_entry = {"filename": f, "title": info.title, "author": info.author, "date": 2018, "abstract": text[0],
                 "keywords": text[1], "text": text[2]}
    dataframe = dataframe.append(new_entry, ignore_index=True)
    i += 1
    print (i, ' out of ', nb_files, ' done ')


#print(dataframe)
dataframe.to_csv(path+"datas.csv" ,index=False)
