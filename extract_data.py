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
print("Hello here")
# print("Hello ", spark.sparkContext.appName)

date = 2012
if date == 2009:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2009/Pdf/"
elif date == 2010:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2010/PDFs/Papers/"
elif date == 2011:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2011/PDFs/Papers/"
elif date == 2012:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2012/pdf/"
elif date == 2013:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2013/PDFs/Papers/"
elif date == 2014:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2014/PDFs/Papers/"
elif date == 2015:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2015/PDFs/Papers/"
elif date == 2016:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2016/PDFs/Papers/"
elif date == 2017:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2017/PDFs/Papers/"
elif date == 2018:
    path = "C:/Users/nicol/Desktop/MicroTAS/MicroTAS2018/PDFs/Papers/"
else:
    path = "./Data/MiniMicroTAS/"

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


def cut_sections(my_list, format="MicroTAS"):
    if format == "MicroTAS":
        ind_abstract = 0
        for word in my_list:
            if word == "abstract":
                break
            ind_abstract += 1
        ind_keywords = ind_abstract
        for word in my_list[ind_abstract:]:
            if word == "keywords" or word == "keyword":
                break
            ind_keywords += 1
        ind_text = ind_keywords
        for word in my_list[ind_keywords:]:
            if word == "introduction":
                break
            ind_text += 1
        # print(ind_abstract, ind_keywords, ind_text)
        # special case when original file is not organized like expected (i.e. with section words abstract, keywords, introduction)
        if ind_abstract == len(my_list):
            print("warning : no abstract found ! ")
            return [my_list[:], [], my_list[:]], -1
        return [my_list[ind_abstract + 1:ind_keywords], my_list[ind_keywords + 1:ind_text], my_list[ind_text + 1:]], 0
    else:
        return [my_list[:], [], my_list[:]], -1


columns = ['filename', 'title', 'author', 'date', 'abstract', 'keywords', 'text']
dataframe = pd.DataFrame(columns=columns)

i = 0
error_title = []
error_author = []
error_text = []
for filename in files:
    try:
        info = get_info(path + filename)  # get metadata (e.g. title, author, etc) of pdf with PdfFileReader
    except:
        info.title = None
        info.author = None

    text = parser.from_file(path + filename)  # parse the text of the pdf with Tika package function
    text = identify_tokens(text["content"].lower())  # convert raw text in a list of words
    # Remove stop words (insignificant words)
    stops = set(stopwords.words("english"))
    text = remove_stops(text)  # remove stop words
    # Cut the raw text in abstract - keywords - text (MicroTAS format!)
    text, error = cut_sections(text, format="MicroTAS")
    if error != 0:
        error_text.append(filename)
    # Stemming
    text[0] = stem_list(text[0])  # stemming of the abstract
    text[2] = stem_list(text[2])  # stemming of the text
    # Exception handling
    try:
        title = info.title.lower()
    except:
        error_title.append(filename)
        title = None
    try:
        author = info.author.lower()
    except:
        error_author.append(filename)
        title = None

    new_entry = {"filename": filename, "title": title, "author": author, "date": date,
                 "abstract": text[0], "keywords": text[1], "text": text[2]}
    dataframe = dataframe.append(new_entry, ignore_index=True)
    i += 1
    print(i, ' out of ', nb_files, ' done (', filename, ')')

### Outputs
output_filename = "Datas_MicroTAS" + str(date)
output_log = "Extraction of data from " + path + "\n\n"
output_log = output_log + "Number of incorrect author data : " + str(len(error_author)) + " out of " + str(
    nb_files) + ' files '
output_log = output_log + str(len(error_author) / nb_files * 100) + "%\n"
output_log = output_log + "Number of incorrect title data : " + str(len(error_title)) + " out of " + str(
    nb_files) + ' files '
output_log = output_log + str(len(error_title) / nb_files * 100) + "%\n"
output_log = output_log + "Number of incorrect text data : " + str(len(error_text)) + " out of " + str(
    nb_files) + ' files '
output_log = output_log + str(len(error_text) / nb_files * 100) + "%\n"
print(output_log)
separator = "; "
output_log = output_log + "\n\nIncorrect author files are : \n"
for f in error_author:
    output_log = output_log + f + separator
output_log = output_log + "\nIncorrect title files are : \n"
for f in error_title:
    output_log = output_log + f + separator
output_log = output_log + "\nIncorrect text files are : \n"
for f in error_text:
    output_log = output_log + f + separator
# print(dataframe)
dataframe.to_csv(path + output_filename + ".csv", index=False)
f = open(path + output_filename + ".txt", "w+")
f.write(output_log)
f.close()
