#!/usr/bin/env python3
import requests
import json
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords  
import re
import nltk
import time
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", help="Enter port number")
parser.add_argument("-host", "--hostname", help="Enter hostname")
parser.add_argument("-f", "--file", help="Enter file name")

args = parser.parse_args()
if args.port == None:
    print("Please enter a port number.")
    print("Exiting program. Use -h when in doubt")
    quit()
if args.hostname == None:
    print("Please enter a hostname.")
    print("Exiting program. Use -h when in doubt")
    quit()
if args.file == None:
    print("Please Give a file location.")
    print("Exiting program. Use -h when in doubt")
    quit()



print("Checking if nltk is downloaded!")
nltk.download("stopwords")
time.sleep(1)
stop_words = set(stopwords.words('english'))

print("opening txt file")
try:
    with open(args.file,"r+") as f:
            x = f.readlines()
            m = ""
            for line in x:
                m = m+line.strip()
except:
    print("File not found!")
    quit()

print("Cleaning the text before tokenizing")
res = re.findall(r'\w+', m) 
print("filtering all stop words in the txt file")
filtered_sentence = [w for w in res if not w in stop_words]
g = ""
for t in filtered_sentence:
    g = g +" "+ t

data = {"type":"request","sentence":g} 

jsonData = json.dumps(data)
requesting = "http://"+args.hostname+":"+args.port
print("Sending your json request to Go Server running at "+requesting)
try:
    r = requests.post(requesting+"/wordcount",data=jsonData)
except:
    print("Error Sending request. Check your hostname or port number.")
    quit()
resp = r.json()
print("The maximum words in the file:"+args.file)
for i in resp['max']:
    print(i)
print("The minimum words in the file:"+args.file)
for i in resp['min']:
    print(i)
print(resp["debug"])
print("Check the server logs to see more!")
