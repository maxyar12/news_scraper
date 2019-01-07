#!/usr/bin/python

import HTMLescapetool
import urllib2
import smtplib
from email.mime.text import MIMEText
import re
import os
import os.path
import sys                  
import csv
import time
import httplib
import datetime
import nltk  
import pymssql as mdb
import pprint
import guess_language  
from BeautifulSoup import BeautifulSoup
#import MySQLdb as mdb
from urllib2 import Request, urlopen, URLError, HTTPError
import _mssql
#import _mysql

 
'''Input data for this python module should be separated into files csv files.
 The csv file contains the main URL which  has links to text articles to be scraped 
and other source specific data. The input files must be formatted in a certain way, see the comments below'''

           # GLOBAL VARIABLES (DATABASE CONNECTIONS/SYSTEM PATHS)

host =      ' '
user =     ' '
password = ' '
database =    ' '
port = '3306'
tagspath = '/home/max/Desktop/workspace/tagwords.txt'
input_data_path = '/home/max/Desktop/workspace/scraper_input/'


              

try:
    del start_index
    del end_index                         # not sure if this is necessary at all
    del href_check
    del link_tags
    del text_tags
    del base_page
    del base_page_prefix
    del checks
except NameError: pass

for root, dirs, files in os.walk(input_data_path):
    
    for name in files:
        
        file_path = os.path.join(root, name)                                         #  LOOP THROUGH ALL INPUT PAGE FILES           
        myfile = open(file_path,'r')                                                                               
        reader = csv.reader(myfile)        
        input_datas = list(reader)
        try:      
            source = filter(None,input_datas[0])[0]                       # news source
            base_page = filter(None,input_datas[1])[0]                    # page with links to text articles 
            base_page_prefix = input_datas[1][1]                          # link base: e.g.: http:www.nytimes.com  
            title_tag = filter(None,input_datas[2])[0]                    # where the title is, usually just <title> tag
            link_tags =  input_datas[3]           # where to find links:  array structure:  [ ELEMENT, attribute (class or id), value ]       
            text_tags =   input_datas[4]               # where to find tags: array structure:  [ ELEMENT ,  attribute (class or id), value ]
            checks =     filter(None,input_datas[5])                      # string array.  split text around these strings.
            if input_datas[6][0] != '':
                title_type = int(input_datas[6][1])                   # split the title before or after the title splitting string
                titlesplitter = input_datas[6][0]                     # the string to split the title about (if necessary)
            tags_irrelevant = input_datas[7][0]    # if this is set to 'yes', then all articles will be loaded whether or not they contain 
            href_check = input_datas[8][0]         # this is used to check the links and to throw out links we don't want, e.g. video links

            if input_datas[9][0] != '':
                start_index = int(input_datas[9][0])       # specifies the start index of the link string that must match
                end_index =  int(input_datas[9][1])        # specifies the end index of the link string that must match
            article_type = input_datas[10][0]          # specifies if we want the whole text ('long') or just the first bit of text ('short')
            end_splitters = filter(None,input_datas[11])             # places to look for the end of the article text
        except IndexError: pass
        myfile.flush()
        myfile.close()
        try:

            soup = BeautifulSoup(urllib2.urlopen(base_page).read())   
        
        except HTTPError:
                print 'http error occurred, snippet may have failed to post'
        

        if link_tags[1] != '':
            critical_elements = soup.findAll(link_tags[0],{link_tags[1]:link_tags[2]})

        elif link_tags[1] == '':
            critical_elements = soup.findAll(link_tags[0])
        
        if len(critical_elements) == 1:                                             # note: this is a weak condition
            critical_elements = critical_elements[0].findAll('a')
        
        count = 1            

        for item in critical_elements :               # loop through all links to articles           
            
            try: 
                del snippet
                del strings
            except NameError: pass
            if count > 15:
                break
            try:   
                if link_tags[0] == 'a':
                    x = item
                elif item.name == 'a':
                    x = item
                else:                       
                    x = item.find("a")                                      
                if x is None:
                    continue
                if href_check != '':
                    if x['href'][start_index : end_index] != href_check:
                        continue
                else: 
                    pass
                if x['href'][:8] == '../../..':       # this is here because http://cancer.northwestern.edu used this link structure
                    x['href'] = x['href'][8:]
                if x['href'][19:24] == 'video':
                    continue
                url1 = base_page_prefix + x['href'] 

                print url1 # for debug                
                soup = BeautifulSoup(urllib2.urlopen(url1).read(),convertEntities=BeautifulSoup.HTML_ENTITIES)
                if soup.find('title') is not None:
                    title = ''.join(soup.find('title').findAll(text=True)) 
                if input_datas[6][0] != '':                                     
                    title = title.split(titlesplitter)[title_type]
                title = title.encode("ascii","ignore")
                print title
                if text_tags[0] != '': 
                    xx = soup.find(text_tags[0],{text_tags[1]:text_tags[2]})
                else:
                    xx = soup                      
                strings = []
                if xx is not None:
                    xx = xx.findAll('p')
                    for item in xx:
                        strings.append(''.join(item.findAll(text=True))+' ')                       
                    snippet = ''.join(strings).encode("ascii","ignore")    
                    snippet = ' '.join(snippet.split())
                    strings = snippet
                if not strings: 
                    if soup.find(text_tags[0],{text_tags[1]:text_tags[2]}) is not None:                                                     
                        xx = soup.find(text_tags[0],{text_tags[1]:text_tags[2]}).findAll(text = True)
                        xx = ''.join(xx).encode("ascii","ignore")
                        xx = ' '.join(xx.split())
                        strings = xx 
                        snippet = strings                  
                                                         
                 #the checks are source specific data located in the scraper_input_data folder           
                                              
                for item in checks:
                    if item in strings:
                        snippet = strings.split(item)                
                        snippet = snippet[1]         
                try:
                   snippet = snippet.lstrip()
                except NameError:
                    continue
                
                
                #snippet = snippet[0:900]         # !! change the number of characters to a variable read in from the source paramater file
                
                strings = snippet
                #now we will strip the parenthenthetical parts from the snippet
                            
                regEx = re.compile(r'([^\(]*)\([^\)]*\) *(.*)') 
                m = regEx.match(snippet) 
                while m: 
                    snippet = m.group(1) + m.group(2) 
                    m = regEx.match(snippet) 
                testlanguage = snippet
                snippet = HTMLescapetool.unescape(snippet).encode('ascii','ignore')          
                tokenizer = nltk.data.load('tokenizers/punkt/english.pickle') 
                sentences = tokenizer.tokenize(snippet)            
                list2 = snippet.split(" ") 
                snippet = snippet.lstrip()
                content_tags = [] 
              
                # check that items are not way too long
                lengthproblem = 0
                list3 = snippet.split(" ")
                for item in list3:
                    if len(item) >=30:
                        lengthproblem = 1
                       
                reader2 = csv.reader(open(tagspath,'r'))  

                 #debug    NOTE THAT SNIPPET WAS lstrip()  applied but NOT strings, so console will display differently from database
                

                if article_type == 'long':
                    for item in end_splitters:
                        if item in snippet:
                            snippet = strings.split(item)                
                            snippet = snippet[0]         
                    fulltext = snippet
                else:
                    fulltext = 'Full text is not provided for this article. Sorry! '                   
                
                if len(fulltext) > 7999:
                    fulltext = fulltext[0:7998]

                for line in reader2: 
                    if line[0] in strings:        
                
                        # while len(tags) <=1:             # if only one tag is desired

                        content_tags.append(line[0]) # may need to change indentation based on number of tags criteria                                
 
                content_tags = list(set(content_tags))   
                
                content_tags = [x.upper() for x in content_tags]      # should tags be uppercase?

                a_date = datetime.date.today()                  
                a_text = sentences[0] +' '+ sentences[1] +' '+ sentences[2]                  
                a_fulltext = fulltext
                a_link = url1
                a_title = title.strip()
                a_source = source
                print a_text

                if (len(content_tags) >= 1 or tags_irrelevant == 'yes') and lengthproblem ==0 and guess_language.guessLanguage(testlanguage) =='en' and len(sentences) >= 3:
                    if not content_tags:                       
                        content_tags.append(' ')
                        a_tag = content_tags[0]
                    else:
                        a_tag = content_tags[0]
                    print content_tags # debug             
                    con = None
                    try:     
                        con = mdb.connect(host='161.58.134.149', user='canc73',password='njsh+erg', database='canc73');                          
                        cur = con.cursor()
                        cur.execute("USE canc73")
                        cur.execute("if not exists (select * from sysobjects where name='Scrapercontent' and xtype='U') create table Scrapercontent ( A_id int PRIMARY KEY IDENTITY, a_text varchar(3000),a_fulltext varchar(8000), a_link varchar(1000), a_title varchar(1000), a_source varchar(300), a_date DATETIME, a_tag varchar(300))")
                        cur.execute('''INSERT INTO Scrapercontent (a_text,a_fulltext,a_link,a_title,a_source,a_date,a_tag) VALUES (%s,%s,%s,%s,%s,%s,%s)''',(a_text,a_fulltext,a_link,a_title,a_source,a_date,a_tag)) 
                        cur.close ()
                        con.commit ()

                    finally:
                        if con:
                            con.close()                 
      
            except HTTPError:
                print 'http error occurred, snippet may have failed to post'
            except httplib.InvalidURL:pass          
            except URLError:
                print ' \n a URL error occurred \n'
            except urllib2.URLError:
                print ' \nurl error occurred \n'
            #except TypeError:
            #    print ' \n an error occurred \n'
            except IndexError:
                print ' \n an Index exception error occurred \n'
            except KeyError: pass
            #except Exception, e: print e
            count += 1
            print count

# this is the indentation level to execute the remove duplicates sql query using cur.execute
try:     
    con = mdb.connect(host=' ', user=' ',password=' ', database=' ');                          
    cur = con.cursor()
    cur.execute("USE dbname")
    cur.execute("DELETE FROM Scrapercontent WHERE A_id NOT IN (SELECT MIN(A_id) FROM Scrapercontent GROUP BY a_text);")
    cur.close ()
    con.commit ()

finally:
    if con:
        con.close()  








