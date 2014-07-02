#! /usr/bin/env python

import os
import sys
import tarfile
import codecs
import io
import shutil
import Queue
import tarfile
import re
import json
import time
import multiprocessing

def find_content(filename):
    # Variables for title finder
    article_title = None #initializes article title to none, if no title, no output
    # Variables for link finder
    inText = False
    nextLink = ''
    linksList = []
    wordbracket = False
    withinBox = False
    searchCurlyBrackets = 0
    searchCurlyBracketEnd = False
    searchCurlyBracketBegin = False
    searchRef = 0
    searchRefBegin = False
    searchRefEnd = False
    starts_with = ('#','WIKIPEDIA:', 'CATEGORY:', 'HELP:', 'TEMPLATE:', 'FILE:', 'IMAGE:', 'FILE:', 'MODULE:', 'MEDIAWIKI:', 'PORTAL:', ' FILE:', ':FILE:')
      
    for line in filename:
        if len(linksList) > 1:
            break;   

        # The following searches for the title which will be the key of the dictionary
        title = re.search(r'>(.*)</title>', line)
        # If the title begins with one of these things, we do not want to keep the article, otherwise assign the found title to 'article_title'
        if title:
            if title.group(1).upper().startswith(starts_with):
                return None, None
            else:
                article_title = title.group(1) 
         
        # The following makes sure to exclude lines that start with comment
        # If we decide to only include stuff after <text> then we can get rid of this
        if '<comment' in line:
            line = re.sub('<comment(.*?)</comment>', "", line)

        if '(' in line and ')' in line:
            line = re.sub('[[(.*?)\((.*?)\)(.*?)]]', "", line)  

        if '<!--' in line and '-->' in line:
            line = re.sub('<!--(.*?)[[(.*?)]](.*?)-->', "", line)  

        if '[[Image:' in line and ']]' in line:
            line = re.sub('[[Image:(.*?)]]', "", line)    

        # The following makes sure to exclude all links within <ref> </ref> tags
        # Essentially it replaces all instances where it's found with an empty space
        if '<ref>' in line:
            line = re.sub('<ref>(.*?)</ref>', "",line)

        # need to ignore curly braces in one line
        # Essentially replaces the line where it's found with an empty space
        if '{{' in line and '}}' in line:
            line = re.sub('{{(.*?)}}', "", line)

        
        # The following makes sure that we ignore text in curly brackets (in boxes that transcend multiple lines)
        searchCurlyBracketBegin = re.search('{{', line)
        searchCurlyBracketEnd = re.search('}}', line)
        findBeginCurlyBracket = line.find('{{')
        findEndCurlyBracket = line.find('}}')
        if searchCurlyBracketBegin:
            searchCurlyBrackets += 1
        if searchCurlyBracketEnd:
            searchCurlyBrackets -= 1
        # The following evaluate to see WHERE in the line the curly brackets are
        # If the open curly brackets is at the end of the line, we obviously may want the line info before it
        # If the end curly brackets comes midway through the line, we still want the line info after it
        if searchCurlyBrackets > 0:
            if findBeginCurlyBracket > 0:
                line = line[:findBeginCurlyBracket]
            elif findEndCurlyBracket > 0:
                line = line[findEndCurlyBracket:]
            else: #Final case - there are no brackets but we know there was at least one before, so just continue
                continue


        links = re.search(r'\[\[(.*?)\]\]', line)

        
        if links:
            if links.group(1).upper().startswith(starts_with):  #Many links have categories at the end, we don't care
                pass
            else:
                linksList.append(links.group(1))

    
    # If there is a title, return these values
    # This function also checks for links divided with the '|' and parses them out to get the actual title
    if (article_title is not None):
        for i in range(len(linksList)):
            breakIndex = linksList[i].find('|')
            if breakIndex != -1:
                linksList[i] = linksList[i][:breakIndex]
        return article_title, linksList
    # Otherwise the function will return none
    else:
        return None, None
 
def process_block(tar_q):
    block_name, q = tar_q
    tar = tarfile.open(block_name, "r:gz")
    # This iterates through the folder we just created and goes through all 1000 files
    # This searches for the content we want in each article ie title and links
    block_folder_name = (block_name).replace(".tar.gz",'')
    
    for tar_info in tar:
        if tar_info.isfile():
            current_wiki_file = tar.extractfile(tar_info)
            title, linkslist = find_content(current_wiki_file)
            if title:
                q.put((title, linkslist))
            current_wiki_file.close()

    print 'Processing:', block_folder_name
    # This deletes the folder we extracted the contents into (zipped file remains-saves space)
    

        

if __name__=='__main__':
    if len(sys.argv) != 2:
        print "Usage: python", argv[0], "<wikipedia_block>"
        sys.exit(1)
    
    print 'STARTING'
    start = time.time()
    os.chdir("tmp_output")

    p = multiprocessing.Pool(multiprocessing.cpu_count()*2)
    m = multiprocessing.Manager()
    q = m.Queue()

    map_r = p.map_async(process_block, [(block, q) for block in os.listdir('.') if block != ".DS_Store" and not block.endswith('.tsv')])
    final_file = 'final_output.tsv'
    with codecs.open(final_file, 'w', 'utf8') as fd:
        while not map_r.ready() or not q.empty():
            try:
                key,value = q.get(timeout=4)
                fd.write('%s\t%s\n' % (json.dumps(key, ensure_ascii=False, encoding="UTF-8"), json.dumps(value, ensure_ascii=False, encoding="UTF-8")))
            except Queue.Empty, e:
                print 'done'

    print final_file
    end = time.time()
    print "TIME TOTAL IS", end-start
    print "Writing to", final_file, "is successful!"
        
  
