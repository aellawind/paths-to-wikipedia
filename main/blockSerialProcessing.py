#! /usr/bin/env python

import os
import sys
import tarfile
import io
import shutil
import tarfile
import re
import json
import time

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

        # We need to exclude links in parentheses but not links within brackets, like [[ (( )) ]]
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


        links = re.finditer(r'\[\[(.*?)\]\]', line)

        
        if links:
            for link in links:
                if link.group(1).upper().startswith(starts_with):  #Many links have categories at the end, we don't care
                    pass
                else:
                    linksList.append(link.group(1))

    
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
 

        

if __name__=='__main__':
    if len(sys.argv) != 2:
        print "Usage: python", argv[0], "<wikipedia_block>"
        sys.exit(1)
    
    print 'STARTING'
    start = time.time()

    # This takes the tar fileblock and unzips it into a folder with 1000 xml articles

    os.chdir("tmp_output")
    for inp in os.listdir("."):
        if inp == ".DS_Store":
            continue
        tar = tarfile.open(inp, "r:gz")
        tar.extractall()
        wikipedia_dict = dict()
        # This iterates through the folder we just created and goes through all 1000 files
        # This searches for the content we want in each article ie title and links
        block_folder_name = (inp).replace(".tar.gz",'')
        for filename in os.listdir(block_folder_name):
            wiki_filename = os.path.join(os.getcwd(),block_folder_name,filename)
            with open(wiki_filename) as current_wiki_file:
                title, linkslist = find_content(current_wiki_file)
                if title:
                        wikipedia_dict[title] = linkslist
        print 'Processing:', block_folder_name
        # This deletes the folder we extracted the contents into (zipped file remains-saves space)
        shutil.rmtree(block_folder_name)
        
        # This opens an output file and writes the values from our articles into it
        output_file_name = '_'.join(['all','articlesList', '.tsv'])
        with io.open(output_file_name, 'a', encoding='UTF-8') as fd:
            for key, value in wikipedia_dict.items():
                fd.write('%s\t%s\n' % (json.dumps(key, ensure_ascii=False, encoding="UTF-8"), json.dumps(value, ensure_ascii=False, encoding="UTF-8")))
          
    print output_file_name
    end = time.time()
    print "TIME TOTAL IS", end-start
    print "Writing to", output_file_name, "is successful!"
        
  



