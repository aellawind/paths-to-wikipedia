#! /usr/bin/env python

#call this as follows ./wikiparser.py latest-pages.xml.bz2 output_dir


from xml.sax.handler import ContentHandler
from xml.sax import make_parser
import time
import bz2
import os
import sys
import tarfile
import io
import shutil

# Subclassing the ContentHandler
class WikiHandler(ContentHandler):

    def __init__(self, output_dir):
        self.buf = []
        self.output_dir = output_dir
        self.files = 0
        self.block = 0
        self.total = 0
        self.block_size = 1000
        self.cur_time = time.time()
        self.current_dir = os.path.join(self.output_dir, 'block-%04d' % self.block)
        if not os.path.isdir(self.current_dir):
            os.makedirs(self.current_dir)

    # Each object element starts at <page>
    def startElement(self, name, attrs):
    
        if name == 'page':
            self.buf = []
        start_tag = ['<', name, ' ']
        for k,v in attrs.items():
            start_tag.append(k + '="' + v + '"') 
        start_tag.append('>')
        
        self.buf.append(''.join(start_tag))
    
    # End the elemebt before the next <page>
    def endElement(self, name):
        self.buf.append('</' + name + '>')
        if name == 'page':
            self.flush_buffer()
        
    def characters(self, content):
        self.buf.append(content)
        
    # writes the content to the opened file    
    def flush_buffer(self):
        with io.open(os.path.join(self.current_dir, 'wiki-%03d.xml' % self.files ), 'w', encoding='UTF-8') as fd:
            for line in self.buf:
                fd.write(unicode(line))
        self.files += 1
        self.total += 1
        if self.files == 1000:
            self.create_block()

        
    def create_block(self):
        tar = tarfile.open(self.current_dir + ".tar.gz", "w:gz")
        tar.add(self.current_dir, arcname='block-%04d' % self.block)
        tar.close()
        shutil.rmtree(self.current_dir)
        print "Done with block %03d in %f seconds" % (self.block, time.time() - self.cur_time )
        self.cur_time = time.time()
        self.block += 1
        self.files = 0
        self.current_dir = os.path.join(self.output_dir, 'block-%04d' % self.block)
        if not os.path.isdir(self.current_dir):
            os.makedirs(self.current_dir)
        
if __name__=='__main__':
    if len(sys.argv) != 3:
        print "Usage: python", argv[0], "<wikipedia_archive> <output_dir>"
        sys.exit(1)
    
    inp = sys.argv[1]
    initial = time.time()
    #make an output directory - mkdir tmp_output or something
    output_dir = sys.argv[2]
    print 'STARTING'
    parser = make_parser()
    parser.setContentHandler(WikiHandler(output_dir))
    parser.parse(bz2.BZ2File(inp, 'r'))
    print "DONE in %f seconds" % time.time() - initial