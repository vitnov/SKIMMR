#!/usr/bin/python
"""
dnwl_bm.py - downloading plain text versions of PubMed abstracts according to 
the PubMed IDs supplied in the SKIMMR text directory (or elsewhere if 
specified)

Guide to execution:

python dwnl_bm.py E-MAIL [PATH]

where E-MAIL is your e-mail (required for downloading stuff from PubMed using 
the Entrez API). The PubMed IDs are expected to be stored in *.pmid file(s) in
the SKIMMR 'text' directory, in a simple format with the IDs separated by
new lines, empty spaces or commas. Optionally, you can also specify an 
alternative path (via the PATH argument). Note that the number of abstracts 
successfully downloaded is limited to 2000 by default.

Copyright (C) 2012 Vit Novacek (vit.novacek@deri.org), Digital Enterprise
Research Institute (DERI), National University of Ireland Galway (NUIG)
All rights reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import sys, os
from Bio import Entrez
from xml.etree.ElementTree import fromstring

# limit on the maximum number of abstracts successfully fetched
LIMIT = 2000

def text_clean(text):
  # cleaning up the text from any XML tags.
  cleaned, adding = [], True
  if text == None:
    return ''
  for c in text:
    if c == '<':
      adding = False
    if adding:
      cleaned.append(c)
    if c == '>':
      adding = True
  return ''.join(cleaned)

def get_abs(pmid):
  # getting abstract from an XML string fetched from PubMed according to the 
  # PubMed ID given
  handle_text = \
    Entrez.efetch(db="pubmed",id=pmid,retmode="xml",rettype="medline")
  xml_text = handle_text.read()
  abstract = ''
  root = fromstring(xml_text)
  abs_elem = root.find('.//Abstract')
  if abs_elem != None:
    abstract = text_clean(' '.join([x.text for x in \
      abs_elem.findall('.//AbstractText') if x.text != None]))
  return abstract.encode('utf-8')

if __name__ == "__main__":
  # setting the e-mail and path to the command line argument(s)
  e_mail, path = '', os.path.join(os.getcwd(),'text')
  if len(sys.argv) > 1:
    e_mail = sys.argv[1]
  if len(sys.argv) > 2:
    path = os.path.abspath(sys.argv[2])
  if e_mail:
    Entrez.email = e_mail
  # reading the PubMed IDs
  print 'Reading in the PubMed IDs from:', path
  pmids = set()
  for fname in os.listdir(path):
    if os.path.splitext(fname)[-1].lower() != '.pmid':
      continue
    text = open(os.path.join(path,fname),'r').read()
    for chunk in text.split():
      for pmid in chunk.split(','):
        if pmid.isdigit():
          pmids.add(pmid.strip())
  print 'Finished reading in - number of unique PubMed IDs:', len(pmids)
  # fetching the abstracts
  print 'Fetching the abstracts'
  i = 0
  for pmid in pmids:
    # fetching the PubMed XML and parsing out the abstract
    abstract = get_abs(pmid)
    if len(abstract):
      # processing non-empty abstracts only
      f = open(os.path.join(path,pmid+'.txt'),'w')
      f.write(abstract)
      f.close()
      i += 1
      if i >= LIMIT:
        print '  ... the limit of maximum number of abstracts reached ...'
        break
      else:
        print '  ... fetched', i, 'out of', min(len(pmids),LIMIT)
    else:
      print '  ... abstract empty for PMID:', pmid
      i += 1
  print 'Finished fetching abstracts to:', path
