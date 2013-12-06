"""
A simple utility for parsing abstracts out from a given PubMed result XML file.
"""

import sys, os
from xml.etree.ElementTree import fromstring

IPATH_DEF = 'pubmed_result.xml'
OPATH_DEF = 'text'
STOP_CHARS = set(['[',']','+','*','.']) # @TODO - possibly update the set

def text_clean(text,stop=set([])):
  """
  Cleaning up the text from any XML tags.
  """

  cleaned, adding = [], True
  if text == None:
    return ''
  for c in text:
    if c == '<':
      adding = False
    if adding and c not in stop:
      cleaned.append(c)
    if c == '>':
      adding = True
  return ''.join(cleaned)

def get_abs(elem):
  """
  Getting abstract from an XML string. Currently only pubmed XML is supported.
  """

  abstract, title = '', ''
  #root = fromstring(xml_text)
  abs_elem = elem.find('.//Abstract')
  tit_elem = elem.find('.//ArticleTitle')
  if abs_elem != None:
    abstract = text_clean(' '.join([x.text for x in \
      abs_elem.findall('.//AbstractText') if x.text != None]))
  if tit_elem != None:
    title = text_clean(tit_elem.text,stop=STOP_CHARS)
  return title+'\n\n'+abstract

if __name__ == "__main__":
  i_path, o_path = IPATH_DEF, OPATH_DEF
  # getting the input filename
  if len(sys.argv) > 1:
    i_path = os.path.abspath(sys.argv[1])
  # getting the output pathname
  if len(sys.argv) > 2:
    o_path = os.path.abspath(sys.argv[2])
  # parsing the XML file
  print 'Parsing the XML input'
  root = fromstring(open(i_path,'r').read())
  # processing each PubMed ID trying to get an abstract from it
  i = 0
  article_elems = root.findall('.//PubmedArticle')
  for elem in article_elems:
    i += 1
    pmid, pmid_elem = '', elem.findall('.//PMID')
    if len(pmid_elem):
      pmid = pmid_elem[0].text
    print 'Processing PMID:', pmid
    print '...', i, 'out of', len(article_elems)
    abstract = get_abs(elem)
    print '... storing the results to:', o_path
    if len(abstract):
      f = open(os.path.join(o_path,pmid+'.txt'),'w')
      f.write(abstract.encode(encoding='ascii',errors='replace'))
      f.close()
