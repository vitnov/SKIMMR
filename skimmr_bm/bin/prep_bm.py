#!/usr/bin/python
"""
prep_bm.py - preparing the folder structure for using SKIMMR there (in the 
current working directory by default), downloading the LingPipe-4.0.1 package 
for the extraction (by default).

Guide to exectuion:

python prep_bm.py [minimal] [PATH]

where 'minimal' can be specified if one wishes only to prepare the folder 
structure without downloading the LingPipe software. Optionally, also 
an existing path may be specified (as the PATH argument), if one wishes to 
prepare other directory than the current working one.

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

import sys, os, pkgutil, urllib2, tarfile

MAIN_DIRS = {
  'data':'data',
  'text':'text',
  'lingpipe' : 'lingpipe'
}
DATA_DIRS = ['cche','html','stre','usrs']
LINGPIPE_URL = \
  'https://dl.dropbox.com/u/21379226/software/lingpipe-4.1.0.tar.gz'

if __name__ == "__main__":
  # setting the path and mimimal parameter to the command line arguments
  path, minimal = os.getcwd(), False
  if len(sys.argv) > 1:
    if sys.argv[1] != 'minimal' and \
    os.path.exists(os.path.abspath(sys.argv[1])):
      path = os.path.abspath(sys.argv[1])
    elif sys.argv[1] == 'minimal':
      minimal = True
  if len(sys.argv) > 2:
    if sys.argv[1] == 'minimal':
      minimal = True
    if os.path.exists(os.path.abspath(sys.argv[2])):
      path = os.path.abspath(sys.argv[2])
  # creating the main directories in the path
  print 'Creating the main directories'
  for main_dir in MAIN_DIRS.values():
    try:
      os.makedirs(os.path.join(path,main_dir))
    except OSError:
      sys.stderr.write('\nW@prep.py: cannot create: %s\n' % main_dir)
  # creating the data sub-directories in the path
  print 'Creating the data sub-directories'
  for data_dir in DATA_DIRS:
    try:
      os.makedirs(os.path.join(path,MAIN_DIRS['data'],data_dir))
    except OSError:
      sys.stderr.write('\nW@prep.py: cannot create: %s\n' % data_dir)
  # copying the package resources
  print 'Fetching the package resource files'
  file_list = pkgutil.get_data('skimmr_bm','data/file_list.txt').split('\n')
  for resource in file_list:
    if len(resource.strip()) == 0:
      # don't process empty strings
      continue
    data = None
    try:
      data = pkgutil.get_data('skimmr_bm','data/'+resource)
    except IOError:
      sys.stderr.write('\nW@prep.py: cannot load the resource: %s\n' % resource)
    if data:
      f = open(os.path.join(path,MAIN_DIRS['data'],'html',resource),'w')
      f.write(data)
      f.close()
  # downloading and unpacking the LingPipe software
  if not minimal:
    try:
      print 'Downloading the LingPipe-4.0.1 text mining package'
      wf = urllib2.urlopen(LINGPIPE_URL)
      arc_name = os.path.join(path,MAIN_DIRS['lingpipe'],\
        LINGPIPE_URL.split('/')[-1])
      lf = open(arc_name,'w')
      lf.write(wf.read())
      wf.close()
      lf.close()
      print 'Unpacking the LingPipe-4.0.1 text mining package'
      f = tarfile.open(arc_name,'r:gz')
      f.extractall(path=os.path.join(path,MAIN_DIRS['lingpipe']))
      f.close()
      print 'Removing the package archive file'
      os.remove(arc_name)
    except:
      exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
      msg = 'could not fetch LingPipe-4.0.1?\nException details:\n'+\
        '  exception type : '+`exceptionType`+'\n'+\
        '  exception value: '+`exceptionValue`+'\n'+\
        '  attempted URL: '+LINGPIPE_URL+'\n'
      sys.stderr.write('\nW@prep.py: ' + msg)
  print 'Finished preparing the folder:', path
