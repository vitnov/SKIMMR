#!/usr/bin/python
"""
prep_gt.py - preparing the folder structure for using SKIMMR there (in the 
current working directory by default)

Guide to execution:

python prep_gt.py [PATH]

where PATH is an existing alternative path to be used if one wishes to 
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

import sys, os, pkgutil

MAIN_DIRS = {
  'data':'data',
  'text':'text'
}
DATA_DIRS = ['cche','html','stre','usrs']

if __name__ == "__main__":
  # setting the path to the command line argument
  path = os.getcwd()
  if len(sys.argv) > 1:
    path = os.path.abspath(sys.argv[1])
  # creating the main directories in the path
  for main_dir in MAIN_DIRS.values():
    try:
      os.makedirs(os.path.join(path,main_dir))
    except OSError:
      sys.stderr.write('\nW@prep.py: cannot create: %s\n' % main_dir)
  # creating the data sub-directories in the path
  for data_dir in DATA_DIRS:
    try:
      os.makedirs(os.path.join(path,MAIN_DIRS['data'],data_dir))
    except OSError:
      sys.stderr.write('\nW@prep.py: cannot create: %s\n' % data_dir)
  file_list = pkgutil.get_data('skimmr_gt','data/file_list.txt').split('\n')
  for resource in file_list:
    if len(resource.strip()) == 0:
      # don't process empty strings
      continue
    data = None
    try:
      data = pkgutil.get_data('skimmr_gt','data/'+resource)
    except IOError:
      sys.stderr.write('\nW@prep.py: cannot load the resource: %s\n' % resource)
    if data:
      f = open(os.path.join(path,MAIN_DIRS['data'],'html',resource),'w')
      f.write(data)
      f.close()
