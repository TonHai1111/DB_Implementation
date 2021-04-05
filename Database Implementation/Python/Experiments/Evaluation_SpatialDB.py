import sys
sys.path.append("/home/tonhai/Data/Depaul/PLIExtension")

import psycopg2

from IBTree import *
from PLITypes.IBEntry import *
from PLITypes.dataTypes import *
from tools.operations import *
from IBPlusTree import *
from tools.timeTools import  *
import re
from decimal import *
from random import uniform, randint
import os

if __name__ == '__main__':
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    print "Insert Spatio-temporal data....\n"

    print "---------------------------\n"
