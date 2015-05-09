#!/usr/bin/env python


'''
-------------------------------------------------------------------------------
ExCrawler (Example Crawler)

By David J. Lee

exCrawler is an example script of building a database from xml files.  This is
actually a small, vanilliarized part of a tool I wrote at Imageworks that pulls
content from a variety of sources in order to build a database of useful
object-related information.

The result is 3-pronged:
    - and easy readout of the most relevant information of a object.
    - the crawl can be multithreaded, which means it can crawl over several,
        possibly hundreds of shots and many objects
    - the ability to write out to a database for further analysis.  Some
        examples might include:
            - number of objects used in a scene
            - how many scenes have out of date objects
-------------------------------------------------------------------------------
'''



import os
import sys
import re
import time
import logging
import argparse
from multiprocessing.pool import ThreadPool
from xml.dom import minidom

import sqlite3
SHOTLOCATION = './'
DBFILE = '/tmp/excrawler.db'


class ExCrawler(object):
    def __init__(self, args):
        self.logger = logging.getLogger('exCrawler')
        if not args.show:
            logging.error('Please setshot first')
            sys.exit()
        self.show = args.show
        self.shot = args.shot
        self.includeShots = args.include
        self.excludeShots = args.exclude
        self.shotList = self.getShotList()
        self.objects = args.objects
        self.verbose = False
        self.useDB = args.db
        if args.verbose:
            self.logger.setLevel(logging.INFO)
            self.verbose = True

    # Obtain shot list
    def getShotList(self):
        try:
            shotList = os.listdir('%s/%s' % (SHOTLOCATION, self.show))
        except:
            self.logger.error('Shot issues.')
            shotList = []
        if self.includeShots:
            if 'all' not in self.includeShots.lower():
                self.includeShots = self.includeShots.split()
                shotList = [s for s in shotList if
                            any(i in s for i in self.includeShots)]
        else:
            shotList = [self.shot]

        if self.excludeShots and 'none' != self.excludeShots.lower():
            excludes = self.excludeShots.split()
            shotList = [s for s in shotList if not
                        any(x in s for x in excludes)]
        return shotList

    # For each shot in the shotList, parse the shotXML and grab the objects in it
    def findShotsWithObjects(self):
        print "Planning to check %s shots..." % len(self.shotList)
        if self.useDB:
            self.logger.debug('Using database')
            self.db = sqliteDB()
        for shot in sorted(self.shotList):
            print ('<shotname>\t    <object>\t\t  <source>'
                   '\t\t\t<version/highest>\t<mayaFile>')
            if not self.verbose:
                print "%s" % shot
            # Get objects
            objects = self.shotObjects(shot)

            # if a regex is supplied, it will cull the list
            if self.objects:
                objects = [ob for ob in objects if
                            any(obj in ob.get('objName') for obj in self.objects) or
                            any(obj in ob.get('name') for obj in self.objects)]
            if not objects:
                continue
            for item in sorted(objects, key=lambda k: k.get('name')):
                self.printItem(item)
        time.sleep(0.1)
        return True

    def shotObjects(self, shot):
        logging.debug("\nChecking: %s" % shot)
        shotData = self.shotXML_objectsGet(shot)
        if not shotData:
            return {}
        return shotData

    def shotXML_objectsGet(self, shot):

        try:
            xmlFile = '%s/%s/%s/%s.xml' % (SHOTLOCATION,
                                           self.show,
                                           shot, shot)
            shotXML = minidom.parse(xmlFile)
            self.logger.info('Checking xml: %s'% xmlFile)
            # Have a FAKE XML
        except Exception, e:
            self.logger.error('ShotXML %s parsing error: %s'
                              % (shot, e))
            return

        childList = shotXML.getElementsByTagName('Object')

        # opportunity to map/multithread
        pool = ThreadPool(processes=2)
        objData= pool.map(self.getObjInfo, childList)

        # Add shot info to each object
        [obj.update({'shot':shot}) for obj in objData]

        # Add entry into database
        if self.useDB:
            for char in objData:
                self.db.insert(char)
            self.db.commit()
        return objData

    def getObjInfo(self, obj):
        # Build dictionary for each object received
        try:
            name = obj.getAttribute('name')
            attrs = obj.childNodes[0]
            shotVer = attrs.getAttribute('shotVer')
            objProd = attrs.getAttribute('objName')
            objHighest = attrs.getAttribute('objHighest')
            objVer = attrs.getAttribute('objVer')
            maya_file = attrs.getAttribute('mb')
            ood = True if int(objHighest) > int(objVer) else False

        except:
            self.logger.info("%s not valid." % obj)
            return {}

        return {'name': name,
                'shotVer': shotVer,
                'mb': maya_file,
                'objName': objProd,
                'objVer': objVer,
                'objHighest': objHighest,
                'ood': ood
                }

    def printItem(self, item):
        # Output to terminal.  Otherwise it can be useful as an API for
        # ingesting or analysis in other tools
        printColor = printFont = ''
        shotReveal = ''

        lineText = '   %s    %s\tshot_v%s %s\t' %('{0: <8}'.format(shotReveal),
                                              '{0: <25}'.format(item.get('name')),
                                              '{0: <4}'.format(item.get('shotVer')),
                                              '{0: <15}'.format(item.get('objName')))

        if item.get('ood'):
            lineText += 'OOD: v%s/v%s\t' % (item.get('objVer'),
                                            '{0: <3}'.format(item.get('objHighest')))
        else:
            lineText += 'current: v%s\t' % ('{0: <3}'.format(item.get('objVer')))
        lineText += '{0: <15}'.format(item.get('mb'))

        print printColor, printFont, lineText

    def run(self):
        self.findShotsWithObjects()


class sqliteDB():
    # Class for database operations

    def __init__(self):
        self.conn = self.dbConnect(DBFILE)
        self.cur = self.conn.cursor()

    def dbConnect(self, dblite):
        newdb = False
        if not os.path.exists(dblite):
            newdb = True
        conn = sqlite3.connect(dblite)
        if not newdb:
            return conn

        with conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE entities("
                        "shot text, name TEXT, objProd TEXT, "
                        "shotVer INT, objVer INT, objHighest INT, ood text, "
                        "mayafile text)")
        return conn

    def insert(self, char):
        sql = ('INSERT INTO entities VALUES '
               '("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")')

        sql = sql % (char.get('shot'),
                     char.get('name'),
                     char.get('objProd'),
                     char.get('shotVer'),
                     char.get('objVer'),
                     char.get('objHighest'),
                     char.get('ood'),
                     char.get('mb'))
        print sql
        self.cur.execute(sql)

    def commit(self):
        self.conn.commit()

    def query(self, db):
        pass


def parse_command_line():
    usage = """--
    %prog <command> [options] <object1> <object2>... <objectn>\n
    Crawls the shots of a show for objects
    """
    parser = argparse.ArgumentParser(usage)
    parser.add_argument("-s", "--show", action="store", default='show',
                        help="Show to perform search on.  By default setshot")
    parser.add_argument("--shot", "--st", action="store", default='shot001',
                        help="Shot to perform search on.  By default setshot")
    parser.add_argument("--include", action="store", default=[],
                        help="only include listed shots (space delimited)."
                             " if 'ALL' is entered, will include all shots"
                             " (excluding excluded) ")
    parser.add_argument("--exclude",
                        action="store",
                        default=('system'),
                        help="exclude certain shots (space delimited) from "
                             "the search, includes ALL others.")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="includes more information")
    parser.add_argument("--db", action="store_true", default=False,
                        help="Use sqlite3 file database system if available.")

    parser.add_argument('objects', metavar='N', type=str, nargs='*',
                        help='list frames')
    # build in rebuilding database
    # I would like to expand this to use database data instead.  It will allow
    # For database development to perform studies like #of out of date shots,
    # total out of date chars per char, and other such statistics
    args = parser.parse_args()

    return args


def main():
    args = parse_command_line()
    searcher = ExCrawler(args)
    searcher.run()

if __name__ == "__main__":
    main()
