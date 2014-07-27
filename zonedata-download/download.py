#!/usr/bin/env python
# -*- coding:utf-8
import requests, json, sys, os, re, datetime

class czdsException(Exception):
    pass

class czdsDownloader(object):
    file_syntax_re = re.compile("""^(\d{8})\-([a-z\-0-9]+)\-zone\-data\.txt\.gz""", re.IGNORECASE)
    content_disposition_header_re = re.compile('^attachment; filename="([^"]+)"', re.IGNORECASE)

    def __init__(self):
        """ Create a session
        """
        self.s = requests.Session()
        self.td = datetime.datetime.today()

    def readConfig(self, configFilename = 'config.json'):
        try:
            self.conf = json.load(open(configFilename))
        except:
            raise czdsException("Error loading '" + configFilename + "' file.")

    def prepareDownloadFolder(self):
        directory = './zonedata-download/zonefiles.' + self.td.strftime('%Y%m%d')
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def getZonefilesList(self):
        """ Get all the files that need to be downloaded using CZDS API.
        """
        r = self.s.get(self.conf['base_url'] + '/user-zone-data-urls.json?token=' + self.conf['token'])
        if r.status_code != 200:
            raise czdsException("Unexpected response from CZDS while fetching urls list.")

        try:
            files = json.loads(r.text)
        except Exception, e:
            raise czdsException("Unable to parse JSON returned from CZDS: " + str(e))

        return files

    def parseHeaders(self, headers):
        if not 'content-disposition' in headers:
            raise czdsException("Missing required 'content-disposition' header in HTTP call response.")
        elif not 'content-length' in headers:
            raise czdsException("Missing required 'content-length' header in HTTP call response.")

        f = self.content_disposition_header_re.search(headers['content-disposition'])
        if not f:
            raise czdsException("'content-disposition' header does not match.")

        filename = f.group(1)

        f = self.file_syntax_re.search(filename)
        if not f:
            raise czdsException("filename does not match.")

        return {
            'date': f.group(1),
            'zone': f.group(2),
            'filename': filename,
            'filesize': int(headers['content-length'])
        }

    def prefetchZone(self, path):
        """ Do a HTTP HEAD call to check if filesize changed
        """
        r = self.s.head(self.conf['base_url'] + path)
        if r.status_code != 200:
            raise czdsException("Unexpected response from CZDS while fetching '" + path + "'.")
        return self.parseHeaders(r.headers)

    def isNewZone(self, directory, hData):
        """ Check if local zonefile exists and has identical filesize
        """
        for filename in os.listdir(directory):
            if hData['date'] + '-' + hData['zone'] + '-' in filename \
               and hData['filesize'] == os.path.getsize(directory + '/' + filename):
               return False
        return True

    def fetchZone(self, directory, path, chunksize = 1024):
        """ Do a regular GET call to fetch zonefile
        """
        r = self.s.get(self.conf['base_url'] + path, stream = True)
        if r.status_code != 200:
            raise czdsException("Unexpected response from CZDS while fetching '" + path + "'.")
        hData = self.parseHeaders(r.headers)
        outputFile = directory + '/' + hData['date'] + '-' + hData['zone'] + '-' + self.td.strftime('%H%M') + '.zone.gz'

        with open(outputFile, 'wb') as f:
            for chunk in r.iter_content(chunksize):
                f.write(chunk)

    def fetch(self):
        directory = self.prepareDownloadFolder()
        paths = self.getZonefilesList()
        """ Grab each file.
        """
        for path in paths:
            if 'prefetch' in self.conf and self.conf['prefetch']:
                hData = self.prefetchZone(path)
                if not self.isNewZone(directory, hData):
                    continue
            self.fetchZone(directory, path)

try:
    downloader = czdsDownloader()
    downloader.readConfig()
    downloader.fetch()
except Exception, e:
    sys.stderr.write("Error occoured: " + str(e) + "\n")
    exit(1)
    
