#!/usr/bin/env python3
# -*- coding:utf-8
import requests, json, sys, os, re, datetime, logging  # , traceback

proxies = {
  'http': 'http://1.2.3.4:48888',
  'https': 'http://1.2.3.4:48888',
}

# uncomment to deactivate proxy

downloaded_zones = 0
# retries = 0


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
        config_file = os.path.dirname(os.path.realpath(__file__)) + '/config.json'
        self.readConfig(config_file)

    def readConfig(self, configFilename='config.json'):
        try:
            self.conf = json.load(open(configFilename))
        except:
            raise czdsException("Error loading '" + configFilename + "' file.")

    def prepareDownloadFolder(self):
        if 'download_directory' in self.conf:
            directory = self.conf['download_directory'] + '/'
        else:
            directory = './zonefiles/'
        directory = directory + self.td.strftime('%Y-%m-%d')
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def getZonefilesList(self):
        """ Get all the files that need to be downloaded using CZDS API.
        """
        retries = 0
        while retries < 11:
            try:
                r = self.s.get(self.conf['base_url'] + '/user-zone-data-urls.json?token=' + self.conf['token'])
            except Exception as e:
                # global retries
                logging.error("Caught exception in getZonefilesList, retry #{}. Error: {}".format(retries, e))
                sys.stderr.write("Caught exception in getZonefilesList, retry #{}. Error: {}".format(retries, e))
                # if retries < 10:
                #     retries += 1
                #     time.sleep(10 * retries)
                #     self.getZonefilesList()
                # else:
                #     logging.error("Giving up, too many retries in getZonefilesList ({})".format(retries))
                #     sys.exit(1)
                retries += 1
            else:
                retries = 100
                if r.status_code != 200:
                    if r.status_code == 403:
                        print("403 error in getZonefilesList for domain {}".format(self.conf['base_url']))
                    raise czdsException("Unexpected response from CZDS while getZonefilesList '" +
                                        self.conf['base_url'] + "...'., code:", r.status_code)
                    break
                try:
                    # remove duplicate zone files
                    files = list(set(json.loads(r.text)))
                except Exception as e:
                    raise czdsException("Unable to parse JSON returned from CZDS: " + str(e))
                    break

                logging.info("getZonefilesList returns {} zones".format(len(files)))
                logging.debug("getZonefilesList returning zones are: {}".format(files))
                return files

    def parseHeaders(self, headers):
        if 'content-disposition' not in headers:
            raise czdsException("Missing required 'content-disposition' header in HTTP call response.")
        elif 'content-length' not in headers:
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
        retries = 0
        while retries < 11:
            try:
                r = self.s.head(self.conf['base_url'] + path)
            except Exception as e:
                logging.error("Caught ulrllib2.HTTPError in head, retrying #{}. Error: {}, path {}".format(retries, e, self.conf['base_url'] + path))
                sys.stderr.write("Caught ulrllib2.HTTPError in head, retrying #{}. Error: {}, path {}\n".format(retries, e, self.conf['base_url'] + path))
                # global retries
                # if retries < 10:
                #    retries += 1
                #    time.sleep(10 * retries)
                #    self.prefetchZone(path)
                # else:
                #    logging.error("Giving up, too many retries ({})".format(retries))
                #    sys.exit(1)
                retries += 1
            else:
                if r.status_code != 200:
                    if r.status_code == 403:
                        print("403 error in getZonefilesList for domain {}".format(self.conf['base_url']))
                    # raise czdsException("Unexpected response from CZDS while fetching '" + path + "'.")
                    logging.error("Unexpected response from CZDS while getZonefilesList '{}{}''.,"
                                  " code: {}".format(self.conf['base_url'], path, r.status_code))
                    raise czdsException("Unexpected response from CZDS while getZonefilesList '" +
                                        self.conf['base_url'] + path + "'., code:", r.status_code)
                    break
                else:
                    return self.parseHeaders(r.headers)

    def isNewZone(self, directory, hData):
        """ Check if local zonefile exists and has identical filesize
        """
        for filename in os.listdir(directory):
            if hData['date'] + '-' + hData['zone'] + '-' in filename \
               and hData['filesize'] == os.path.getsize(directory + '/' + filename):
                return False
        return True

    def fetchZone(self, directory, path, chunksize=1024):
        """ Do a regular GET call to fetch zonefile
        """
        logging.debug("fetching zone {}".format(self.conf['base_url'] + path))
        retries = 0
        while retries < 11:
            try:
                r = self.s.get(self.conf['base_url'] + path, stream=True)
            except Exception as e:
                logging.error("Caught ulrllib2.HTTPError, retrying. Error: {}".format(e))
                sys.stderr.write("Caught ulrllib2.HTTPError, retrying. Error: {}".format(e))
                # global retries
                # if retries < 10:
                #    retries += 1
                #    time.sleep(10 * retries)
                #    self.fetchZone(directory, path, chunksize)
                # else:
                #    logging.error("Giving up, too many retries ({})".format(retries))
                #    sys.exit(1)
                retries += 1
            else:
                retries = 100
                if r.status_code != 200:
                    if r.status_code == 403:
                        print("403 error in getZonefilesList for domain {}".format(self.conf['base_url']))

                    # raise czdsException("Unexpected response from CZDS while fetching '" + path + "'.")
                    logging.warning("Unexpected response from CZDS while getZonefilesList '" +
                                    self.conf['base_url'] + path + "'., code:", r.status_code)
                    raise czdsException("Unexpected response from CZDS while getZonefilesList '" +
                                        self.conf['base_url'] + path + "'., code:", r.status_code)
                    break
                hData = self.parseHeaders(r.headers)
                finalOutputFile = directory + '/' + hData['zone'] + '.zone.gz'
                outputFile = finalOutputFile + '.tmp'

                if os.path.isfile(finalOutputFile):
                    logging.warning("file for zone '{}' already exists!".format(hData['zone']))
                    return

                with open(outputFile, 'wb') as f:
                    for chunk in r.iter_content(chunksize):
                        f.write(chunk)

                os.rename(outputFile, finalOutputFile)
                logging.debug("Downloaded \"{}\" zone".format(hData['zone']))
                global downloaded_zones
                downloaded_zones = downloaded_zones + 1

    def fetch(self):
        directory = self.prepareDownloadFolder()
        logging.basicConfig(filename=directory + "/downloader.log", level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s:%(name)s:%(module)s:%(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        logging.warning("testlog")
        paths = self.getZonefilesList()
        # Grab each file.
        for path in paths:
            try:
                if 'prefetch' in self.conf and self.conf['prefetch']:
                    hData = self.prefetchZone(path)
                    if not self.isNewZone(directory, hData):
                        continue
                self.fetchZone(directory, path)
            except czdsException as e:
                logging.info("Exception at fetch: {}".format(e))


# TODO: make this a proper main function
try:
    downloader = czdsDownloader()
    downloader.fetch()
except Exception as e:
    sys.stderr.write("CZDS: After downloading {} domains, fatal error occoured: {}.\n".format(downloaded_zones, e))
    logging.error("CZDS: After downloading {} domains, fatal error occoured: {}.".format(downloaded_zones, e))
    # sys.stderr.write(traceback.print_exception())
    # sys.stderr.write(traceback.print_exc())
    exit(1)
else:
    logging.info("Complete, downloaded {} zone files.".format(downloaded_zones))
    sys.stderr.write("CZDownloads: Complete, downloaded {} zone files.\n".format(downloaded_zones))
