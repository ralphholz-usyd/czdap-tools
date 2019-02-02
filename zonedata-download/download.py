#!/usr/bin/env python3
# -*- coding:utf-8
import argparse
import datetime
import json
import logging
import os
import requests
import smtplib
import sys


class GetError(Exception):
    def __init__(self, message):
        super().__init__(message)


class CZDSError(Exception):
    pass


class CZDSDownloader(object):
    def __init__(self, config_file):
        """ Create a session
        """
        self.s = requests.Session()
        self.td = datetime.datetime.today()
        self.config = None
        self.load_config(config_file)
        self.retries = 0
        self.downloaded_zones = 0
        # these will be set up as we go
        self.config_fd = None
        self.directory = None
        self.access_token = None
        self.downloadable_zones = 0
        # set up everything, including logging
        self.prepare_download_folder()
        # proxies
        if self.get_config_item('proxy.http') != '' or self.get_config_item('proxy.https') != '':
            self.proxies = {'http': self.get_config_item('proxy.http'),
                            'https': self.get_config_item('proxy.https')}
        else:
            self.proxies = None

    def load_config(self, cfg_file):
        try:
            self.config_fd = open(cfg_file, 'r')
            self.config = json.load(self.config_fd)
            self.config_fd.close()
        except Exception as e:
            self.send_msg('Failed to load configuration from {} ({})'.format(cfg_file, e))
            sys.exit(1)

    def get_config_item(self, item, default_value=None):
        if item not in self.config:
            if default_value is None:
                self.send_msg("Could not find mandatory item '{}' in the configuration".format(item))
                sys.exit(1)

            return default_value

        return self.config[item]

    def prepare_download_folder(self):
        self.directory = self.get_config_item('output_directory') + '/' + self.td.strftime('%Y-%m-%d')
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        logging.basicConfig(filename=self.directory + "/download.log", level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s:%(name)s:%(module)s:%(funcName)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def czds_authenticate(self):
        auth_headers = {'Content-Type': 'application/json',
                        'Accept': 'application/json'}

        credentials = {'username': self.get_config_item('czds.user'),
                       'password': self.get_config_item('czds.password')}

        auth_url = self.get_config_item('czds.auth_url') + '/api/authenticate'

        try:
            response = requests.post(auth_url, data=json.dumps(credentials),
                                     headers=auth_headers, proxies=self.proxies)

            if response.status_code == 200:
                self.access_token = response.json()['accessToken']
                logging.info('Authenticated to CZDS as {}'.format(self.get_config_item('czds.user')))
            elif response.status_code == 404:
                self.send_msg("Invalid URL '{}' (returns a 404)".format(auth_url))
                sys.exit(1)
            elif response.status_code == 401:
                self.send_msg("Authentication to CZDS for {} failed with 401, not authorized"
                              .format(self.get_config_item('czds.user')))
                sys.exit(1)
            elif response.status_code == 500:
                self.send_msg("CZDS server returned a 500 Internal Server Error for POST to '{}'".format(auth_url))
                sys.exit(1)
            else:
                self.send_msg("CZDS returned {} for POST to '{}'".format(response.status_code, auth_url))
                sys.exit(1)
        except Exception as e:
            self.send_msg("Failed to POST to '{}' ({})".format(auth_url, e))
            sys.exit(1)

    def send_msg(self, msg, fail=True):
        smtp_username = self.get_config_item("smtp.username")
        smtp_password = self.get_config_item("smtp.password")

        sender = self.get_config_item('sender')
        rcpt = self.get_config_item('recipient')

        message = 'From: {}\n'.format(sender)
        message += 'To: {}\n'.format(rcpt)
        if fail:
            message += 'Subject: FAILURE to fetch data from ICANN CZDS\n\n'
        else:
            message += 'Subject: SUCCESS in fetching data from ICANN CZDS\n\n'
        message += msg

        sys.stderr.write('{}\n'.format(msg))
        sys.stderr.flush()

        try:
            smtpobj = smtplib.SMTP(self.get_config_item('smtp.server'),
                                   self.get_config_item('smtp.server.port'))
            if self.get_config_item('smtp.server.starttls'):
                smtpobj.ehlo()
                smtpobj.starttls()
            else:
                smtpobj.helo()
            smtpobj.login(smtp_username, smtp_password)
            smtpobj.sendmail(sender, rcpt, message)
            smtpobj.close()
        except smtplib.SMTPException as e:
            logging.error('Failed to send failure e-mail: {}'.format(str(e)))

    def get_with_token(self, url, stream=False):
        try:
            bearer_token_headers = {'Content-Type': 'application/json',
                                    'Accept': 'application/json',
                                    'Authorization': 'Bearer {0}'.format(self.access_token)}

            response = requests.get(url, headers=bearer_token_headers, stream=stream, proxies=self.proxies)

            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                raise GetError("GET for '{}' returned 404 (not found)".format(url))
            elif response.status_code == 401:
                raise GetError("GET for '{}' returned 401 (not authorised)".format(url))
            elif response.status_code == 500:
                raise GetError("GET for '{}' returned 500 (internal server error)".format(url))
            else:
                raise GetError("GET for '{}' returned error {}".format(url, response.status_code))
        except Exception as e:
            raise GetError("Failed to GET '{}' ({})".format(url, e))

    def get_zonefiles_list(self):
        """ Get all the files that need to be downloaded using CZDS API.
        """
        # Fetch the list of zones
        while self.retries <= self.get_config_item('max_retries'):
            zonelist_url = self.get_config_item("czds.download_base_url") + '/czds/downloads/links'
            try:
                zonelist_response = self.get_with_token(zonelist_url).json()
            except GetError as e:
                logging.error("Caught exception in get_zonefiles_list, retry #{}. Error: {}".format(self.retries, e))
                sys.stderr.write("Caught exception in get_zonefiles_list, retry #{}. Error: {}".format(self.retries, e))
                if self.retries == self.get_config_item('max_retries'):
                    raise CZDSError("Maximum number of retries reached while trying to obtain zonelist.")
                else:
                    self.retries += 1
            else:
                try:
                    # remove duplicate zone files
                    full_list = list(zonelist_response)
                    distinct_list = list(set(zonelist_response))
                    if len(distinct_list) != len(full_list):
                        logging.warning("Duplicate entries in zonefile list.")
                        sys.stderr.write("Duplicate entries in zonefile list.\n")
                        self.send_msg("Duplicate entries in zonefile list.")
                except Exception as e:
                    raise CZDSError("Unable to parse JSON returned from CZDS: \n" + str(e))

                self.downloadable_zones = len(distinct_list)
                logging.info("get_zonefiles_list returns {} zones".format(len(distinct_list)))
                logging.debug("get_zonefiles_list returning zones are: {}".format(distinct_list))
                return distinct_list

    def fetch_zone(self, zone, zone_name):
        """ Do a regular GET call to fetch zonefile
        """
        logging.debug("Downloading zone '{}' from '{}'".format(zone_name, zone))

        while self.retries <= self.get_config_item('max_retries'):
            try:
                download = self.get_with_token(zone, stream=True)
            except GetError as e:
                logging.error("Caught exception in fetch_zone, retry #{}. Error: {}".format(self.retries, e))
                sys.stderr.write("Caught exception in fetch_zone, retry #{}. Error: {}".format(self.retries, e))
                if self.retries == self.get_config_item('max_retries'):
                    logging.error("Maximum number of retries reached while trying to obtain zonefiles. " 
                                  "Last zone attempted (and failed): {}".format(zone_name))
                    self.send_msg("Maximum number of retries reached while trying to obtain zonefiles. "
                                  "Last zone attempted (and failed): {}".format(zone_name))
                    raise CZDSError("Maximum number of retries reached while trying to obtain zonefiles. "
                                    "Last zone attempted (and failed): {}".format(zone_name))
                else:
                    self.retries += 1
            else:
                if 'Content-Type' not in download.headers:
                    self.send_msg("GET for '{}' did not return a content type in the header".format(zone))
                    raise CZDSError("GET for '{}' did not return a content type in the header".format(zone))

                if download.headers['Content-Type'] != 'application/x-gzip':
                    self.send_msg("Unsupported content type '{}' for '{}'"
                                  .format(download.headers['Content-Type'], zone))
                    raise CZDSError("Unsupported content type '{}' for '{}'"
                                    .format(download.headers['Content-Type'], zone))
                return download

    def fetch(self):
        which_zones = self.get_config_item("zones", "all")
        logging.info('Fetching the following zones from CZDS: {}'.format(which_zones))

        try:
            zonelist = self.get_zonefiles_list()
        except CZDSError as e:
            self.send_msg("Unrecoverable error: could not obtain zonefiles list" + str(e))
            sys.exit(1)

        # Fetch the zones specified
        for zone in zonelist:
            # Extract the zone name from the URL
            zone_name = zone.split('/')[-1]
            if which_zones == 'all' or zone_name in which_zones:
                try:
                    download = self.fetch_zone(zone, zone_name)
                except CZDSError as e:
                    logging.error("Failed to download zone: " + str(e))
                else:
                    out_name = '{}/{}.gz'.format(self.directory, zone_name)

                    if 'Content-Length' in download.headers:
                        logging.info('Downloading {} bytes to {}'.format(download.headers['Content-Length'], out_name))
                    else:
                        logging.info('Downloading to {}'.format(out_name))

                    try:
                        out_fd = open(out_name, 'wb')

                        for chunk in download.iter_content(int(self.get_config_item('output_buffer_size', 1024*1024))):
                            out_fd.write(chunk)
                            out_fd.flush()

                        out_fd.close()
                    except Exception as e:
                        self.send_msg("Failed to write zone '{}' to file ({})".format(zone_name, e))
                        sys.stderr.write("CZDS: After downloading {} domains, fatal error occurred: {}.\n"
                                         .format(self.downloaded_zones, e))
                        logging.error("CZDS: After downloading {} domains, fatal error occurred: {}."
                                      .format(self.downloaded_zones, e))
                        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, help="use config file")
    args = parser.parse_args()

    if not args.config:
        print("No config file given, trying default (config.json).")
        args.config = "config.json"

    downloader = CZDSDownloader(args.config)

    downloader.czds_authenticate()

    downloader.fetch()

    logging.info("Complete, downloaded {} zone files of {}."
                 .format(downloader.downloaded_zones, downloader.downloadable_zones))
    sys.stderr.write("CZDownloads: Complete, downloaded {} zone files of {}.\n"
                     .format(downloader.downloaded_zones, downloader.downloadable_zones))
    downloader.send_msg("Downloaded {} zonefiles of {}."
                        .format(downloader.downloaded_zones, downloader.downloadable_zones), fail=False)


if __name__ == "__main__":
    main()
