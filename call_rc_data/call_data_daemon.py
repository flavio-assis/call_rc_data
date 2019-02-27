#!/usr/bin/python3.6
# -*- coding: utf-8 -*-


import os
import sys
import time
import json
import requests
import datetime
from psycopg2 import connect 
from call_rc_data.rc_logger import rc_logger
from call_rc_data.crypto_phone import crypto_phone


class call_data(object):
    """
    Class to get data from rc-demands.

    Connect into rc-demands api and return a dict with the last data over the last hour.
    """

    def __init__(self):
        """Init some variables."""

        self.port = 5480
        self.id_last = ""
        self.id_list = []
        self.last_id_call = ""
        self.item = '"{}": "{}"'
        self.host = os.environ['DB_IP']
        self.user = os.environ['DB_USER']
        self.passwd = os.environ['DB_PASSWD']
        self.db = "database_single"
        self.crypto_phone = crypto_phone(os.environ['PHONES_KEY'])
        self.error_msg = "{} ERROR message='{} {}'\n"
        self.url = os.environ['SPLUNK_URL']
        self.query = "SELECT * FROM callcent_queuecalls where idcallcent_queuecalls > {}"
        self.column_names = ['idcallcent_queuecalls', 'q_num', 'time_start', 'time_end', 'ts_waiting',
                             'ts_polling', 'ts_servicing', 'ts_locating', 'count_polls', 'count_dialed',
                             'count_rejected', 'count_dials_timed', 'reason_noanswercode', 'reason_failcode',
                             'reason_noanswerdesc', 'reason_faildesc', 'call_history_id', 'q_cal', 'from_userpart',
                             'from_displayname', 'to_dialednum', 'to_dn', 'to_dntype', 'cb_num', 'call_result',
                             'deal_status', 'is_visible']
        self.column_utils = ['ts_waiting', 'ts_polling', 'ts_servicing', 'count_polls', 'count_rejected', 'to_dn',
                             'q_num', 'idcallcent_queuecalls', 'call_history_id', 'q_cal', 'from_userpart',
                             'from_displayname', 'call_result', 'time_start', 'time_end']
        self.payload = """{{\n  \"index\": \"rc.call.metrics\",
  \"sourcetype\": \"_json\",
  \"time\": {},
  \"event\":
  {{
    {}
  }}
}}"""
        self.headers = {'Content-Type': "application/json",
                        'Accept': "application/json",
                        'Authorization': "Splunk {}".format(os.environ['SPLUNK_TOKEN']),
                        'cache-control': "no-cache"}

    def conn(self):
        """Connect on the RC database."""

        try:
            _conn = connect(host=self.host, user=self.user, password=self.passwd, port=self.port, database=self.db)
            _cur = _conn.cursor()
            return _cur
        except Exception as error:
            full_time = datetime.datetime.now().strftime('%d/%m/%Y %T')
            rc_logger(self.error_msg.format(full_time, error, 'conn')).log_data()

    def get_last_id(self):
        """Get the last id from the table callcent_queuecalls"""

        try:
            cur = self.conn()
            cur.execute("SELECT idcallcent_queuecalls FROM callcent_queuecalls order by idcallcent_queuecalls desc limit 1")
            _last_id = cur.fetchall()
            return _last_id[0][0]
        except Exception as error:
            full_time = datetime.datetime.now().strftime('%d/%m/%Y %T')
            rc_logger(self.error_msg.format(full_time, error, 'conn')).log_data()

    def format_fields(self, _dict):
        """Format fields to protect phone number and split displayname."""

        try:
            if _dict['from_userpart'].startswith('0'):
                _dict.update({"ddd": "{}".format(_dict['from_userpart'][:3])})
            else:
                _dict.update({"ddd": "0{}".format(_dict['from_userpart'][:2])})
            _dict['from_userpart'] = self.crypto_phone.encrypt(_dict['from_userpart']).decode()
            _dict['from_displayname'] = _dict['from_displayname'].split(":")[1].split('-')[0].strip()
        except:
            pass
        return _dict

    def get_data(self):
        """Return output data to be collect by splunk and generate a index to be indexed."""

        try:
            print('teste0')
            if self.last_id_call == "":
                self.last_id_call = self.get_last_id()
            cur = self.conn()
            cur.execute(self.query.format(self.last_id_call))
            get_data = cur.fetchall()
            if len(get_data) > 0:
                items = [dict(zip(self.column_names, i)) for i in get_data]
                self.last_id_call = items[-1]['idcallcent_queuecalls']
                list_items = []
                for item in items:
                    _n = {}
                    _ = [_n.update({"{}".format(y):"{}".format(z)}) for y, z in item.items() if y in self.column_utils]
                    _n.update({"type_name": "QueueCalls"})
                    self.id_last = _n['call_history_id']
                    self.id_list.append(self.id_last)
                    _n = self.format_fields(_n)
                    if self.id_list.count(self.id_last) < 2:
                        list_items.append(str(_n).rstrip("}").lstrip("{").replace("'", '\"'))
                    if len(list_items) > 1000:
                        self.send_data(list_items)
                        list_items = []
                self.send_data(list_items)
        except Exception as error:
            full_time = datetime.datetime.now().strftime('%d/%m/%Y %T')
            rc_logger(self.error_msg.format(full_time, error, 'get_data')).log_data()

    def send_data(self, _data):
        """Send data to splunk."""

        try:
            print('teste')
            data_payload = []
            for d in _data:
                full_time = int(datetime.datetime.strptime(d.split("\"time_start\": \"")[1].split('"')[0], "%Y-%m-%d %H:%M:%S").timestamp())
                data_payload.append(self.payload.format(full_time, d))
        
            with open('/tmp/dpay', 'w') as dpay:
                dpay.write("\n".join(data_payload))
        
            _ = requests.request("POST", self.url, data="\n".join(data_payload), headers=self.headers)
            print(_.text)
            
            if len(self.id_list) > 1000:
                self.id_list.pop(0)
        except Exception as error:
            full_time = datetime.datetime.now().strftime('%d/%m/%Y %T')
            rc_logger(self.error_msg.format(full_time, error, 'send_data')).log_data()
