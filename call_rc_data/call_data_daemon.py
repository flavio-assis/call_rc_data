#!/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
import requests
import datetime
from psycopg2 import connect 


class call_data(object):
    """
    Class to get data from rc-demands.

    Connect into rc-demands api and return a dict with the last data over the last hour.
    """

    def __init__(self):
        """Init some variables."""

        self.user = "cc_stone"
        self.host = "172.16.6.16"
        self.passwd = "St0n3$$Cc"
        self.port = 5480
        self.db = "database_single"
        self.item = '"{}": "{}"'
        self.error_msg = "{} ERROR message='{}'\n"
        self.url = "http://10.10.4.18:8088/services/collector"
        self.query = "SELECT * FROM callcent_queuecalls where time_start between '{}' and now()"
        self.last_time = datetime.datetime.now() - datetime.timedelta(minutes=1)
        self.column_names = ['idcallcent_queuecalls', 'q_num', 'time_start', 'time_end', 'ts_waiting',
                             'ts_polling', 'ts_servicing', 'ts_locating', 'count_polls', 'count_dialed',
                             'count_rejected', 'count_dials_timed', 'reason_noanswercode', 'reason_failcode',
                             'reason_noanswerdesc', 'reason_faildesc', 'call_history_id', 'q_cal', 'from_userpart',
                             'from_displayname', 'to_dialednum', 'to_dn', 'to_dntype', 'cb_num', 'call_result',
                             'deal_status', 'is_visible']
        self.column_utils = ['ts_waiting', 'ts_polling', 'ts_servicing', 'count_polls', 'count_rejected', 'to_dn',
                             'call_history_id', 'q_cal', 'from_userpart', 'from_displayname', 'call_result']
        self.payload = """{{\n\t\"index\": \"rc_calls_database\",
\t\"sourcetype\": \"_json\",
\t\"event\":
\t{{
\t\t\"created_at\": \"{}\",
\t\t\"subject\": \"Send data from rc database to splunk\",
\t\t\"status\": \"Ok\",
\t\t\"requester_name\": \"OCC_Analitycs\",
\t\t\"department_name\": \"Command Center\",
\t\t{}
\t}}
}}"""
        self.headers = {'Content-Type': "application/json",
                        'Accept': "application/json",
                        'Authorization': "Splunk 5a70da0f-68c4-411f-b6b9-1b6ea1454211",
                        'cache-control': "no-cache"}

    def conn(self):
        try:
            _conn = connect(host=self.host, user=self.user, password=self.passwd, port=self.port, database=self.db)
            _cur = _conn.cursor()
            return _cur
        except Exception as error:
            full_time = datetime.datetime.now().strftime('%d/%m/%Y %T')
            sys.stderr.write(self.error_msg.format(full_time, error))

    def format_fields(self, _dict):
        _dict['from_displayname'] = _dict['from_displayname'].split(":")[1].split('-')[0].strip()
        return _dict

    def get_data(self):
        """Return output data to be collect by splunk and generate a index to be indexed."""

        try:
            cur = self.conn()
            cur.execute(self.query.format(self.last_time))
            get_data = cur.fetchall()
            if len(get_data) > 0:
                items = [dict(zip(self.column_names, i)) for i in get_data]
                self.last_time = items[-1]['time_start']
                list_items = []
                for item in items:
                    _n = {}
                    _ = [_n.update({"{}".format(y):"{}".format(z)}) for y, z in item.items() if y in self.column_utils]
                    _n = self.format_fields(_n)
                    list_items.append(str(_n).rstrip("}").lstrip("{").replace(", ", ",\n\t\t").replace("'", '\"'))
                self.send_data(list_items)
        except Exception as error:
            full_time = datetime.datetime.now().strftime('%d/%m/%Y %T')
            sys.stderr.write(self.error_msg.format(full_time, error))

    def send_data(self, _data):

        data_payload = []
        for d in _data:
            full_time = datetime.datetime.now().strftime('%d/%m/%Y %T')
            data_payload.append(self.payload.format(full_time, d))
        
        with open('/tmp/dpay', 'w') as dpay:
            dpay.write("\n".join(data_payload))
        
        for _data in data_payload:
            _ = requests.request("POST", self.url, data=_data, headers=self.headers)