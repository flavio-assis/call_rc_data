#!/usr/bin/python3.6
# -*- coding: utf-8 -*-


import os
import time
from datetime import datetime


class rc_logger():
    """Create file, directories and log errors."""

    def __init__(self, msg_error):
        self.msg = msg_error
        self.time = datetime.utcfromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')
        self.directory = "/var/log/rc_call"
        self.log_file = os.path.join(str(self.directory), "rc_call.log")

    def check_dir(self):
        """Check if log directory exists and create if not."""

        if not os.path.isdir(self.directory):
            os.mkdir(self.directory, 755)
        if not os.path.exists(self.log_file):
            from pathlib import Path
            Path(self.log_file).touch()

    def log_data(self):
        """Log errors into file rc_call.log in the directory /var/log/rc_call."""

        self.check_dir()
        with open(self.log_file, "a") as logger_file:
            logger_file.write("{}, {}\n".format(self.time, self.msg))