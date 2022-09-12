# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

import logging
import logging.handlers
import sys

class Logger:

    def __init__(self, f, console=False, maxBytes=100000000, backupCount=1000):
        self.f = f
        self.logger = None
        self.setup(console, maxBytes, backupCount)

    def setup(self, console, maxBytes, backupCount):
        try:
            # setup the logger
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
            handler = logging.handlers.RotatingFileHandler(
                self.f, maxBytes=maxBytes, backupCount=backupCount)
            formatter = logging.Formatter("%(asctime)s-%(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            if console: self.logger.addHandler(logging.StreamHandler(sys.stdout))
        except Exception as err:
            raise LoggerException(err)

class LoggerException(Exception):
    pass