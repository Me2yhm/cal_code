#!/usr/bin/env python
# -*- coding: utf-8 -*-
set -ex
docker build -t cal-code .
docker rm -f cal-code; docker run -d -p 9004:9004 \
    -v /home/cal_code:/cal_code \
    -v /opt/logs/vola:/cal_code/vola \
    --name=cal-code cal-code