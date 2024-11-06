#!/usr/bin/env python
# -*- coding: utf-8 -*-
set -ex
docker build -t cal-code -f jy_test/dockerfile
docker rm -f cal-code; docker run -d --env-file .env \
    -v /home/cal_code/scripts:/app/cal_code \
    -v /opt/logs/vola:/app/vola \
    --name=cal-code cal-code