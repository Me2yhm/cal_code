#!/usr/bin/env python
# -*- coding: utf-8 -*-
set -ex
ImageName="jy-calcode"
docker build -t registry.cn-shanghai.aliyuncs.com/jqy-common/$ImageName -f jy_test/dockerfile .
docker push registry.cn-shanghai.aliyuncs.com/jqy-common/$ImageName
call_remote="ssh root@172.16.7.34"
$call_remote "docker pull registry.cn-shanghai.aliyuncs.com/jqy-common/$ImageName"
$call_remote "docker rm -f $ImageName" 
$call_remote "docker run -d --env-file .env \
    -v /opt/vola:/app/vola \
    --name=$ImageName\
    registry.cn-shanghai.aliyuncs.com/jqy-common/$ImageName"