#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：calc_camera_pix_offset 
@File    ：rtsp_client.py
@Author  ：huangwenxi
@Date    ：2022/4/26 9:57 
'''
import time
from rtsp_client_udp import RtspClientUdp
from rtsp_client_tcp import *
import re
from log import Logger
import os
import cv2
logger = Logger(os.path.basename(__file__)).getlog()


class RTPProtocol:
    RTP_OVER_TCP = 1
    RTP_OVER_UDP = 2


class RtspClient:
    def __init__(self, rtp_protocol, rtsp_url):
        # rtp承载的协议类型
        self._rtp_protocol = rtp_protocol
        self._rtsp_url = rtsp_url
        # 从url中解析ip和端口号
        self._ip = rtsp_url.split('//')[1].split('/')[0].split(':')[0]
        self._port = int(rtsp_url.split('//')[1].split('/')[0].split(':')[1])
        logger.info('从URL解析得到ip:{} port:{}'.format(self._ip, self._port))
        # 根据rtp承载的协议创建指定的RTSP客户端对象
        if rtp_protocol == RTPProtocol.RTP_OVER_UDP:
            self._rtsp_client = RtspClientUdp(self._ip, self._port, self._rtsp_url)
        elif rtp_protocol == RTPProtocol.RTP_OVER_TCP:
            self._rtsp_client = RtspClientTcp(self._ip, self._port, self._rtsp_url)
        else:
            logger.error('RTP协议初始化失败')
        # 测试录制的视频和时间戳
        self._video_name = 'test.h264'
        if os.path.exists(self._video_name):
            os.remove(self._video_name)
        self._video_fd = open(self._video_name, 'wb')

        self._timestamp_name = 'test.timestamp'
        if os.path.exists(self._timestamp_name):
            os.remove(self._timestamp_name)
        self._timestamp_fd = open(self._timestamp_name, 'w')

    def connect(self):
        """
        连接到RTSP server
        :return:True成功 False失败
        """
        return self._rtsp_client.connect()

    def disconnect(self):
        """
        释放资源
        :return:
        """
        try:
            self._rtsp_client.disconnect()
            self._video_fd.close()
            self._timestamp_fd.close()
        except Exception as e:
            logger.error('释放资源失败 :{}'.format(e.args))

    def read_frame(self):
        """
        读取视频帧和曝光时间戳
        :return: [视频帧，曝光时间]
        """
        return self._rtsp_client.read_frame()

    def write_h264(self, frame):
        self._video_fd.write(frame)

    def write_snap_timestamp(self, timestamp):
        self._timestamp_fd.writelines('{} \n'.format(timestamp))


if __name__ == '__main__':
    ivs = IVS3800('10.10.43.12', 'aokan_2022', 'broadxt@333', 300)
    ivs.login()
    # ivs.run()
    ivs.get_device_list()
    _camera_code = ivs.get_camera_code('10.10.40.28')
    ret, url = ivs.get_playback_resource(_camera_code, '20220425110000', '20220425120000')
    logger.info('ret:{}, url:{}'.format(ret, url))
    if not ret:
        exit(-1)
    rtsp_client = RtspClient(RTPProtocol.RTP_OVER_TCP, url)
    rtsp_client.connect()
    time_start = time.time()
    try:
        while time.time() < time_start + 10:
            data, rtp_extension = rtsp_client.read_frame()
            logger.info('读取 {} 字节的数据 扩展头:{}'.format(len(data), rtp_extension))
            rtsp_client.write_h264(data)
        rtsp_client.disconnect()
    except Exception as e:
        logger.error(e.args)
