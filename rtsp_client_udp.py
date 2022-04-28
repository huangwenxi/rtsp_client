#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：calc_camera_pix_offset 
@File    ：rtsp_client_udp.py
@Author  ：huangwenxi
@Date    ：2022/4/25 10:04 
'''
import socket
import time
import threading
from rtsp_client_base import RtspClientBase, RTSPCmd, RTSPCSeq
from log import Logger
import os
import math
logger = Logger(os.path.basename(__file__)).getlog()
MAX_BUFFER_LEN = 10240


class RtspClientUdp(RtspClientBase):
    def __init__(self, rtsp_server_ip, rtsp_server_port, url):
        RtspClientBase.__init__(self, rtsp_server_ip, rtsp_server_port, url)
        logger.info('ip:{} port:{} url:{}'.format(rtsp_server_ip, rtsp_server_port, url))
        self._video_rtp_port = 61234
        self._video_rtcp_port = 61235
        self._video_rtp_socket = None
        self._video_rtcp_socket = None

    def connect(self):
        if not super(RtspClientUdp, self).connect():
            logger.error('创建socket资源失败')
            return False
        parse_rtsp_task = threading.Thread(target=self._rtsp_msg_parse_task, args=())
        parse_rtsp_task.start()
        parse_rtp_task = threading.Thread(target=self._rtp_data_parse_task, args=())
        parse_rtp_task.start()
        logger.info('启动RTSP消息解析任务成功')
        return True

    def disconnect(self):
        """
        释放创建的socket资源
        :return:
        """
        try:
            # 释放rtp的连接资源
            self._video_rtp_socket.close()
            logger.info('释放rtp的socket成功')
            # 释放rtcp的连接资源
            self._video_rtcp_socket.close()
            logger.info('释放rtcp的socket成功')
            # 释放rtsp的连接资源
            self._release_rtsp_session()
            logger.info('释放rtsp的socket成功')
        except Exception as e:
            logger.error('释放资源失败:{}'.format(e.args))

    def _rtp_data_parse_task(self):
        """
        解析RTP的数据
        :return:
        """
        if not self._rtp_socket:
            logger.error('RTP的链路不存在')
            return
        while True:
            data = self._rtp_socket.recv(MAX_BUFFER_LEN)
            if not len(data):
                continue
            # logger.debug('received len:{} {}'.format(len(data), data))
            complete_packet = data
            if not complete_packet:
                logger.warning('等待完整的RTP包')
                continue
            frame, snap_timestamp = self._rtp_packet_parse(complete_packet)
            self._frame_queue.put([frame, snap_timestamp])

    def _rtsp_msg_parse_task(self):
        """
        解析rtsp的消息
        :return:
        """
        logger.info('开始运行RTSP消息解析任务')
        self._start_rtsp_flow()
        try:
            while True:
                data = self._rtsp_socket.recv(MAX_BUFFER_LEN)
                if not len(data):
                    continue
                logger.info('从socket收到 {} 字节的数据 {}'.format(len(data), data))
                data = data.decode()
                self._rtsp_response_parse(data)
        except Exception as e:
            logger.error('RTSP消息接收线程出错:{}'.format(e.args))

    def _create(self):
        """
        创建rtp和rtcp的服务端
        :return:
        """
        try:
            # 创建rtp的连接资源
            self._video_rtp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._video_rtp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
            self._video_rtp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
            self._video_rtp_socket.bind(("", self._video_rtp_port))
            self._rtp_socket = self._video_rtp_socket
            logger.info('创建本地接收RTP数据的TCP 服务成功, port:{}'.format(self._video_rtp_port))
            # 创建rtcp的连接资源
            self._video_rtcp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._video_rtcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
            self._video_rtcp_socket.bind(("", self._video_rtcp_port))
            logger.info('创建本地接收RTCP数据的TCP 服务成功 port:{}'.format(self._video_rtcp_port))
            return True
        except Exception as e:
            logger.error('创建服务端失败,{}'.format(e.args))
            return False

    def _setup_audio(self):
        """
        和rtsp服务器建立音频的连接
        :return:
        """
        pass

    def _setup_video(self):
        """
        和rtsp服务器建立视频的连接
        :return:
        """
        cmd = '{} {}/trackID=1 RTSP/1.0 \r\n' \
              'Transport: RTP/AVP;unicast;client_port={}-{} \r\n' \
              'CSeq: {} \r\n' \
              'User-Agent: Lavf57.83.100 \r\n' \
              'Session: {} \r\n\r\n'.format(RTSPCmd.SETUP, self.url,
                                            self._video_rtp_port, self._video_rtcp_port,
                                            RTSPCSeq.SETUP_VIDEO, self._rtsp_session_id).encode()
        self._rtsp_socket.send(cmd)
        logger.info('发送SETUP视频消息成功')


if __name__ == '__main__':
    rtsp_client = RtspClientUdp('10.10.10.53', 554,
                                'rtsp://10.10.10.53:554/LiveMedia/ch1/Media1')
    rtsp_client.connect()
    time_start = time.time()
    video_fd = open('test_udp.h264', 'wb')
    timestamp_fd = open('test_udp.timestamp', 'w')
    while time.time() < time_start + 10:
        data, rtp_extension = rtsp_client.read_frame()
        video_fd.write(data)
    video_fd.close()
    timestamp_fd.close()
    rtsp_client.disconnect()



