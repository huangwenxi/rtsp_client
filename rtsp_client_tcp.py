#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：calc_camera_pix_offset 
@File    ：rtsp_client_tcp.py
@Author  ：huangwenxi
@Date    ：2022/4/25 10:04 
'''
from rtsp_client_base import RtspClientBase, RTSPCmd, RTSPCSeq
from log import Logger
import os
import threading
logger = Logger(os.path.basename(__file__)).getlog()
MAX_BUFFER_LEN = 10240


class RtspClientTcp(RtspClientBase):
    def __init__(self, rtsp_server_ip, rtsp_server_port, url):
        RtspClientBase.__init__(self, rtsp_server_ip, rtsp_server_port, url)
        self._rtsp_data_buffer = b''

    def connect(self):
        """
        连接到RTSP server并启动解析程序
        :return:True 成功 False失败
        """
        if not super(RtspClientTcp, self).connect():
            logger.error('创建socket资源失败')
            return False
        parse_rtsp_task = threading.Thread(target=self._rtsp_rtp_msg_parse_task, args=())
        parse_rtsp_task.start()
        return True

    def _create(self):
        """
        创建rtp和rtcp的服务端
        :return:
        """
        self._rtp_socket = self._rtsp_socket
        return True

    def disconnect(self):
        """
        释放创建的socket资源
        :return:
        """
        self._release_rtsp_session()

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
              'Transport: RTP/AVP/TCP;unicast;interleaved=0-1 \r\n' \
              'CSeq: {} \r\n' \
              'User-Agent: Lavf57.83.100 \r\n' \
              'Session: {} \r\n\r\n'.format(RTSPCmd.SETUP, self.url,
                                            RTSPCSeq.SETUP_VIDEO, self._rtsp_session_id).encode()
        self._rtsp_socket.send(cmd)
        logger.info('发送SETUP视频消息成功')

    def _rtsp_rtp_msg_parse_task(self):
        """
        解析rtsp和rtp的消息
        :return:
        """
        logger.info('开始运行RTSP消息解析任务')
        self._start_rtsp_flow()
        try:
            while True:
                data = self._rtsp_socket.recv(MAX_BUFFER_LEN)
                if not len(data):
                    continue
                logger.info('从socket收到 {} 字节的数据'.format(len(data)))
                result = self._split_rtsp_rtp(data)
                for packet_type, packet in result:
                    if packet_type is 'RTSP':
                        logger.info(packet[0])
                        packet = packet.decode()
                        self._rtsp_response_parse(packet)
                    elif packet_type is 'RTP':
                        frame, snap_timestamp = self._rtp_packet_parse(packet)
                        self._frame_queue.put([frame, snap_timestamp])
                    else:
                        logger.warn('无有效数据包')
        except Exception as e:
            logger.error('RTSP消息接收线程出错:{}'.format(e.args))

    def _split_rtsp_rtp(self, data):
        self._rtsp_data_buffer += data
        result = []
        try:
            while len(self._rtsp_data_buffer) >= 4:
                if self._rtsp_data_buffer[0] == 36:
                    channel = self._rtsp_data_buffer[1]
                    length = self._rtsp_data_buffer[2]*256 + self._rtsp_data_buffer[3]
                    if len(self._rtsp_data_buffer) < length + 4:
                        logger.error('当前RTP包不完整，等待更多数据')
                        break
                    if channel == 0:
                        logger.info('当前数据属于RTP数据,长度:{}'.format(length))
                        complete_packet = self._rtsp_data_buffer[4:length + 4]
                        self._rtsp_data_buffer = self._rtsp_data_buffer[4 + length:]
                        result.append(['RTP', complete_packet])
                    else:
                        logger.info('当前数据输入RTCP数据, {} 长度:{}'.format(channel, length))
                        self._rtsp_data_buffer = self._rtsp_data_buffer[4 + length:]
                else:
                    logger.info('当前数据是RTSP数据,长度:{}'.format(len(data)))
                    self._rtsp_data_buffer = b''
                    result.append(['RTSP', data])
                    break
            return result
        except Exception as e:
            logger.error('分割rtsp和rtp消息出错{}'.format(e.args))


