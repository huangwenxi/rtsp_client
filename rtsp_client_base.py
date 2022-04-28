#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：calc_camera_pix_offset 
@File    ：rtsp_client_base.py
@Author  ：huangwenxi
@Date    ：2022/4/24 17:27 
'''
import time

import bitstring

from log import Logger
import os
import socket
import threading
import json
import abc
import re
import math
import queue

logger = Logger(os.path.basename(__file__)).getlog()
MAX_BUFFER_LEN = 10240
NAL_START_CODE = "\x00\x00\x00\x01"
BIT_SIZE_2_BYTES = 16
BIT_SIZE_4_BYTES = 32


class RTSPCmd:
    OPTIONS = 'OPTIONS'
    DESCRIBE = 'DESCRIBE'
    SETUP = 'SETUP'
    PLAY = 'PLAY'


class RTPHeader:
    RTP_VER_START_BIT = 0
    RTP_VER_END_BIT = 2
    RTP_PADDING_START_BIT = 2
    RTP_PADDING_END_BIT = 3
    RTP_EXTENSION_START_BIT = 3
    RTP_EXTENSION_END_BIT = 4
    RTP_CSRC_COUNT_START_BIT = 4
    RTP_CSRC_COUNT_END_BIT = 8
    RTP_MARK_START_BIT = 8
    RTP_MARK_END_BIT = 9
    RTP_PAYLOAD_START_BIT = 9
    RTP_PAYLOAD_END_BIT = 16
    RTP_SEQUENCE_NUMBER_START_BIT = 16
    RTP_SEQUENCE_NUMBER_END_BIT = 32
    RTP_TIMESTAMP_START_BIT = 32
    RTP_TIMESTAMP_END_BIT = 64
    RTP_SSRC_START_BIT = 64
    RTP_SSRC_END_BIT = 96
    RTP_CSRC_START_BIT = 96


class RTSPCSeq:
    OPTIONS = 1
    DESCRIBE = 2
    SETUP_VIDEO = 3
    SETUP_AUDIO = 4
    PLAY = 5


class RTPFragmentType:
    SINGLE_NAL_MAX = 23
    FU_A = 28


class NALUnitType:
    NONE_IDX = 1
    A = 2
    B = 3
    C = 4
    IDX = 5
    SEI = 6
    SPS = 7
    PPS = 8


class RtspClientBase(metaclass=abc.ABCMeta):
    def __init__(self, rtsp_server_ip, rtsp_server_port, url):
        self._rtsp_server_ip = rtsp_server_ip
        self._rtsp_server_port = rtsp_server_port
        self._url = url
        self._rtsp_session_connection_status = False
        self._rtsp_session_id = 0
        self._rtsp_session_timeout = 0
        self._cseq_function_map = {RTSPCSeq.OPTIONS: self._parse_option_response,
                                   RTSPCSeq.DESCRIBE: self._parse_describe_response,
                                   RTSPCSeq.SETUP_VIDEO: self._parse_setup_video_response,
                                   RTSPCSeq.SETUP_AUDIO: self._parse_setup_audio_response,
                                   RTSPCSeq.PLAY: self._parse_play_response}
        self._rtp_socket = None
        self._i_received_flag = False
        self._frame_queue = queue.Queue()

    def connect(self):
        """
        连接到rtsp服务器
        :return: 返回连接的结果，True表示成功 False表示失败
        """
        if not self._init_rtsp_session():
            return
        logger.info('启动RTSP消息解析任务')
        return self._create()

    @abc.abstractmethod
    def disconnect(self):
        """
        释放资源，比如说创建的rtp rtcp rtsp的socket
        :return:
        """

    def read_frame(self):
        """
        读取视频流数据
        :return: 视频流数据
        """
        return self._frame_queue.get()

    @abc.abstractmethod
    def _create(self):
        """
        如果需要的话在子类创建rtp的socket
        :return:
        """
        pass

    def _init_rtsp_session(self):
        """
        创建用于rtsp连接的的tcp链路
        :return:初始化成功返回True 失败返回False
        """
        try:
            self._rtsp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._rtsp_socket.connect((self.rtsp_server_ip, self.rtsp_server_port))
            self._rtsp_session_connection_status = True
            logger.info('初始化rtsp session connection 成功')
            return True
        except Exception as e:
            logger.error('连接到RTSP服务器的tcp服务端失败 {}, ip:{}, port:{}'.format(e.args, self.rtsp_server_ip,
                                                                      self.rtsp_server_port))
            self._rtsp_session_connection_status = True
            return False

    def _release_rtsp_session(self):
        """
        释放rtsp的资源
        :return:
        """
        try:
            if self._rtsp_socket:
                self._rtsp_socket.close()
        except Exception as e:
            logger.error('释放RTSP的socket资源失败 {}'.format(e.args))

    def _start_rtsp_flow(self):
        """
        开启RTSP的交互流程
        :return:
        """
        logger.info('开始RTSP的交互流程')
        self._option()
        self._describe()

    @property
    def rtsp_server_ip(self):
        return self._rtsp_server_ip

    @property
    def rtsp_server_port(self):
        return self._rtsp_server_port

    @property
    def url(self):
        return self._url

    @property
    def session_id(self):
        return self._rtsp_session_id

    def _option(self):
        """
        获取rtsp支持的方法并解析
        :return:
        """
        cmd = '{} {} RTSP/1.0 \r\n' \
              'CSeq: {} \r\n' \
              'User-Agent: Lavf57.83.100 \r\n\r\n'.format(RTSPCmd.OPTIONS, self.url, RTSPCSeq.OPTIONS).encode()
        self._rtsp_socket.send(cmd)
        logger.info('发送OPTION消息成功')

    def _parse_option_response(self, data):
        """
        解析option的回复
        :param data:
        :return:
        """
        if '200 OK' in data:
            logger.info(' RTSP 回复 OPTION 成功')
        else:
            logger.error('RTSP 回复 OPTION 失败')

    def _describe(self):
        """
        获取视频流的属性并解析
        :return:
        """
        cmd = '{} {} RTSP/1.0 \r\n' \
              'CSeq: {} \r\n' \
              'User-Agent: Lavf57.83.100 \r\n\r\n'.format(RTSPCmd.DESCRIBE, self.url, RTSPCSeq.DESCRIBE).encode()
        self._rtsp_socket.send(cmd)
        logger.info('发送DESCRIBE消息成功')

    def _parse_describe_response(self, data):
        """
        解析订阅的回复
        :param data:
        :return:
        """
        if '200 OK' not in data:
            logger.error('RTSP回复DESCRIBE失败')
            return
        if 'Session:' in data:
            session_pattern = re.compile(r'Session: (.*)\r\n')
            result = session_pattern.findall(data)
            self._rtsp_session_id = result[0]
        logger.info('RTSP回复DESCRIBE成功')
        self._setup_video()

    @abc.abstractmethod
    def _setup_video(self):
        """
        建立和rtsp server视频的连接
        :return:
        """
        pass

    def _parse_setup_video_response(self, data):
        """
        解析setup视频的回复
        :param data:
        :return:
        """
        if '200 OK' not in data:
            logger.error('SET VIDEO失败')
            return
        if 'timeout' in data and 'Session' in data:
            session_pattern = re.compile(r'Session: (.*);timeout=(.*)\r\n')
            result = session_pattern.findall(data)
            self._rtsp_session_id = result[0][0]
            self._rtsp_session_timeout = result[0][1]
        else:
            session_pattern = re.compile(r'Session: (.*)\r\n')
            result = session_pattern.findall(data)
            self._rtsp_session_id = result[0]
        logger.info(result)
        logger.info('RTSP回复SETUP成功 session id:{} timeout:{}'.format(self._rtsp_session_id, self._rtsp_session_timeout))
        self._play()

    @abc.abstractmethod
    def _setup_audio(self):
        """
        建立和rtsp server音频的连接
        :return:
        """
        cmd = '{} {} '

    def _parse_setup_audio_response(self, data):
        """
        解析setup音频的回复
        :param data:
        :return:
        """
        pass

    def _play(self):
        """
        播放视频流
        :return:
        """
        cmd = '{} {} RTSP/1.0\r\n' \
              'Range: npt=0.000-\r\n' \
              'CSeq: {}\r\n' \
              'User-Agent: Lavf57.83.100\r\n' \
              'Session: {}\r\n\r\n'.format(RTSPCmd.PLAY, self.url, RTSPCSeq.PLAY, self._rtsp_session_id).encode()
        self._rtsp_socket.send(cmd)
        logger.info('发送PLAY消息成功')

    def _parse_play_response(self, data):
        """
        解析播放的回复
        :param data:
        :return:
        """
        if '200 OK' not in data:
            logger.error('RTSP 回复PLAY失败')
            return
        logger.info('RTSP回复PLAY成功，和RTSP服务器之间建立连接完成')

    def _rtsp_response_parse(self, data):
        """
        解析rtsp的回复消息
        :param data:
        :return:
        """
        if not len(data):
            return
        if data.startswith('RTSP'):
            pattern = re.compile(r'(?<=CSeq: )\d+\.?\d*')
            communication_sequence = int(pattern.findall(data)[0])
            self._cseq_function_map[communication_sequence](data)
        elif data.startswith('ANNOUNCE'):
            pass
        else:
            logger.warning('received {}'.format(data))

    def _rtp_packet_parse(self, complete_packet):
        """
        输入是一个完成的RTP的数据包，解析rtp完整的包，从中获取H264数据和扩展头
        :param complete_packet:RTP包
        :return: 视频有效数据，曝光时间戳
        """
        rtp_extension = []
        bt = bitstring.BitArray(
            bytes=complete_packet)
        lc = 12
        bc = 12 * 8

        version = bt[0:2].uint
        p = bt[2]
        "是否存在扩展头"
        x = bt[3]
        "固定头后面 CSRC 识别符的数目"
        cc = bt[4:8].uint
        m = bt[8]
        pt = bt[9:16].uint

        sn = bt[16:32].uint  # sequence number
        timestamp = bt[32:64].uint  # timestamp
        ssrc = bt[64:96].uint  # ssrc identifier

        lc = 12  # so, we have red twelve bytes
        bc = 12 * 8  # .. and that many bits

        cids = []
        for i in range(cc):
            cids.append(bt[bc:bc + 32].uint)
            bc += 32
            lc += 4

        if x:
            hid = bt[bc:bc + 16].uint
            bc += 16
            lc += 2
            hlen = bt[bc:bc + 16].uint
            bc += 16
            lc += 2
            # # frame_ts = f'{(ts_s + ts_ms * 232.83 / math.pow(10, 12) - 2208988800):0<.6f}'.replace('.', '')
            # frame_ts = ts_s + ts_ms/1000
            for index in range(hlen):
                rtp_extension.append(bt[bc+32*index:bc + 32 * (index+1)].uint)
            bc += 32 * hlen
            lc += 4 * hlen

        # FU identifier
        fu_identifier_f_bit = bt[bc]  # i.e. "F"
        fu_identifier_nri = bt[bc + 1:bc + 3].uint  # "NRI"
        fu_identifier_f_bit_nri = bt[bc:bc + 3]  # "3 NAL UNIT BITS" (i.e. [F | NRI])
        fu_identifier_fragment_type = bt[bc + 3:bc + 8].uint  # "Type"

        # FU header
        bc += 8
        lc += 1

        fu_header_start_bit = bt[bc]  # fu_header_start_bit bit
        fu_header_end_bit = bt[bc + 1]  # fu_header_end_bit bit
        fu_header_nal_unit_type = bt[bc + 3:bc + 8]  # 5 nal unit bits

        nal_start_code_and_nal_header = b""
        nal_header = fu_identifier_f_bit_nri + fu_header_nal_unit_type  # Create "[3 NAL UNIT BITS | 5 NAL UNIT BITS]"
        if fu_header_start_bit:  # OK, this is a first fragment in a movie frame
            nal_start_code_and_nal_header = NAL_START_CODE.encode() + nal_header.bytes  # .. add the NAL starting sequence
        lc += 1
        if fu_identifier_fragment_type == RTPFragmentType.FU_A:  # This code only handles "Type" = 28, i.e. "FU-A"
            if fu_header_nal_unit_type.uint == NALUnitType.IDX:
                self._i_received_flag = True
            if not self._i_received_flag:
                logger.info('not Param.get_i_frame')
                return None, []
            if fu_header_start_bit:
                ret = nal_start_code_and_nal_header + complete_packet[lc:]
                return ret, rtp_extension
            else:
                ret = complete_packet[lc:]
                return ret, []
        elif fu_identifier_fragment_type <= RTPFragmentType.SINGLE_NAL_MAX:
            ret = NAL_START_CODE.encode() + complete_packet[lc - 2:]
            return ret, []
        else:
            logger.info('return None, None')
            return None, []


