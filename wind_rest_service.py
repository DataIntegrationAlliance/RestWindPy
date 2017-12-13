# -*- coding: utf-8 -*-
"""
Created on 2016-12-22
@author: MG
"""
from flask import request, Flask
from flask_restplus import Resource, Api
from WindPy import w
import pandas as pd
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)
STR_FORMAT_DATE = '%Y-%m-%d'
STR_FORMAT_DATETIME_WIND = '%Y-%m-%d %H:%M:%S'  # 2017-03-06 00:00:00
UN_AVAILABLE_DATETIME = datetime.strptime('1900-01-01', STR_FORMAT_DATE)
UN_AVAILABLE_DATE = UN_AVAILABLE_DATETIME.date()

app = Flask(__name__)

api = Api(app,
          title='Wind Rest API',
          version='0.0.1',
          description='',
          )

header = {'Content-Type': 'application/json'}
rec = api.namespace('wind', description='')

ERROR_CODE_MSG_DIC = {
    -40522005: "不支持的万得代码",
    -40522003: "非法请求",
    -40520007: "没有可用数据",
    -40521009: "数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月",
    -40521010: "网络超时",
    -40522017: "数据提取量超限",
}


def format_2_date_str(dt):
    if dt is None:
        return None
    dt_type = type(dt)
    if dt_type == str:
        return dt
    elif dt_type == date:
        if dt > UN_AVAILABLE_DATE:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    elif dt_type == datetime:
        if dt > UN_AVAILABLE_DATETIME:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    else:
        return dt


def format_2_datetime_str(dt):
    if dt is None:
        return None
    dt_type = type(dt)
    if dt_type == str:
        return dt
    elif dt_type == date:
        if dt > UN_AVAILABLE_DATE:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    elif dt_type == datetime:
        if dt > UN_AVAILABLE_DATETIME:
            return dt.strftime(STR_FORMAT_DATETIME_WIND)
        else:
            return None
    else:
        return dt


@rec.route('/wset/')
class ReceiveWSET(Resource):
    def post(self):
        """
        json str:{"table_name": "sectorconstituent", "options": "date=2017-03-21;sectorid=1000023121000000"}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        logger.info('/wset/ data_dic:%s' % data_dic)
        # print('data_dic:%s' % data_dic)
        table_name = data_dic['table_name']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.wset(table_name, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('wset("%s", "%s") ErrorCode=%d %s' % (table_name, options, error_code, msg))
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404

        data_count = len(ret_data.Data)
        if data_count > 0:
            # print('ret_data.Fields\n', ret_data.Fields)
            ret_data.Data[0] = [format_2_date_str(dt) for dt in ret_data.Data[0]]
            # print('ret_data.Data\n', ret_data.Data)
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields, columns=ret_data.Codes)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/wsd/')
class ReceiveWSD(Resource):
    def post(self):
        """
        json str:{"codes": "603555.SH", "fields": "close,pct_chg", "begin_time": "2017-01-04", "end_time": "2017-02-28", "options": "PriceAdj=F"}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        # print(request.json)
        logger.info('/wsd/ data_dic:%s' % data_dic)
        codes = data_dic['codes']
        fields = data_dic['fields']
        begin_time = data_dic['begin_time']
        end_time = data_dic['end_time']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.wsd(codes, fields, begin_time, end_time, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('wsd("%s", "%s", "%s", "%s", "%s") ErrorCode=%d %s' % (
                codes, fields, begin_time, end_time, options, error_code, msg))
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404
        # if ret_data.ErrorCode != 0:
        #     logger.error('wsd("%s", "%s", "%s", "%s", "%s") ErrorCode=%d' % (
        #         codes, fields, begin_time, end_time, options, ret_data.ErrorCode))
        #     return {'error_code': ret_data.ErrorCode}, 404
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)

        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                # 取出第一个部位None的数据
                for item_check in data:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields,
                              columns=[format_2_date_str(dt) for dt in ret_data.Times])
        # print(ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/wsi/')
class ReceiveWSI(Resource):
    def post(self):
        """
        json str:{"codes": "RU1801.SHF", "fields": "open,high,low,close,volume,amt,oi", "begin_time": "2017-12-11 09:00:00", "end_time": "2017-12-11 10:27:41", "options": ""}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        # print(request.json)
        logger.info('/wsi/ data_dic:%s' % data_dic)
        codes = data_dic['codes']
        fields = data_dic['fields']
        begin_time = data_dic['begin_time']
        end_time = data_dic['end_time']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.wsi(codes, fields, begin_time, end_time, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('wsi("%s", "%s", "%s", "%s", "%s") ErrorCode=%d %s' % (
                codes, fields, begin_time, end_time, options, error_code, msg))
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404
        # if ret_data.ErrorCode != 0:
        #     logger.error('wsd("%s", "%s", "%s", "%s", "%s") ErrorCode=%d' % (
        #         codes, fields, begin_time, end_time, options, ret_data.ErrorCode))
        #     return {'error_code': ret_data.ErrorCode}, 404
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)

        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                # 取出第一个部位None的数据
                for item_check in data:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_datetime_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields,
                              columns=[format_2_datetime_str(dt) for dt in ret_data.Times])
        # print(ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/wss/')
class ReceiveWSS(Resource):
    def post(self):
        """
        json str:{"codes": "XT1522613.XT", "fields": "fund_setupdate,fund_maturitydate,fund_mgrcomp,fund_existingyear,fund_ptmyear,fund_type,fund_fundmanager", "options": ""}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        logger.info('/wss/ data_dic:%s', data_dic)
        codes = data_dic['codes']
        fields = data_dic['fields']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.wss(codes, fields, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('wss("%s", "%s", "%s") ErrorCode=%d %s' % (codes, fields, options, error_code, msg))
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)
        logger.debug('ret_data.Data len:%d', data_len)
        logger.debug('ret_data.Codes : %s', ret_data.Codes)
        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                if type(data[0]) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # print('ret_data.Data:\n', ret_data.Data)
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields, columns=ret_data.Codes)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/tdaysoffset/')
class ReceiveTdaysoffset(Resource):
    def post(self):
        """
        json str:{"offset": "1", "begin_time": "2017-3-31", "options": ""}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        logger.info('/tdaysoffset/ data_dic:%s', data_dic)
        offset = int(data_dic['offset'])
        begin_time = data_dic['begin_time']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.tdaysoffset(offset, begin_time, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('tdaysoffset("%s", "%s", "%s") ErrorCode=%d %s' % (offset, begin_time, options, error_code, msg))
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404
        # if ret_data.ErrorCode != 0:
        #     logger.error(
        #         'tdaysoffset("%s", "%s", "%s") ErrorCode=%d' % (offset, begin_time, options, ret_data.ErrorCode))
        #     return {'error_code': ret_data.ErrorCode}, 404
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        if len(ret_data.Data) > 0 and len(ret_data.Data[0]) > 0:
            date_str = format_2_date_str(ret_data.Data[0][0])
        else:
            logger.warning('tdaysoffset(%s, %s, %s) No value return' % (offset, begin_time, options))
            date_str = ''
        ret_dic = {'Date': date_str}
        # print('offset:\n', ret_dic)
        return ret_dic


@rec.route('/tdays/')
class ReceiveTdays(Resource):
    def post(self):
        """
        json str:{"begin_time": "2017-3-31", "end_time": "2017-3-31", "options": ""}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        logger.info('/tdays/ data_dic:%s', data_dic)
        begin_time = data_dic['begin_time']
        end_time = data_dic['end_time']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.tdays(begin_time, end_time, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('tdays("%s", "%s", "%s") ErrorCode=%d %s' % (begin_time, end_time, options, error_code, msg))
            if ret_data.ErrorCode == 40521010:
                w.close()
                w.start()
                logger.warning('网络连接超时，端口重新启动')
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404
        # if ret_data.ErrorCode != 0:
        #     logger.error(
        #         'tdays("%s", "%s", "%s") ErrorCode=%d' % (begin_time, end_time, options, ret_data.ErrorCode))
        #     if ret_data.ErrorCode == 40521010:
        #         w.close()
        #         w.start()
        #         logger.warning('网络连接超时，端口重新启动')
        #     return {'error_code': ret_data.ErrorCode}, 404
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        if len(ret_data.Data) > 0 and len(ret_data.Data[0]) > 0:
            # date_str = format_datetime_to_str(ret_data.Data[0][0])
            # ret_df = pd.DataFrame({'date': [format_datetime_to_str(d) for d in ret_data.Data[0]]})
            # ret_df.index = [str(idx) for idx in ret_df.index]
            # ret_dic = {'date': [format_datetime_to_str(d) for d in ret_data.Data[0]]}
            ret_dic = [format_2_date_str(d) for d in ret_data.Data[0]]
        else:
            logger.warning('tdays(%s, %s, %s) No value return' % (begin_time, end_time, options))
            ret_dic = []
        # ret_dic = ret_df.to_dict()
        # print('tdays:\n', ret_dic)
        return ret_dic


@rec.route('/wsq/')
class ReceiveWSQ(Resource):
    def post(self):
        """
        json str:{"codes": "600008.SH,600010.SH,600017.SH", "fields": "rt_open,rt_low_limit", "options": ""}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        logger.info('/wsq/ data_dic:%s', data_dic)
        codes = data_dic['codes']
        fields = data_dic['fields']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.wsq(codes, fields, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('wsq("%s", "%s", "%s") ErrorCode=%d %s' % (codes, fields, options, error_code, msg))
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)
        logger.debug('ret_data.Data len:%d', data_len)
        logger.debug('ret_data.Codes : %s', ret_data.Codes)
        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                if type(data[0]) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # print('ret_data.Data:\n', ret_data.Data)
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields, columns=ret_data.Codes)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/wst/')
class ReceiveWST(Resource):
    def post(self):
        """
        json str:{"codes": "600008.SH, "fields": "ask1,bid1,asize1,bsize1,volume,amt,pre_close,open,high,low,last", "begin_time": "2017-01-04", "end_time": "2017-02-28", "options": ""}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        logger.info('/wst/ data_dic:%s', data_dic)
        codes = data_dic['codes']
        fields = data_dic['fields']
        begin_time = data_dic['begin_time']
        end_time = data_dic['end_time']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.wst(codes, fields, begin_time, end_time, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('wst("%s", "%s", "%s", "%s", "%s") ErrorCode=%d %s' % (
                codes, fields, begin_time, end_time, options, error_code, msg))
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404
        # if ret_data.ErrorCode != 0:
        #     logger.error('wsd("%s", "%s", "%s", "%s", "%s") ErrorCode=%d' % (
        #         codes, fields, begin_time, end_time, options, ret_data.ErrorCode))
        #     return {'error_code': ret_data.ErrorCode}, 404
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)

        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                # 取出第一个部位None的数据
                for item_check in data:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_datetime_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields,
                              columns=[format_2_datetime_str(dt) for dt in ret_data.Times])
        # print(ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/edb/')
class ReceiveEDB(Resource):
    def post(self):
        """
        json str:{"codes": "M0017126,M0017127,M0017128", "begin_time": "2016-11-10", "end_time": "2017-11-10", "options": "Fill=Previous"}
        :return: 返回万得返回数据dict
        """
        data_dic = request.json
        logger.info('/edb/ data_dic:%s', data_dic)
        codes = data_dic['codes']
        begin_time = data_dic['begin_time']
        end_time = data_dic['end_time']
        options = data_dic['options']
        if not w.isconnected():
            w.start()
        if options == "":
            options = None
        ret_data = w.edb(codes, begin_time, end_time, options)
        error_code = ret_data.ErrorCode
        if error_code != 0:
            msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
            logger.error('wst("%s", "%s", "%s", "%s", "%s") ErrorCode=%d %s' % (
                codes, begin_time, end_time, options, error_code, msg))
            return {'error_code': ret_data.ErrorCode, 'message': msg}, 404
        # if ret_data.ErrorCode != 0:
        #     logger.error('wsd("%s", "%s", "%s", "%s", "%s") ErrorCode=%d' % (
        #         codes, fields, begin_time, end_time, options, ret_data.ErrorCode))
        #     return {'error_code': ret_data.ErrorCode}, 404
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)

        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                # 取出第一个部位None的数据
                for item_check in data:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=[xx.strip() for xx in codes.split(',')],
                              columns=[format_2_date_str(dt) for dt in ret_data.Times])
        # print(ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


def start_service():
    if w.isconnected():
        w.start()
    try:
        app.run(host="0.0.0.0", debug=True)
    finally:
        w.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    start_service()
