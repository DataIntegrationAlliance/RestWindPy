# -*- coding: utf-8 -*-
"""
Created on 2016-12-22
@author: MG
"""
import pandas as pd
import requests
import json
from datetime import datetime, date

STR_FORMAT_DATE = '%Y-%m-%d'
STR_FORMAT_DATETIME_WIND = '%Y-%m-%d %H:%M:%S'  # 2017-03-06 00:00:00.005000
UN_AVAILABLE_DATETIME = datetime.strptime('1900-01-01', STR_FORMAT_DATE)
UN_AVAILABLE_DATE = UN_AVAILABLE_DATETIME.date()


def format_2_date_str(dt) -> str:
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


def format_2_datetime_str(dt) -> str:
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


class APIError(Exception):
    def __init__(self, status, ret_dic):
        self.status = status
        self.ret_dic = ret_dic

    def __str__(self):
        return "APIError:status=POST / {} {}".format(self.status, self.ret_dic)


class WindRest:
    def __init__(self, url_str):
        self.url = url_str
        self.header = {'Content-Type': 'application/json'}

    def _url(self, path: str) -> str:
        return self.url + path

    def public_post(self, path: str, req_data: str) -> list:

        # print('self._url(path):', self._url(path))
        ret_data = requests.post(self._url(path), data=req_data, headers=self.header)
        ret_dic = ret_data.json()

        if ret_data.status_code != 200:
            raise APIError(ret_data.status_code, ret_dic)
        else:
            return ret_data.status_code, ret_dic

    def wset(self, table_name, options) -> pd.DataFrame:
        path = 'wset/'
        req_data_dic = {"table_name": table_name, "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wss(self, codes, fields, options="") -> pd.DataFrame:
        path = 'wss/'
        req_data_dic = {"codes": codes, "fields": fields, "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wsd(self, codes, fields, begin_time, end_time, options="") -> pd.DataFrame:
        path = 'wsd/'
        req_data_dic = {"codes": codes, "fields": fields,
                        "begin_time": format_2_date_str(begin_time),
                        "end_time": format_2_date_str(end_time),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wsi(self, codes, fields, begin_time, end_time, options="") -> pd.DataFrame:
        path = 'wsi/'
        req_data_dic = {"codes": codes, "fields": fields,
                        "begin_time": format_2_date_str(begin_time),
                        "end_time": format_2_date_str(end_time),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wst(self, codes, fields, begin_time, end_time, options="") -> pd.DataFrame:
        path = 'wst/'
        req_data_dic = {"codes": codes, "fields": fields,
                        "begin_time": format_2_datetime_str(begin_time),
                        "end_time": format_2_datetime_str(end_time),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wsq(self, codes, fields, options="") -> pd.DataFrame:
        path = 'wsq/'
        req_data_dic = {"codes": codes, "fields": fields, "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def tdaysoffset(self, offset, begin_time, options="") -> dict:
        path = 'tdaysoffset/'
        req_data_dic = {"offset": offset,
                        "begin_time": format_2_date_str(begin_time),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        date_str = json_dic['Date']
        return date_str

    def tdays(self, begin_time, end_time, options="") -> dict:
        path = 'tdays/'
        req_data_dic = {"begin_time": format_2_date_str(begin_time),
                        "end_time": format_2_date_str(end_time),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        # df = pd.DataFrame(json_dic)
        return json_dic

    def edb(self, codes, begin_time, end_time, options) -> pd.DataFrame:
        path = 'edb/'
        req_data_dic = {"codes": codes,
                        "begin_time": format_2_date_str(begin_time),
                        "end_time": format_2_date_str(end_time),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

if __name__ == "__main__":
    # url_str = "http://10.0.5.65:5000/wind/"
    url_str = "http://10.0.3.78:5000/wind/"
    rest = WindRest(url_str)
    # data_df = rest.wset(table_name="sectorconstituent", options="date=2017-03-21;sectorid=1000023121000000")
    # data_df = rest.wss(codes="QHZG160525.OF", fields="fund_setupdate,fund_maturitydate,fund_mgrcomp,fund_existingyear,fund_ptmyear,fund_type,fund_fundmanager")
    # data_df = rest.wsd("601398.SH", "open,high,low,close,volume", "2017-01-04", "2017-02-28", "PriceAdj=F")
    # data_df = rest.tdays(begin_time="2017-01-04", end_time="2017-02-28")
    # data_df = rest.wst("600000.SH", "ask1,bid1,asize1,bsize1,volume,amt,pre_close,open,high,low,last", "2017-10-20 09:15:00", "2017-10-20 09:26:00", "")
    # data_df = rest.wsi("RU1801.SHF", "open,high,low,close,volume,amt,oi", "2017-12-8 09:00:00", "2017-12-8 11:30:00", "")

    try:
        # w.wsd("601398.SH", "open,high,low,close,volume", "2017-09-29", "2017-10-28", "")
        data_df = rest.wsi("AU1801.SHF", "open,high,low,close,volume,amt,oi", "2017-12-7 2:25:00", "2017-12-8 9:05:00", "")
        print(data_df)
    except APIError as exp:
        if exp.status == 500:
            print('APIError.status:', exp.status, exp.ret_dic['message'])
        else:
            print(exp.ret_dic.setdefault('error_code', ''), exp.ret_dic['message'])
    # date_str = rest.tdaysoffset(1, '2017-3-31')
    # print(date_str)
