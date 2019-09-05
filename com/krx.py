import io
import json
import time
import requests
import pandas as pd
from pandas.io.json import json_normalize
import sqlite3


def 주식종목검색기():
    url_tmpl = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=COM%2Ffinder_stkisu&name=form&_={}'
    url = url_tmpl.format(int(time.time() * 1000))
    r = requests.get(url)

    down_url = 'http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx'
    down_data = {
        'mktsel': 'ALL',
        'pagePath': '/contents/COM/FinderStkIsu.jsp',
        'code': r.content,
        'geFirstCall': 'Y',
    }
    r = requests.post(down_url, down_data)
    jo = json.loads(r.text)
    return json_normalize(jo, 'block1')


def 상장회사검색():
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name': 'fileDown',
        'filetype': 'xls',
        'url': 'MKD/04/0406/04060100/mkd04060100_01',
        'market_gubun': 'ALL',  # ''ALL':전체, STK': 코스피
        'isu_cdnm': '전체',
        'sort_type': 'A',
        'std_ind_cd': '01',
        'cpt': '1',
        'in_cpt': '',
        'in_cpt2': '',
        'pagePath': '/contents/MKD/04/0406/04060100/MKD04060100.jsp',
    }

    r = requests.post(gen_otp_url, gen_otp_data)
    code = r.content

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }
    headers = {'Referer': 'http://marketdata.krx.co.kr'}
    r = requests.post(down_url, down_data, headers=headers)

    df = pd.read_excel(io.BytesIO(r.content), converters={'종목코드': str, '업종코드': str}, thousands=',')
    df.rename(columns={'기업명': '종목명'}, inplace=True)
    return df


if __name__ == "__main__":
    # 주식종목검색기
    df_master = 주식종목검색기()

    # 컬럼 이름 변경
    dict_cols = {'codeName': '종목명', 'full_code': '표준코드', 'marketName': '시장', 'short_code': '종목코드'}
    df_master.rename(columns=dict_cols, inplace=True)

    # 종목코드(6자리로)
    df_master['종목코드'] = df_master['종목코드'].str[1:]

    # 종목코드를 인덱스로 설정
    df_master.set_index('종목코드', inplace=True)

    # 상장회사검색
    df_corp = 상장회사검색()
    df_corp.set_index('종목코드', inplace=True)
    df_corp.rename(columns={'상장주식수(주)': '상장주식수', '자본금(원)': '자본금', '액면가(원)': '액면가'}, inplace=True)
    cols = ['업종코드', '업종', '상장주식수', '자본금', '액면가', '통화구분', '대표전화', '주소']
    df_master[cols] = df_corp[cols]

    conn = sqlite3.connect('stock_master.db')
    df_master.to_sql('stock_master', conn, if_exists='replace')
    print(len(df_master), '종목의 종목 마스터를 갱신하였습니다')
    conn.close()
