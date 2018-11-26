# encoding: utf-8

# DBまわり
import sqlalchemy
import sqlalchemy.orm
import pandas as pd

# ファイル操作まわり
import glob
import shutil

import sys
import os
import codecs

# Webスクレイピングおよび解析まわり
import requests
from bs4 import BeautifulSoup

import xml.etree.ElementTree as ET



import os
import re 
import io, sys
#sys.stdout = io.TextIOWrapper(sys.stdout.buffer,
#                              encoding=sys.stdout.encoding, 
#                              errors='backslashreplace', 
#                              line_buffering=sys.stdout.line_buffering)

import datetime
import yaml
from collections import OrderedDict


#################################################################################        

# SQLAlchemy初期化
CONNECT_INFO = 'mssql+pyodbc://fins' # FINS鯖の[Edinet]データベース(odbc接続)
engine = sqlalchemy.create_engine(CONNECT_INFO, encoding='utf-8')


def xbrl(vle):
    
    # 件数取得
    tx = str(len(vle)) + "件"
    tx = tx + '\n\n' + '----'+ '\n'

    # 各ファイルの読み込み及び項目取得
    for i,url in enumerate(vle["URL"]):

        #print("■" +  vle["MentionedName"][i])
        fn = vle["CodeNumber"][i]  #EdinetCode
        #print(fn )
        #print( url )
        
        # XBRLファイルの格納dirを特定
        u1 = url.replace('index.html','').replace('http://','\\').replace('/','\\')
        
        #print(u1 )
        x = u1.rsplit('\\')
        #print(x )
        u12 = u1 + str(x[9]) + '\\' + 'XBRL' + '\\' + 'PublicDoc' + '\\'
        #print( u12 )

        # ローカルにファイルダウンロード
        for file in glob.glob(str("\\") + u12 + '*.xbrl'):
            #print(file)
            f1 = str(fn) + '.xbrl'
            current_dir = os.getcwd() 
            xbrl_path = current_dir +"/work/" +  f1 
            shutil.copy(file, xbrl_path )

            # sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
            # f = open(f1 + '.txt', 'w') 
        
        # XBRLパース
        dict = get_itm(xbrl_path )
        
        ReleaseDate = dict["FilingDateCoverPage"]
        print(ReleaseDate )
        print('--1--' ) 
        print(tdy )
        print('--3--' )              
        if ReleaseDate == tdy:
            for k, v in dict.items():
                #print(k, v)
                txt = str(txt) + str(k) +':' + str(v) + '\n' 

            fn_msg(txt)
       

    # メールの末尾の文言
    #tx = tx + '\n' + '[参考]東証HP↓' + '\n' + 'http://www.jpx.co.jp/listing/stocks/new/index.html'
    
    # 戻り値を返す
    return tx

def fn_msg(txt):
    webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAA-teiBlQ/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=9wIIj8AsYLpNZDGAc0p-Bma9aVaVJpiiuDxx2JBTpoo%3D'
    print(txt)

    response = requests.post(
               webhook_url,
               json={"text": txt } #,"image":"test0.png" [response:400]
               )
               
def get_itm( xbrl_path ):


    print( xbrl_path)
    # 変数宣言
    lst = []  #取得項目リスト
    
    #xbrl_path = 'work/E21671.xbrl'


    ### タクソノミーURLの取得(using beautiful Soup)
    ff = open( xbrl_path , "r" ,encoding="utf-8" ).read() 
    soup = BeautifulSoup( ff ,"html.parser")

    #find_allが効いてない？
    #x = soup.find_all("xbrli:xbrl" )
    x = str(soup )[0:2000] 
    print(soup.string)

    x = x.rsplit("xmlns")

    # 該当attr（crpとdei）の取得
    crp  =['{' + i.replace( i[0:11] ,"").rstrip(' ').replace('"',"") + '}' for i in x[1:10] if i[0:11] == ':jpcrp_cor=']
    dei  =['{' + i.replace( i[0:11] ,"").rstrip(' ').replace('"',"") + '}' for i in x[1:10] if i[0:11] == ':jpdei_cor=']
    crp,dei = crp[0] ,dei[0]

    #print(crp)
    #print(dei)

    ### 取得項目のリスト化
    #1.EDinetCode/証券コード/英文社名/日本語社名/直近本決算の会計基準/直近本決算_期初/直近本決算_期末
    lst1= ['EDINETCodeDEI','SecurityCodeDEI','FilerNameInJapaneseDEI','FilerNameInEnglishDEI','AccountingStandardsDEI']  
    lst1= lst1 + ['CurrentFiscalYearEndDateDEI','CurrentFiscalYearStartDateDEI']
    lst1 =[ str(dei)+str(i)  for i in lst1]
    #print(lst1)

    #2.提出者/提出者の連絡先住所/提出者の連絡先TEL/特別記載事項/提出日
    lst2= ['TitleAndNameOfRepresentativeCoverPage','NearestPlaceOfContactCoverPage','TelephoneNumberNearestPlaceOfContactCoverPage']
    lst2= lst2 + ['SpecialDisclosureAboutPublicOfferingOrSecondaryDistributionTextBlock','FilingDateCoverPage']
    lst2 =[ str(crp)+str(i) for i in lst2]
    #print(lst2)
    lst = lst1 + lst2 
    #print(lst3) 
    # XBRLの項目値取得
    dict = get_vle( xbrl_path ,lst, crp , dei)

    # 戻り値を返す
    return dict
            
def get_vle( xbrl_path ,lst, crp , dei):
    dict = OrderedDict() # 格納用の辞書
    
    # XML項目の取得(using elementTree)
    tree = ET.parse( xbrl_path ) 
    root = tree.getroot() 
            
    for c1 in root:
        for i in lst :
            if c1.tag == i:
               
               ### 値の取得 
               tx = str(c1.text)

               #特別事項の記述の部分から上場市場名を抽出する
               if c1.tag == crp + 'SpecialDisclosureAboutPublicOfferingOrSecondaryDistributionTextBlock':
                  spc =  c1.text.split("</p>") #</p>を区切り文字にして配列化
                          
                  tx = str(spc[0:4])
                  for j in spc:
                      #上場市場についての記載文章を特定
                      if j.find("上場")  > -1 and j.find("予定") > -1 and j.find("主幹事")> -1: 
                         if j.find("株式について、")  > -1: 
                            tx = '...' + str( j.split('株式について、')[1])
                         else:
                            tx = str(j)
                            hr = j.split('">')
                            for q in hr:
                                if q.find("上場")  > -1 and q.find("予定") > -1 and q.find("主幹事")> -1: 
                                   tx = str(q)
                          
  
               
               ### 取得した値を辞書に格納
               if c1.tag.find("BusinessResults") == -1:
                  a = str(i).replace(dei,'').replace(crp,'')
                  dict[a] = tx

                 
                             
    # 戻り値を返す
    return dict


def rtrv_db():

    # セッション作成
    #Session = sqlalchemy.orm.sessionmaker(bind=engine)
    #session = Session()
    
    # SQL文の生成
    fl = "CodeNumber,DocumentName,URL,MentionedName,InsertTime"
    tb = "dbo.EdinetDocumentList"
    wh1 = "InsertTime >= DATEADD(DAY,0, DATEDIFF(Day,0, DATEADD(day, 0 ,GETDATE())))"
    
    #wh1 = "InsertTime >= '2018-11-19 15:00:00.000'"
    wh2 = "DocumentType = 4 AND DocumentName LIKE '%新規公開%'"

    # SQL実行
    query = "SELECT {0} FROM {1} WHERE {2} AND {3}".format(fl,tb,wh1,wh2)
    vle = pd.read_sql_query(query, engine)  #pandasを使う必然性はないがノリで...
    
    # セッションあとしまつ
    #session.commit()
    #session.close()
    
    return vle


# yamlファイル読み込み
def initialize_yaml():
    str = open('context.yaml').read()
    yamls = yaml.load(str)
    return yamls
    
        
# main
if __name__ == '__main__':

    #datetime.date.today()#今日の日付
    tdy = datetime.datetime.today().strftime("%Y/%m/%d %H:%M:%S")
    tx = str(tdy) + "時点の検索結果：" 

    # DB検索
    vle = rtrv_db()
    
    # XBRL参照
    if vle.empty:
       print( "開示なし" )
       ret = "0件" + '\n' + "（開示なし）"
    else:
       # XBRL参照
       ret = xbrl(vle)

       #for k, v in dict.items():
       #        print("(a)" + k)
       #        print("(b)" +v)

    
    print("finish!")
  
    