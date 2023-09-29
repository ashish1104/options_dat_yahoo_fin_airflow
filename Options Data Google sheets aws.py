import gspread as gc
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import  time as tm
import pandas as pd, numpy as np
import pickle, os, datetime, json, requests, locale
from datetime import datetime, date, time, timedelta
from yahoo_fin import options as op
import yfinance as yf
from urllib3 import HTTPResponse 


# Google Authentication
credentials = {
  "type": "service_account",
  "project_id": "putoptions",
  "private_key_id": "fbea0fbe5a287ca4315427dbfb744695908437cd",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC/sOVfA+XK5GDI\n/ielA2pdEtOtHU68yfQhZ6Qu+3PZzG1NBI7PMWeE4DK+DZl+k5i/S8rnX4gWxWXp\n/QOJ67hr8dC24z4Fa/TiXp3WuG4M/eS64QFSzTMUNRM/R7/80KI1pcSyCX+nGNQd\nSO/7yPbRYmJ0X4iYzm5CS5vBbGkAxAe3HP8c9GDYXikpmiU3BFFZqOF7uiZ3Je9A\nOhSJ4BNgrUMWagnqQ0e6ZtYI+19MjbIZTthPn6rqmlNf2Rzk0h1+Bqb3oIPP+rYr\n6beISfvBdmCkgqeS0sR+KheXz+lY+5QOOECpJdtp6o35ssMkVDK/2Z0FhtdXnNrg\nqArhuWbTAgMBAAECggEAEijPaeNkIwbkllXhapP6wQt2CxnSKjdVjc6UMrE2heUE\nW735S26KOH/GvtNp/aA84a5r7+RQb5vOrk5+RdQTWgKD+7dmWjwibEl7WWqKXme1\nAfEr32y6EImAo8eHzAr1uvBTxGv68Fj9SzLsLSWaIDbaTdz5A0TXFCe2fynmfqBp\ngJyi7UEw+/KdBNc8xIMrRiyNiEWmHsMI6QbGQFgVDjhjAKPNNHsDjnyVWCVdz8fl\npFASctDb6+JFA0Qf6+bs7BRp2Py2LtHS/9hVf6AyzNNdORzwFZrsk5X6HxAX6a/X\ndPVIMgbslha4759TibBNPiThRhD/JCrBMxmZWTfDDQKBgQDjFn4axzmepkp/aQEp\nvOrE0NRibpgHY0NdrtXXGvHpmcySvH+dMg8tS5hIUOY5OW6U6+W/kq9WtDlRafZV\ndl1a7OnWA28MK1/aMmqHTvnbO1zL49ytpQdNCIDfc7ElIRhtSbR0H1NaPsgsyz2Z\nTJK3DxLJWTBqHK83stftk1tOpQKBgQDYGLVXlnt5uLN8lSWwLHrUClT454VFwjcR\n96oUHLeNnK1ihXd4pST9Ul7RJcF8s2jIBV0bgV0n4EZf+NlZcHEOZURnVm4oD3uM\nt11czJzTMFj0lK+9lNpoCarhd0ztv/LF6u9Uk5b6mfqGsKR+RLO8CcEOuOHUXqlh\nYDV9aLYeFwKBgQCvpyVSB0ptiklTtmA87bFXHgU6Qjt8c26XV3Z3FZOL0vhnhEDd\n0evlaHTZcyIxn1gG/1VOUuMnmO5BKEpvnsflXh4d+bf63M4e1CKMmS0xRACqgY2g\nD2fPUUWGElIy2x+u4XUg4V4zDx79bxQtsQpHaPRqJIALhDaQSdk2HmZhNQKBgB/n\npf6EjT5vcrCWJLZaGthrnhgRtq+R+SeB9W56vu79juvY4MbDOy7blnwyotqScon1\noMWK2AInLrzEtPJrk4WYlfzb2e+4OXmRKQumehEENl4GBdxrucNaigw33PfNOz7V\nbPiDwerGCSMRuXebFR5SIlrWGSGNyFxEJCGb8CeJAoGBAKKQTmI3a2HgwbqviQKy\nwdTL9/YhpsAY2eyWbkwUhOfsCzCKUBhA9y9f6Hi3R44sfuGmHMybWTNkOpR5In9U\n0kl7wEza8ZMA6nQTYcZWiAY+mgD4Ccgfzee/TQV0HFmQqfE4MU0CvPdxXMz+QYwI\n1929IadhLrODWOYjOGMshzyp\n-----END PRIVATE KEY-----\n",
  "client_email": "python-access@putoptions.iam.gserviceaccount.com",
  "client_id": "109050425241888850775",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/python-access%40putoptions.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

gc = gc.service_account_from_dict(credentials)
locale.setlocale( locale.LC_ALL, '' )


@dag(
    schedule=None,
    start_date=datetime(2023, 9, 24),
    catchup=False,
    tags=["options"],
)

def options_pipe():

    @task()
    def get_all_tickers(wb_id,sheet_name):
        sheet = gc.open_by_key(wb_id).worksheet(sheet_name)
        values_list = sheet.col_values(1)
        ticker_list = []
        for i in values_list[1:]:
            ticker_list.append(i)
        return ticker_list

    @task()
    def stock_info_yahoo(ticker_list):
        data = yf.download('GOOG', period='1d')
        col_names = list(data)
        col_names.append('ticker')
        current_price = pd.DataFrame(columns = col_names)
        company_attributes_df = pd.DataFrame(columns=col_names)
        count = 0
        for i in ticker_list:
            try:
                print(count)
                count = count + 1
                company_attributes_df = pd.concat([company_attributes_df, pd.DataFrame(yf.Ticker(i.replace('.', '-')).info).drop('companyOfficers', axis=1).drop_duplicates()])
            except:     
                continue
        company_attributes_df = company_attributes_df[['symbol','shortName','country','industry','sector','longBusinessSummary','beta','forwardPE','fiftyTwoWeekLow','fiftyTwoWeekHigh','marketCap','currentPrice']]
        company_attributes_df['rank'] = company_attributes_df['marketCap'].rank(ascending=False)
        with open('company_attributes_df.pkl', 'wb') as f:
            pickle.dump(company_attributes_df, f)

    @task()
    def options_dat_yahoo(ticker_list):
        expirationDates = op.get_expiration_dates('AAPL')
        putData = op.get_puts('AAPL', date = expirationDates[10]).iloc[:0]

        putData = op.get_puts('AAPL', date = expirationDates[10]).iloc[:0]
        col_names = list(putData)
        col_names.extend(['ticker','exp_date','close_price','close_price_date'])
        putData = pd.DataFrame(columns=col_names)
        
        for ticker in ticker_list:
            expirationDates = op.get_expiration_dates(ticker)
            for exp_date in expirationDates[4:]:
                try:
                    temp = op.get_puts(ticker, date = exp_date)
                    temp['ticker'] = ticker
                    temp['exp_date'] = exp_date
                    putData = pd.concat([putData, temp])
                    print(ticker, exp_date)
                    # del temp
                except:
                    print('Error somewhere')
                    continue
        with open('putData.pkl', 'wb') as f:
            pickle.dump(putData, f)

    @task()
    def combine_data():
        with open('company_attributes_df.pkl', 'rb') as f:
            stock_info = pickle.load(f)
        with open('putData.pkl', 'rb') as f:
            options_dat = pickle.load(f)

        all_data = pd.merge(stock_info,options_dat, how= 'left', left_on ='symbol' ,right_on = 'ticker')
        all_data['exp_date'] = pd.to_datetime(all_data['exp_date'], format='%B %d, %Y').dt.date
        all_data['today'] = date.today()
        all_data['num_days'] = (all_data['exp_date'] - date.today())
        all_data['num_days'] = pd.to_timedelta(all_data.num_days, errors='coerce').dt.days
        all_data['exp_date'] =  all_data['exp_date'].astype(str)
        all_data['today'] =  all_data['today'].astype(str)
        all_data['Bid'] = pd.to_numeric(all_data['Bid'], errors='coerce')
        all_data['Ask'] = pd.to_numeric(all_data['Ask'], errors='coerce')
        all_data['Strike'] = pd.to_numeric(all_data['Strike'], errors='coerce')
        all_data = all_data.assign(daily_return =lambda x: ((x.Strike/(x.Strike-(x.Bid + x.Ask)/2))**(1/x.num_days))-1)
        
        all_data = all_data.assign(annual_return =lambda x: ((1+x.daily_return)**365)-1)
        all_data = all_data.assign(daily_decline =lambda x: ((x.Strike/x.currentPrice)**(1/x.num_days))-1)
        all_data = all_data.assign(current_strike_ratio =lambda x:x.Strike*1.00/x.currentPrice  )
        
        with open('all_data.pkl', 'wb') as f:
            pickle.dump(all_data, f)

    @task()
    def upload_data(wb_id,sheet_name):
        with open('all_data.pkl', 'rb') as f:
            all_data = pickle.load(f).fillna('')
        op_sheet = gc.open_by_key(wb_id).worksheet(sheet_name)
        op_sheet.clear()
        op_sheet.update([all_data.columns.values.tolist()] + all_data.values.tolist())
    
    ticker_list = get_all_tickers('1JPuCJUMEZfnsokn3WO_uLKvk4HrhYgLtwf6XLOeZGC4','input_ticker_list')
    [stock_info_yahoo(ticker_list),options_dat_yahoo(ticker_list)] >> combine_data() >> upload_data('1JPuCJUMEZfnsokn3WO_uLKvk4HrhYgLtwf6XLOeZGC4','All Data')

options_pipe()