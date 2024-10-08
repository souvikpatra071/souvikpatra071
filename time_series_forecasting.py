# -*- coding: utf-8 -*-
"""time_series_forecasting.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1M0oeoGe9cLhzzaJWE3_HbSUEvsq71CeQ
"""

#pip install pandas
#pip install prophet

import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt

df = pd.read_csv('/Users/souvikpatra/Downloads/Ad_test_spotify_manual.csv - Sheet1.csv')

#m = Prophet(interval_width=1,growth='linear',daily_seasonality=False,weekly_seasonality=False,yearly_seasonality=False).add_seasonality(name='monthly',period=30.5,fourier_order=5)
m=Prophet(interval_width=1,growth='linear',seasonality_mode='multiplicative',periods=5)
#plt.plot(df.ds,df.y)

m.fit(df)

future = pd.read_csv('/mnt/youtube-test.csv')

forecast = m.predict(future)
prediction=forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
plt.plot(prediction.ds,prediction.yhat)
print(prediction)