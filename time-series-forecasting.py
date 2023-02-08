import pandas as pd
from prophet import Prophet

df = pd.read_csv('youtube-train.csv')
df.head()
print(df.head())

m = Prophet()
m.fit(df)

future = pd.read_csv('youtube-test.csv')
print(future.tail())

forecast = m.predict(future)
#print(type(forecast))
prediction=forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
print(prediction)
prediction.to_csv("prediction_result.csv",sep=",")
#print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
