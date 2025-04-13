import pandas as pd
import numpy as np
import pathlib
import json
import datetime
import configparser as cp
import os
import ast

percentil = 99

time_series = pd.read_csv("data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert.csv")

uc_list = ["public", "semi-public", "other_private", "home", "business", "agrar/bau"]

times_business_day = []
times_business_night = []

weekdays = list(range(0, 7))
timeframe_business_night = [0,1,2,3,4,5,6,7,21,22,23]
timeframe_business_day = [8,9,10,11,12,13,14,15,16,17,18,19,20]
for i,day in enumerate(weekdays):

    addon_day = [x + i*24 for x in timeframe_business_day]
    addon_night = [x + i*24 for x in timeframe_business_night]

    times_business_day = times_business_day + addon_day
    times_business_night = times_business_night + addon_night

occupation = {}

for uc in uc_list:
    #time_series[uc].sum(axis=1)
    occupation[uc] = int(np.percentile(time_series[uc], percentil))
    print(uc, occupation[uc])

    if uc == "business":
        occ_business_day = time_series["business"].loc[time_series.index.isin(times_business_day)]
        occ_business_night = time_series["business"].loc[time_series.index.isin(times_business_night)]

        occupation[uc + "_day"] = int(np.percentile(occ_business_day, percentil))
        occupation[uc + "_night"] = int(np.percentile(occ_business_night, percentil))

        print(uc + "_day", occupation[uc + "_day"])
        print(uc + "_night", occupation[uc + "_night"])