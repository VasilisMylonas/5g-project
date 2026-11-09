"""Export the model forecasts the slides plot with pgfplots.

Run as the last cell of notebook.ipynb, after everything else has run:
    %run -i presentation/slide_figures.py
It reads the notebook's variables (df, Y, PRED, MAIN, SERIES) and writes
presentation/figs/forecast_holidays.dat: the holiday period 24/12 - 01/01, long-term (H = 5)
forecasts of the model that represents each family, for Internet and SMS.
Columns: x = days since 24/12 00:00, then actual / ARIMA / LSTM / TimesFM per series.
"""
import numpy as np
import pandas as pd

t0, t1 = pd.Timestamp("2013-12-24"), pd.Timestamp("2014-01-02")
sel = (df.index >= t0) & (df.index < t1)                       # noqa: F821  (notebook variables)
cols = {"x": (df.index[sel] - t0) / pd.Timedelta("1D")}        # noqa: F821
for s, tag in [("internet", "inet"), ("sms", "sms")]:
    cols[f"{tag}_actual"] = Y[s][sel]                           # noqa: F821
    for fam, key in [("ARIMA", "arima"), ("LSTM", "lstm"), ("TimesFM", "tfm")]:
        idx, yhat = PRED[(s, 5)][MAIN[fam]]                     # noqa: F821
        full = np.full(len(df), np.nan)                         # noqa: F821
        full[idx] = yhat
        cols[f"{tag}_{key}"] = full[sel]
out = pd.DataFrame(cols).round(2)
out.to_csv("presentation/figs/forecast_holidays.dat", sep=" ", index=False, na_rep="nan")
print("wrote presentation/figs/forecast_holidays.dat,", len(out), "rows; models:", MAIN)  # noqa: F821

# ---- 95% intervals, short-term (h = 1), Internet, two working days 18/12 - 19/12 ----
# Writes presentation/figs/intervals.dat. Columns: x = days since 18/12 00:00, actual, then
# forecast / lower / upper for ARIMA-x and for TimesFM fine-tuned (the notebook's PI variable).
t0, t1 = pd.Timestamp("2013-12-18"), pd.Timestamp("2013-12-20")
tt = df.index[idx1]                                                     # noqa: F821
sel = (tt >= t0) & (tt < t1)
cols = {"x": (tt[sel] - t0) / pd.Timedelta("1D"), "actual": Y["internet"][idx1][sel]}   # noqa: F821
for m, key in [("ARIMA-x", "arima"), ("TimesFM fine-tuned", "tfm")]:
    lo, hi = PI[("internet", m)]                                        # noqa: F821
    cols[key] = PRED[("internet", 1)][m][1][sel]                        # noqa: F821
    cols[f"{key}_lo"], cols[f"{key}_hi"] = lo[sel], hi[sel]
pd.DataFrame(cols).round(2).to_csv("presentation/figs/intervals.dat", sep=" ", index=False)
print("wrote presentation/figs/intervals.dat")
