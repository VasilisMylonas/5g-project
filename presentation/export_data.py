"""Export the raw-data series the slides plot with pgfplots (standard library only).

Run from the repository root:  python presentation/export_data.py
Writes into presentation/figs/:
  internet_week.dat   mean Internet week of the train set, hourly (title slide)
  series_hourly.dat   hourly mean of the three series over the whole period, plus a day-off flag
  acf_y.dat           ACF of Internet (train) up to 3 days
  acf_d.dat           ACF of Internet minus yesterday's value (train) up to 7 days
  pacf_d.dat          PACF of each series minus yesterday's value (train), lags 0-36, plus the 95% band
"""
import csv
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

SRC = Path("data/grid5060_rows.tsv")
OUT = Path("presentation/figs")
CET = timezone(timedelta(hours=1))           # Nov-Jan: no daylight saving, Milan is UTC+1
START = datetime(2013, 11, 1, tzinfo=CET)
N, N_TRAIN, SEASON = 8928, 6624, 144
HOLIDAYS = {date(2013, 11, 1), date(2013, 12, 7), date(2013, 12, 8),
            date(2013, 12, 25), date(2013, 12, 26), date(2014, 1, 1)}

# ---- three series at 10-minute resolution, summed over country codes ----
series = {k: [0.0] * N for k in ("internet", "sms", "calls")}
with SRC.open(newline="") as f:
    for row in csv.reader(f, delimiter="\t"):
        i = (int(row[1]) // 1000 - int(START.timestamp())) // 600
        v = [float(x) if x else 0.0 for x in row[3:8]]   # sms_in, sms_out, call_in, call_out, internet
        series["sms"][i] += v[0] + v[1]
        series["calls"][i] += v[2] + v[3]
        series["internet"][i] += v[4]

def stamp(i):
    return START + timedelta(minutes=10 * i)

def off_day(d):
    return d.weekday() >= 5 or d in HOLIDAYS

# ---- hourly means over the whole period ----
with (OUT / "series_hourly.dat").open("w") as f:
    f.write("x internet sms calls off\n")
    for h in range(N // 6):
        d = stamp(6 * h).date()
        means = [sum(series[k][6 * h:6 * h + 6]) / 6 for k in ("internet", "sms", "calls")]
        f.write(f"{h / 24:.4f} {means[0]:.1f} {means[1]:.1f} {means[2]:.1f} {int(off_day(d))}\n")

# ---- mean Internet week on the train set, holidays left out ----
acc = defaultdict(list)
for i in range(N_TRAIN):
    t = stamp(i)
    if t.date() not in HOLIDAYS:
        acc[(t.weekday(), t.hour)].append(series["internet"][i])
with (OUT / "internet_week.dat").open("w") as f:
    f.write("x y\n")
    for wd in range(7):
        for h in range(24):
            vals = acc[(wd, h)]
            f.write(f"{wd + h / 24:.4f} {sum(vals) / len(vals):.1f}\n")

# ---- ACF of Internet on the train set ----
def acf(x, nlags):
    n, m = len(x), sum(x) / len(x)
    c = [v - m for v in x]
    c0 = sum(v * v for v in c)
    return [sum(c[t] * c[t + k] for t in range(n - k)) / c0 for k in range(nlags + 1)]

y = series["internet"][:N_TRAIN]
d = [y[t] - y[t - SEASON] for t in range(SEASON, N_TRAIN)]
for name, x, lags in [("acf_y", y, 3 * SEASON), ("acf_d", d, 7 * SEASON)]:
    with (OUT / f"{name}.dat").open("w") as f:
        f.write("lag acf\n")
        for k, r in enumerate(acf(x, lags)):
            f.write(f"{k / SEASON:.4f} {r:.4f}\n")
# ---- PACF of y[t] - y[t-144] on the train set, all three series (Durbin-Levinson = Yule-Walker, as "ywm") ----
def pacf(x, nlags):
    r = acf(x, nlags)
    out, phi = [1.0], []
    for k in range(1, nlags + 1):
        num = r[k] - sum(phi[j] * r[k - 1 - j] for j in range(k - 1))
        den = 1 - sum(phi[j] * r[j + 1] for j in range(k - 1))
        a = num / den
        phi = [phi[j] - a * phi[k - 2 - j] for j in range(k - 1)] + [a]
        out.append(a)
    return out

PL = 36
cols = {}
for k in ("internet", "sms", "calls"):
    yk = series[k][:N_TRAIN]
    cols[k] = pacf([yk[t] - yk[t - SEASON] for t in range(SEASON, N_TRAIN)], PL)
band = 1.96 / (N_TRAIN - SEASON) ** 0.5
with (OUT / "pacf_d.dat").open("w") as f:
    f.write("lag internet sms calls band\n")
    for k in range(PL + 1):
        f.write(f"{k} {cols['internet'][k]:.4f} {cols['sms'][k]:.4f} {cols['calls'][k]:.4f} {band:.4f}\n")

print("wrote", sorted(p.name for p in OUT.glob("*.dat")))
