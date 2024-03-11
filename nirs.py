from datetime import timedelta
import numpy as np
import pandas as pd


def _read_raw_nirs_file(filename):
    """load single nirs csv data file into pandas dataframe"""
    df = pd.read_csv(
        filename,
        sep=",",
        skiprows=5,
        na_values=["--", " "],
        parse_dates=["Time"],
        date_format="%H:%M:%S",
    )

    df.rename(columns={"rSO2 (%)": "rSO2"}, inplace=True)
    df.rename(columns={"Poor Signal Quality": "Bad_rSO2_auto"}, inplace=True)
    if df.rSO2.mean() < 10:
        print(filename)
        print(df)
    with open(filename, "r") as fd:
        next(fd)
        startdate = next(fd).partition(",")[2].strip()
        for _ in range(4):
            next(fd)
        startdate = startdate + "T" + next(fd).partition(",")[0].strip()

    timeindex = []
    time = pd.to_datetime(startdate)  # , format='%m-%d-%Y')
    for _ in range(len(df.index)):
        timeindex.append(time)
        time = time + timedelta(seconds=1)

    df["Time"] = timeindex
    df.set_index("Time", inplace=True)

    return df


def load_nirs_amend_csv(filename):
    """load manual nirs corrections from 'amend' csv file to dataframe"""
    return pd.read_csv(
        filename,
        sep=";",
        na_values=["--", " "],
        parse_dates=["Time"],
        date_format="%d%m%Y",
    )


def load_raw_nirs(filenames):
    """load raw nirs data, combining multipart if needed"""
    filenames = sorted(filenames)
    d = _read_raw_nirs_file(filenames[0])
    for f in filenames[1:]:
        to_concat = [d, _read_raw_nirs_file(f)]
        d = pd.concat(to_concat).sort_index()
    return d


DISCARD_COL = "HuonoSignaali2"


def process_nirs(raw, amend):
    if len(raw.index) != len(amend.index):
        print("Error: data length mismatch")
        msg = f"raw: {len(raw.index)}, "
        print(msg + f"amend: {len(amend.index)}")
        exit(1)
    df = pd.DataFrame()
    df["rSO2"] = raw["rSO2"]
    df["Mark"] = amend["Mark"].array
    df["Bad_rSO2_auto"] = raw["Bad_rSO2_auto"]
    df["Bad_rSO2_manual"] = amend[DISCARD_COL].array
    df["rSO2"] = np.where(df["Bad_rSO2_manual"] != 0, np.nan, df["rSO2"])
    return df


def resolve_timestamps(df):
    try:
        t_mark = df.loc[df["Mark"] == 1].index[0]
    except IndexError:
        t_mark = None
    t_start = df["rSO2"].first_valid_index()
    t_end = df.index[-1]
    return t_start, t_mark, t_end


def load_nirs(raw_files, amend_files):
    raw = load_raw_nirs(raw_files)
    assert len(amend_files) == 1
    amend = load_nirs_amend_csv(amend_files[0])
    df = process_nirs(raw, amend)
    df["timestamp"] = pd.array(df.index).tz_localize(tz="Europe/Helsinki")
    df = df.set_index("timestamp", drop=False)
    t_start, t_mark, t_end = resolve_timestamps(df)
    return df, t_start, t_mark, t_end
