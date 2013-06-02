"""
an ADVFN forum post popularity indicate stock movement?
--------------------------------------------------------

* Construct dataframe with time, no. of posts for each thread
  - construct frame column by column - get unique scrape times - for each scrape_time, get


scrape_time     thread1 thread2
dt              count   count

* Calculate x - (x-1)
* What are the stocks which have the highest change in P. delta P / delta T
"""
from datetime import datetime
import pickle
import hashlib
import os

import pandas
import pymongo

conn = pymongo.Connection()
db = conn['advfn']
coll = db['forum_scrape']
coll2 = db['forum_scrape2']

date1 = datetime(2013, 5, 1)
date2 = datetime(2013, 5, 8)


def make_series(name, column):

    data = column['data']
    s = pandas.Series()
    for date, count in data:
        s = s.set_value(date, int(count))

    s.index.name = 'scrape_time'
    s.name = name
    return s


def get_series(n=3000):
    columns = coll2.distinct('_id')
    series = []
    ticker_map = {}
    for i, c in enumerate(columns):
        print(i)
        if i < n:
            column = coll2.find_one({'_id': c})
            ticker = column['epic']
            ticker_map[c] = ticker
            series.append(make_series(c, column))
        else:
            break

    return series, ticker_map


def slice_date(df):
    return df.ix[date1:date2]


def construct_dataframe():
    sl, map = get_series()
    sld = dict([(s.name, s) for s in sl])
    df = pandas.DataFrame(sld)
    hash_name = str(df).encode('utf-8')
    df.save(hashlib.md5(hash_name).hexdigest())
    return df


def filter_df(df):
    df = df.diff()
    # order by biggest diff 
    indexes = df.max().order(ascending=False).index

    new = {}
    # get 5 biggest 
    for i, index  in enumerate(indexes):
        if i < 5:
            new[index] = df[index]
        else:
            break

    return pandas.DataFrame(new)


def save_columns():
    columns = coll.distinct('title')
    pkl(columns, COLUMNS_FILE)


def get_columns():
    with open(COLUMNS_FILE, 'rb') as f:
        columns = pickle.load(f)
    return columns


def re_save_mongo():
    # {'title': [(dt, count)]}
    titles = coll.distinct('title')

    for t in titles:
        entries = coll.find({'title': t})
        for e in entries:
            data = (e['scrape_time'], int(e['status1'])) 
            coll2.update({'_id': t, 'epic': e['epic']},
                         {'$push': {'data': data}}, upsert=True)

            #print(data)
            #input()


def save_cols_as_series(cols):
    for c in cols:
        series = make_series(c)
        pkl_series(series)


def title_ticker_map():
    cols = get_columns()
    title_ticker = {}
    for c in cols:
        epic = coll.find_one({'title': c}, {'epic': 1})['epic']
        print(epic)
        title_ticker[c] = epic

    pkl(title_ticker, 'title_ticker.pkl')
    return title_ticker


def get_ts():
    fdir = TIMESERIES_DIR
    ls = os.listdir(fdir)
    for f in ls:
        fpath = fdir + '/' + f
        if os.path.isfile(fpath):
            with open(fpath, 'rb') as f:
                yield pickle.load(f)


def transform_ts():
    for t in get_ts():
        t_new = t.apply(int)
        pkl_series(t_new)


def get_df():
    d = {}
    for ts in get_ts():
        d[ts.name] = ts

    return pandas.DataFrame(d)


def get_one():
    return coll2.find_one()


def run():
    df = plot(
        filter_df(
            slice_date(
                construct_dataframe()
            )
        )
    )
    return df


import io
from matplotlib import pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib import dates as dates

def plot(df):
    #fig = d3py.PandasFigure(df, name="basic_example", width=300, height=300) 
    ## add some red points
    #fig += d3py.geoms.Point(x="pressure", y="temp", fill="red")
    ## writes 3 files, starts up a server, then draws some beautiful points in Chrome
    #fig.show() 
    #fig = pyplot.figure()
    #fig.savefig('test.png')

    #ax = fig.add_subplot(111)
    #df.plot(ax=ax)
    #canvas = FigureCanvas(fig)
    #output = io.StringIO()
    #canvas.print_png(output)
    #fh=open('check.png','wb')
    #fh.write(output.getvalue())

    
    #idx = pandas.date_range('2011-05-01', '2011-07-01')

    #fig, ax = plt.subplots()
    #ax.plot_date(df, 'v-')

    #ax.xaxis.set_major_locator(dates.WeekdayLocator(byweekday=(1),
    #                                                interval=1))
    #ax.xaxis.set_major_formatter(dates.DateFormatter('%d\n%a'))
    #ax.xaxis.grid()
    #ax.yaxis.grid()
    #plt.tight_layout()

    fig = plt.figure(figsize=(10, 10))
    ax1 = fig.add_subplot(131, yticklabels=df.columns, xticklabels=df.index)
    ax1.tick_params(axis='both', direction='out')
    #im1 = ax1.imshow(df, 
    #                 interpolation='nearest', 
    #                 aspect='auto',
    #                 )
    #                 #cmap=cmap )
    #ax1.plot(

    plot = df.plot(figsize=(10,10))
    #, xticks=[1,23,4,5,6,7])
    plt.savefig('test.svg')

    #plt.show()
    return df

def plot2():
    df = pandas.DataFrame.load('0a0e864ab0b4c9e4d9c7617b28525bc6')
    idx = pandas.date_range('2012-05-01', '2012-05-08')
    s = df[df.columns[0]]

    fig, ax = plt.subplots()
    ax.plot_date(idx.to_pydatetime(), s, 'v-')

    ax.xaxis.set_minor_locator(dates.WeekdayLocator(byweekday=(1),
                                                    interval=1))
    ax.xaxis.set_minor_formatter(dates.DateFormatter('%d\n%a'))
    ax.xaxis.grid(True, which="minor")
    ax.yaxis.grid()
    ax.xaxis.set_major_locator(dates.MonthLocator())
    ax.xaxis.set_major_formatter(dates.DateFormatter('\n\n\n%b\n%Y'))
    plt.tight_layout()
    #plt.show()
    plt.savefig('test.svg')
