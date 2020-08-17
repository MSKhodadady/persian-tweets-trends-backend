import typing as t
from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum

import i_database as db
from i_main_handler import MainHandler

Frequency = t.Dict[str, int]
Frequencies = t.List[Frequency]

#: chart types
class ChartType(Enum):
    FREQUENCY = 'frequency'
    MA = 'MA'
    TREND_MOMENTUM = 'trend-momentum'
    MOMENTUM = 'momentum'

#: time units
class TimeUnit(Enum):
    HOUR = 'hour'
    DAY = 'day'


class ChartHandler(MainHandler):

    #pylint: disable=too-many-locals
    async def post(self):
        try:
            token = self.get_json_arg('token')
            since = self.get_json_arg('since', datetime.now(), datetime.fromisoformat)
            until = self.get_json_arg('until', datetime.now() - timedelta(days=10), datetime.fromisoformat)
        except Exception as e:
            self.send_error(400, message = str(e))

        try:
            chart_type = self.get_json_arg('chart-type', ChartType.FREQUENCY, ChartType)
            time_unit = self.get_json_arg('time-unit', TimeUnit.DAY, TimeUnit)
        except ValueError as e:
            self.send_error(400, message = str(e))

        usernames: list = self.get_json_arg('usernames', None)

        def row_json(x: dict):
            return {
                **x,
                "date": x["date"].isoformat()
            }

        if chart_type == ChartType.FREQUENCY:
            output: list = await get_frequencies(
                token, time_unit, since, until, usernames, is_zero_filled=False
            )

        if chart_type == ChartType.MA:
            try:
                k_param = self.get_json_arg('k-param', arg_changer_callback=int)
            except Exception as e:
                self.send_error(400, message = str(e))

            output = await get_MA(
                await get_frequencies(token, time_unit, since, until, usernames),
                k_param
            )

        if chart_type == ChartType.TREND_MOMENTUM:
            try:
                k_s = self.get_json_arg('k-s', arg_changer_callback=int)
                k_l = self.get_json_arg('k-l', arg_changer_callback=int)
                alpha = self.get_json_arg('alpha', arg_changer_callback=float)
            except Exception as e:
                self.send_error(400, message = str(e))

            output = await get_trend_momentum(
                await get_frequencies(token, time_unit, since, until, usernames),
                k_s, k_l,
                alpha
            )

        if chart_type == ChartType.MOMENTUM:
            try:
                k_s = self.get_json_arg('k-s', arg_changer_callback=int)
                k_l = self.get_json_arg('k-l', arg_changer_callback=int)
                alpha = self.get_json_arg('alpha', arg_changer_callback=float)
                k_param = self.get_json_arg('k-param', arg_changer_callback=int)
            except Exception as e:
                self.send_error(400, message = str(e))

            output = await get_MA(
                await get_trend_momentum(
                    await get_frequencies(
                        token, time_unit, since, until, usernames
                    ), k_s, k_l, alpha
                ), k_param
            )

        self.write({
            "data": list(map(
                row_json, output
            ))
        })

class ChartSearchHandler(MainHandler):

    async def get(self):

        token = self.get_json_arg('token')

        conn: db.DBConnection
        async with await db.get_db_context() as conn:
            async with conn.transaction():

                rows: db.DBRows = await conn.fetch(
                    f'''
                    select distinct(tt."token") from tweet_token tt where tt."token" like '%{token}%';
                    '''
                )

                self.write({
                    "data": list(
                        map(lambda x: x.get('token'), rows)
                    )
                })


#pylint: disable=too-many-arguments
async def get_frequencies(
    token,
    time_unit: TimeUnit,
    since: datetime,
    until: datetime,
    usernames: list,
    is_zero_filled: bool = True
) -> Frequencies:
    usernames_caluse = ''
    if usernames:
        _usernames = ' OR '.join(
            list(map(
                lambda u: f"t2.username = '{u}'",
                usernames
            ))
        )

        usernames_caluse = f' AND ({_usernames})'

    conn: db.DBConnection
    async with await db.get_db_context() as conn:
        async with conn.transaction():

            query = f'''
            select
                date_trunc($1, t2.tweet_time) as "date", -- minute, hour, day, week, month, year
                count(1)
            from tweet t2
            join tweet_token tt
                on t2.tweet_id  = tt.tweet_id
            where
                tt."token" = $2
                and t2.tweet_time >= $3 and t2.tweet_time <= $4 {usernames_caluse}
            group by 1;
            '''

            rows: db.DBRows = await conn.fetch(query, time_unit.value, token, since, until)

            frequencies = list(map(
                #: removing timezone from 'datetime's
                lambda d: { **d, "date": d["date"].replace(tzinfo=None) },
                map(
                    #: conevert to dict
                    dict,
                    rows
                )
            ))

            sorted_f = sorted(frequencies, key=lambda s: s["date"])

            if is_zero_filled:
                return zero_filled_frequencies(sorted_f, since, until, time_unit)
            
            return sorted_f

def zero_filled_frequencies(frequencies, since, until, time_unit) -> Frequencies:
    """
    the 'frequencies' has not the times with zero 'count' value.
    because of that, we must fill the the list with zero 'count' values
    """
    acc = 0
    zero_filled: Frequencies = []
    for d in daterange(since, until, time_unit):
        if acc < len(frequencies) and (k:= frequencies[acc])['date'] == d:
            zero_filled.append({
                "date": d,
                "count": k['count']
            })

            acc += 1
        else:
            zero_filled.append({
                "date": d,
                "count": 0
            })

    return zero_filled


def daterange(since, until, time_unit) -> Frequencies:
    """
    This function generates a range of 'datetime's, between two 'datetime'.
    """
    # #: find distance between two time
    # time_distance: timedelta = until - since

    # #: finding the count of 'time_unit's between two time
    # interval_count = 0
    # if time_unit == TimeUnit.HOUR:
    #     interval_count = int(time_distance.seconds / (60 * 60))

    # if time_unit == TimeUnit.DAY:
    #     interval_count = int(time_distance.days)

    # #: yield every 'datetime' one by one
    # for n in range(interval_count):
    #     if time_unit == TimeUnit.DAY:
    #         yield since + timedelta(days=n)

    #     if time_unit == TimeUnit.HOUR:
    #         yield since + timedelta(hours=n)

    loop_iterator = deepcopy(since)
    
    if time_unit == TimeUnit.DAY:
        loop_iterator = loop_iterator.replace(hour=0, minute=0, second=0, microsecond=0)

        while True:
            yield loop_iterator
            loop_iterator += timedelta(days=1)

            if loop_iterator > until:
                break
    elif time_unit == TimeUnit.HOUR:
        loop_iterator = loop_iterator.replace(minute=0, second=0, microsecond=0)

        while True:
            yield loop_iterator
            loop_iterator += timedelta(hours=1)
            
            if loop_iterator > until:
                break



async def get_MA(
    frequencies,
    k_param: int
) -> Frequencies:
    """
    This function calculates MA time series
    """

    #: calculate MA
    ma_series = []
    for n, v in enumerate(frequencies):
        if n < (k_param - 1):
            sigma = sum(
                frequencies[i]['count'] for i in range(0, n+1)
            )
            ma_series.append({
                "date": v['date'],
                "count": sigma / n if sigma != 0 else 0
            })
        else:
            sigma = sum(
                frequencies[i]['count'] for i in range(n - k_param + 2, n)
            )
            ma_series.append({
                "date": v['date'],
                "count": sigma / k_param if sigma != 0 else 0
            })

    return ma_series


async def get_trend_momentum(
    frequencies,
    k_s: int,
    k_l: int,
    alpha
) -> Frequencies:
    trend_momentum = []
    for n, v in enumerate(frequencies):
        ma_s = 0
        ma_l = 0

        if n < (k_s - 1):
            sigma = sum(
                frequencies[i]['count'] for i in range(0, n+1)
            )
            ma_s = sigma / n if sigma != 0 else 0
        else:
            sigma = sum(
                frequencies[i]['count'] for i in range(n - k_s + 2, n)
            )
            ma_s = sigma / k_s if sigma != 0 else 0

        if n < (k_l - 1):
            sigma = sum(
                frequencies[i]['count'] for i in range(0, n+1)
            )
            ma_l = sigma / n if sigma != 0 else 0
        else:
            sigma = sum(
                frequencies[i]['count'] for i in range(n - k_l + 2, n)
            )
            ma_l =  sigma / k_l if sigma != 0 else 0

        trend_momentum.append({
            'date': v['date'],
            'count': ma_s - (ma_l ** alpha)
        })

    return trend_momentum
