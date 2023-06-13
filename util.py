import datetime
import time

time_String = "2022-09-02 11:30:00"


def get_kpi_name(kpi):
    kpi_dict = {
        0: "call_count",
        1: "system_fail",
        2: "logic_fail",
        3: "average_latency",
        4: "final_fail",
    }
    return kpi_dict[kpi]


def get_timestamp(time_String):
    '''
    convert time_String to timestamp
    :input
    - time_String: e.g., "2022-09-02 11:30:00"

    :output
    - timestamp for the corespoding time_String, e.g., 1662089400

    '''
    dt_format = datetime.datetime.strptime(time_String, '%Y-%m-%d %H:%M:%S')
    timestamp = int(dt_format.timestamp())

    # print(timestamp)

    return timestamp


def cal_timestamp(tiemstamp, time_change):
    tiemstamp = tiemstamp + time_change
    time_local = time.localtime(tiemstamp)
    dt_format = time.strftime("%Y-%m-%d %H:%M:%S", time_local)

    return dt_format


def cal_time(time_String, time_change):
    dt_format = datetime.datetime.strptime(time_String, '%Y-%m-%d %H:%M:%S')
    dt_format = (dt_format + datetime.timedelta(seconds=time_change)
                 ).strftime("%Y-%m-%d %H:%M:%S")

    # print(dt_format)

    return dt_format


def same_minute(timestamp1, timestamp2):
    dt1 = datetime.datetime.fromtimestamp(timestamp1)
    dt2 = datetime.datetime.fromtimestamp(timestamp2)
    return dt1.minute == dt2.minute


def cal_intervel(time_String1, time_String2):
    date1 = datetime.datetime.strptime(time_String1, '%Y-%m-%d %H:%M:%S')
    date2 = datetime.datetime.strptime(time_String2, '%Y-%m-%d %H:%M:%S')
    delta = date1 - date2

    minutes = delta.seconds // 60 + delta.days * 24 * 60

    print(minutes)


if __name__ == '__main__':

    # timestamp1 = 1629801000  # 2021-08-24 14:30:00
    # timestamp2 = 1629801060  # 2021-08-24 14:31:00
    # timestamp3 = 1629801080  # 2021-08-24 14:31:00
    # print(same_minute(timestamp1, timestamp2))  # True
    # print(same_minute(timestamp2, timestamp3))  # False

    cal_intervel("2022-10-31 20:19:00", "2022-11-01 10:39:00")
