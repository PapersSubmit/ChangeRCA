from log import Logger
import pandas as pd
from pandas.core.frame import DataFrame
import statsmodels.formula.api as smf
import datetime
import os
import ujson
import numpy as np
import logging
import tqdm


import concurrent.futures

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

log_path = os.getcwd() + '/log/' + str(datetime.datetime.now().strftime(
    '%Y-%m-%d')) + '_changerca.log'
logger = Logger(log_path, logging.DEBUG, __name__).getlog()


def find_iqr(data):
    data.sort()
    q1 = data[int(len(data) * 0.25)]
    q3 = data[int(len(data) * 0.75)]
    iqr = q3 - q1
    return iqr, q1, q3


def detect_outliers(normal_data, online_data, k=1.5, abnormal_number=8):
    """
    time series anoamly detection method
    """
    iqr, q1, q3 = find_iqr(normal_data)
    lower_bound = q1 - k * iqr
    upper_bound = q3 + k * iqr
    # outliers = [x for x in online_data if x < lower_bound or x > upper_bound]
    outliers = [x for x in online_data if x > upper_bound]

    for item in outliers:
        if item < 100:
            outliers.remove(item)
    if len(outliers) < abnormal_number:
        return False
    else:
        return True


class Differentiator:
    def __init__(self,
                 alert_time,
                 alert_module,
                 p_threshold=0.7,
                 max_workers=5,
                 ):
        """
        func init
        :input
        - alert_time: alert time from idkey
        - alert_module: alert module from idkey
        - p_threshold: Threshold for pvaule to determine if it is siginificant different
        - max_workers: number of concurrently running processes
        """
        self.alert_time = alert_time
        self.alert_module = alert_module
        self.max_workers = max_workers
        self.day = alert_time.split(" ")[0]
        self.base_path = os.getcwd() + "/data/" + self.day + \
            "_" + self.alert_module + "/"

        self.file = self.base_path + self.day + "_" + self.alert_module + "_data.json"
        self.p_threshold = p_threshold

        with open(self.file, "r+") as f:
            self.data = ujson.load(f)

        self.complete_module = []

        for item in self.data:
            self.complete_module.append(item)

    def ddd(self,
            data,
            kpi,
            threshold):
        """
        func ddd: calculate ddd result and determine whether significant
        : input
        - data: contact data
        - kpi: kpi_name
        - threshold: threshold for p-values
        : ouput
        - significant result
        if significant return True, else return False
        """

        # kpi = a * change + b * time + c * history + d *  change * time +  e * time * history + f * change * history + g *  change * time * history +  call_count
        model_present = kpi + \
            ' ~ change + time + history + change:time + time:history + change:history + change:time:history + call_count'

        model = smf.ols(
            model_present, data=data).fit()
        # print(model.summary())

        # and model.params["change:time"] > -1:
        if model.pvalues['change:time:history'] < threshold:
            # logger.info(model.summary())
            # logger.info(data)
            # logger.info('P-values for the DDD method is significant')
            return True
        else:
            # logger.info('P-values for the DDD method is not significant')
            return False

    def did(self,
            data,
            kpi,
            threshold):
        """
        func did: calculate did result and determine whether significant
        :input
        - data: contact data
        - kpi: kpi_name
        - threshold: threshold for p-values
        :ouput
        - significant result
        if significant return True , else return False
        """

        # kpi = a * change + b * time + c *  change * time + call_count
        model_present = kpi + ' ~ change + time + change:time + call_count'
        model = smf.ols(
            model_present, data=data).fit()

        # and model.params["change:time"] > -1:
        if model.pvalues['change:time'] < threshold:
            # logger.info(model.summary())
            # logger.info(data)
            # logger.info('P-values for the DID method is significant')
            return True
        else:
            # logger.info('P-values for the DID method is not significant')
            return False

    def before_did(self,
                   data,
                   kpi,
                   threshold):
        """
        func did: calculate before_did result and determine whether significant
        :input
        - data: contact data
        - kpi: kpi_name
        - threshold: threshold for p-values
        :ouput
        - significant result
        if significant return True , else return False
        """

        # kpi = a * change + b * time + c *  change * time + call_count
        # model_present = kpi + ' ~ change + call_count'

        if kpi == "call_count":
            model_present = kpi + ' ~ change'
            model = smf.ols(
                model_present, data=data).fit()
            if model.pvalues['change'] < threshold:
                return True
            else:
                # logger.info('P-values for the DID method is not significant')
                return False
        else:
            model_present = kpi + ' ~ change +  call_count'

            model = smf.ols(
                model_present, data=data).fit()
            # logger.info(model.params)
            # logger.info(model.summary())

            # and model.params["change"] > -1:
            if model.pvalues['change'] < threshold:
                # logger.info(model.summary())
                # logger.info('P-values for the DID method is significant')
                # if model.params["change"] > 0:
                return True
            else:
                # logger.info('P-values for the DID method is not significant')
                return False

    def did_concat_input(self,
                         list1,
                         list2,
                         list3,
                         list4,
                         call_count,
                         kpi):
        """
        func did_concat_input: concat did method input
        :input
        - list1: change group before change
        - list2: change group before alert
        - list3: change group before change in diff day
        - list4: change group before alert in diff day
        - call_count: call count of the above list

        example of input
        list1 = [1,2,3,4]
        list2 = [4,5,6,7]

        list3 = [1,2,3,4]
        list4 = [4,5,6,7]

        call_count = [11,12,13,14,14,15,16,17,11,12,13,14,14,15,16,17]

        :output
        - DataFrame concat_input

        example
            final_fail  time  change  call_count
        0             1     0       0        11
        1             2     0       0        12
        2             3     0       0        13
        3             4     0       0        14
        4             4     1       0        14
        5             5     1       0        15
        6             6     1       0        16
        7             7     1       0        17
        8             1     0       1        11
        9             2     0       1        12
        10            3     0       1        13
        11            4     0       1        14
        12            4     1       1        14
        13            5     1       1        15
        14            6     1       1        16
        15            7     1       1        17
        """
        dict1 = {kpi: list1}
        data1 = DataFrame(dict1)
        dict2 = {kpi: list2}
        data2 = DataFrame(dict2)
        data1["time"] = [0] * len(data1)
        data2["time"] = [1] * len(data2)

        data_control = pd.concat([data1, data2], axis=0, ignore_index=True)
        data_control["change"] = [0] * len(data_control)

        dict3 = {kpi: list3}
        data3 = DataFrame(dict3)
        dict4 = {kpi: list4}
        data4 = DataFrame(dict4)
        data3["time"] = [0] * len(data3)
        data4["time"] = [1] * len(data4)
        data_exp = pd.concat([data3, data4], axis=0, ignore_index=True)
        data_exp["change"] = [1] * len(data_exp)

        data = pd.concat([data_control, data_exp], axis=0, ignore_index=True)

        data["call_count"] = call_count

        return data

    def before_did_concat_input(self,
                                a,
                                b,
                                call_count,
                                kpi):
        """
        func before_did_concat_input: concat did method input
        :input
        - a: data before alert
        - b: data before alert 
        - call_count: call count of the above list

        example of input
        a = [1,2,3,4,4,5,6,7]
        b = [1,2,3,4,4,5,6,7]

        call_count = [11,12,13,14,14,15,16,17,11,12,13,14,14,15,16,17]

        :output
        - DataFrame concat_input

        example
            final_fail  time  change  call_count
        0             1     0       0        11
        1             2     0       0        12
        2             3     0       0        13
        3             4     0       0        14
        4             4     1       0        14
        5             5     1       0        15
        6             6     1       0        16
        7             7     1       0        17
        8             1     0       1        11
        9             2     0       1        12
        10            3     0       1        13
        11            4     0       1        14
        12            4     1       1        14
        13            5     1       1        15
        14            6     1       1        16
        15            7     1       1        17
        """
        half = len(a) // 2
        list1 = a[:half]
        list2 = a[half + len(a) % 2:]

        half = len(b) // 2
        list3 = b[:half]
        list4 = b[half + len(b) % 2:]

        # half = len(a) // 2
        # list1 = a
        # list2 = a

        # half = len(b) // 2
        # list3 = a
        # list4 = b

        dict1 = {kpi: list1}
        data1 = DataFrame(dict1)
        dict2 = {kpi: list2}
        data2 = DataFrame(dict2)
        data1["time"] = [0] * len(data1)
        data2["time"] = [1] * len(data2)

        data_control = pd.concat([data1, data2], axis=0, ignore_index=True)
        data_control["change"] = [0] * len(data_control)

        dict3 = {kpi: list3}
        data3 = DataFrame(dict3)
        dict4 = {kpi: list4}
        data4 = DataFrame(dict4)
        data3["time"] = [0] * len(data3)
        data4["time"] = [1] * len(data4)
        data_exp = pd.concat([data3, data4], axis=0, ignore_index=True)
        data_exp["change"] = [1] * len(data_exp)

        data = pd.concat([data_control, data_exp], axis=0, ignore_index=True)

        data["call_count"] = call_count

        return data

    def ddd_concat_input(self,
                         list1,
                         list2,
                         list3,
                         list4,
                         list5,
                         list6,
                         call_count,
                         kpi):
        """
        func ddd_concat_input:
        :input
        - list1: change group before change
        - list2: change group before alert
        - list3: not change group before change
        - list4: not change group before alert
        - list5: change group before change in diff day
        - list6: change group before alert in diff day
        - call_count: call count of the above list

        input example:
        list1 = [1,2]
        list2 = [3,4]
        list3 = [1,2]
        list4 = [3,4]
        list1 = [5,6]
        list2 = [7,8]
        call_count = [1,2,3,4,1,2,3,4,5,6,7,8]

        :output
        - DataFrame concat_input
            logic_fail  time  change  history  call_count
        0            1     0       0        1          1
        1            2     0       0        1          2
        2            3     1       0        1          3
        3            4     1       0        1          4
        4            1     0       1        0          1
        5            2     0       1        0          2
        6            3     1       1        0          3
        7            4     1       1        0          4
        8            5     0       1        1          5
        9            6     0       1        1          6
        10           7     1       1        1          7
        11           8     1       1        1          8
        """
        dict1 = {kpi: list1}
        data1 = DataFrame(dict1)
        dict2 = {kpi: list2}
        data2 = DataFrame(dict2)
        data1["time"] = [0] * len(data1)
        data2["time"] = [1] * len(data2)

        data_control = pd.concat([data1, data2], axis=0, ignore_index=True)
        data_control["change"] = [0] * len(data_control)
        data_control["history"] = [1] * len(data_control)

        dict3 = {kpi: list3}
        data3 = DataFrame(dict3)
        dict4 = {kpi: list4}
        data4 = DataFrame(dict4)
        data3["time"] = [0] * len(data3)
        data4["time"] = [1] * len(data4)
        data_exp = pd.concat([data3, data4], axis=0, ignore_index=True)
        data_exp["change"] = [1] * len(data_exp)
        data_exp["history"] = [0] * len(data_exp)

        dict5 = {kpi: list5}
        data5 = DataFrame(dict5)
        dict6 = {kpi: list6}
        data6 = DataFrame(dict6)
        data5["time"] = [0] * len(data5)
        data6["time"] = [1] * len(data6)
        data_his = pd.concat([data5, data6], axis=0, ignore_index=True)
        data_his["change"] = [1] * len(data_his)
        data_his["history"] = [1] * len(data_his)

        data = pd.concat([data_control, data_exp, data_his],
                         axis=0, ignore_index=True)

        data["call_count"] = call_count

        # print(data)
        return data

    def get_all_difference_result(self,
                                  target="change"):
        """
        func get_all_difference_result: Polling all modules to get difference_result
        : input:
        target: target difference, option
          - target="change": determine post-change instance difference
          - target="old": determine pre-change instance difference
          - target="nochange": determine no-change instance difference
        :output
        - difference_result: dict of score {"modle": score}
        """

        difference_result = {}

        # logger.info(self.complete_module)

        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor1:
            futures1 = {executor1.submit(
                self.difference_method, module, target) for module in self.data}

            future1_list = concurrent.futures.as_completed(futures1)
            # if not verbose:
            #     future1_list = tqdm.tqdm(future1_list, total=len(self.data))

            for future1 in tqdm.tqdm(future1_list, total=len(list(self.data)), desc="Get difference result processing", unit="module"):
                # for future1 in future1_list:
                result = future1.result()
                # logger.info(result[0])
                # self.complete_module.remove(result[0])
                # logger.info(self.complete_module)
                if result[1] is not None:
                    difference_result[result[0]] = (
                        result[1], result[2], result[3], result[4], result[5], result[6])

        # for module in self.data:
        #     if self.data[module]["change_ticket"]:
        #         score = self.difference_method(
        #             module, self.data[module]["change_ticket"]["task_id"])
        #         difference_result[module] = score

        logger.info(
            "-------------------%s difference score------------------", target)
        logger.info(difference_result)

        return difference_result

    def get_ddd_data(self,
                     instance,
                     old_instance,
                     pair,
                     kpi):
        """
        func get_ddd_data: load ddd data from json
        :input
        - instance: post-change instance ip
        - old_instance: pre-change instance ip
        - pair: call pair, e.g., a_b is a call be
        - kpi: kpi name, e.g., fail number
        :output
        - ddd data of instance's pair's KPI in change ticket
        """
        list2 = instance["alert_metric"][pair][kpi]["points"]
        if pair not in instance["before_change_metric"] or kpi not in instance["before_change_metric"][pair]:
            list1 = [0]*len(instance["alert_metric"]
                            [pair][kpi]["points"])
        else:
            list1 = instance["before_change_metric"][pair][kpi]["points"]

        if pair not in old_instance["alert_metric"] or kpi not in old_instance["alert_metric"][pair]:
            list3 = [0]*len(instance["alert_metric"]
                            [pair][kpi]["points"])
        else:
            list3 = old_instance["alert_metric"][pair][kpi]["diff_points"]

        if pair not in old_instance["alert_metric"] or kpi not in old_instance["alert_metric"][pair]:
            list4 = [0]*len(instance["alert_metric"]
                            [pair][kpi]["points"])
        else:
            list4 = old_instance["alert_metric"][pair][kpi]["points"]

        if pair not in instance["before_change_metric"] or kpi not in instance["before_change_metric"][pair]:
            list5 = [0]*len(instance["alert_metric"]
                            [pair][kpi]["points"])
        else:
            list5 = instance["before_change_metric"][pair][kpi]["diff_points"]

        if pair not in instance["alert_metric"] or kpi not in instance["alert_metric"][pair]:
            list6 = [0]*len(instance["alert_metric"]
                            [pair][kpi]["points"])
        else:
            list6 = instance["alert_metric"][pair][kpi]["diff_points"]

        return list1, list2, list3, list4, list5, list6

    def get_did_data(self,
                     instance,
                     pair,
                     kpi):
        """
        func get_ddd_data: load ddd data from json
        :input
        - old_instance: pre-change instance ip
        - pair: call pair, e.g., a_b is a call be
        - kpi: kpi name, e.g., fail number
        :output
        - did data of instance's pair's KPI in change ticket
        """
        list2 = instance["alert_metric"][pair][kpi]["points"]
        if pair not in instance["before_change_metric"] or kpi not in instance["before_change_metric"][pair]:
            list1 = [0]*len(instance["alert_metric"]
                            [pair][kpi]["points"])
        else:
            list1 = instance["before_change_metric"][pair][kpi]["points"]

        if pair not in instance["before_change_metric"] or kpi not in instance["before_change_metric"][pair]:
            list3 = [0]*len(instance["alert_metric"]
                            [pair][kpi]["points"])
        else:
            list3 = instance["before_change_metric"][pair][kpi]["diff_points"]

        if pair not in instance["alert_metric"] or kpi not in instance["alert_metric"][pair]:
            list4 = [0]*len(instance["alert_metric"]
                            [pair][kpi]["points"])
        else:
            list4 = instance["alert_metric"][pair][kpi]["diff_points"]

        return list1, list2, list3, list4

    def get_old_instance_data(self,
                              old_instance,
                              pair,
                              kpi):
        """
        func get_old_instance_data: load ddd data from json
        :input
        - old_instance: pre-change instance ip
        - pair: call pair, e.g., a_b is a call be
        - kpi: kpi name, e.g., fail number
        :output
        - old instance data of  old_instance in change ticket
        """
        list2 = old_instance["alert_metric"][pair][kpi]["points"]

        if pair not in old_instance["alert_metric"] or kpi not in old_instance["alert_metric"][pair]:
            return None, None
        else:
            list1 = old_instance["alert_metric"][pair][kpi]["diff_points"]

            return list1, list2

    def get_call_count(self,
                       list1,
                       list2,
                       list3=None,
                       list4=None,
                       list5=None,
                       list6=None):
        """
        func get_call_count: load call count data from json
        :input
        - list1: call count of change group before change
        - list2: call count of change group before alert
        - list3: call count of not change group before change
        - list4: call count of not change group before alert
        - list5: call count of change group before change in diff day
        - list6: call count of change group before alert in diff day
        :output
        - call count of  instance in change ticket
        """
        if list5 is None:
            call_count_list = list1 + list2 + list3 + list4
            call_count_dict = {
                "call_count": call_count_list}
            call_count = DataFrame(call_count_dict)
        else:
            call_count_list = list1 + list2 + list3 + list4 + list5 + list6
            call_count_dict = {
                "call_count": call_count_list}
            call_count = DataFrame(call_count_dict)

        before_call_count_list = list1 + list3  # list1 + list1 + list3
        before_call_count_dict = {
            "call_count": before_call_count_list}
        before_call_count = DataFrame(
            before_call_count_dict)

        return before_call_count, call_count

    def get_similar_instance(self,
                             change_data,
                             instance,
                             pair,
                             kpi="call_count"):
        """
        get the instance with the most similar kpi 
        :input
        - change_data: all instance data of a module
        - instance: target instance
        - pair: call pair
        - kpi: target similar kpi name 
        :return
        instance ip with the most similar kpi 
        """
        old_instance_candidate = []

        old_instance_candidate = change_data["change_ticket"]["host_list_old"]
        old_instance_result = None

        if len(old_instance_candidate) > 0:
            # print(instance["alert_metric"], pair, kpi)
            a = instance["alert_metric"][pair][kpi]["points"]
            # average_count = np.mean(
            #     instance["alert_metric"][pair][kpi]["points"])
            min_difference = 1000000000000000000000
            for old_instance in old_instance_candidate:
                if pair in old_instance["alert_metric"]:
                    b = old_instance["alert_metric"][pair][kpi]["points"]
                    # old_average_count = np.mean(
                    #     old_instance["alert_metric"][pair][kpi]["points"])

                    if np.sum(np.abs(np.array(a) - np.array(b))) < min_difference:
                        min_difference = np.sum(
                            np.abs(np.array(a) - np.array(b)))
                        old_instance_result = old_instance
                    # if np.abs(average_count - old_average_count) <= min_difference:
                    #     min_difference = np.abs(
                    #         average_count - old_average_count)
                    #     old_instance_result = old_instance
        return old_instance_result

    def determine_change_complete_anomaly(self,
                                          instance,
                                          min_call_count,
                                          min_fail_count):
        """
        func determine_change_complete_anomaly: determine if an instance of a service that has completed a change is abnormal
        :input
        - instance: instance ip
        - min_call_count: if the call count less min_call_count we ignore this pair
        - min_fail_count: if the fail count less min_fail_count we ignore this pair
        :return
        - if abnormal return instance ip
        - else return None
        """
        if "alert_metric" in instance and instance["alert_metric"]:
            for pair in instance["alert_metric"]:
                if "call_count" in instance["alert_metric"][pair]:
                    for kpi in instance["alert_metric"][pair]:
                        list1, list2, list3, list4 = self.get_did_data(
                            instance, pair, kpi)
                        if kpi == "call_count":
                            if max(list1 + list2) < min_call_count:
                                # logger.debug(
                                #     "skip because %s %s call_count less than 100 ", instance["ip_addr"], pair)
                                break
                            before_call_count, call_count = self.get_call_count(
                                list1, list2, list3, list4)

                        else:
                            if max(list1 + list2) > min_fail_count:
                                # logger.info(
                                #     "skip because %s %s %s less than %s ", instance["ip_addr"], pair,kpi,min_fail_count)

                                # logger.info("%s, %s, %s",
                                #             instance["ip_addr"], pair, kpi)
                                if detect_outliers(normal_data=list1, online_data=list2, abnormal_number=3):
                                    # logger.info("%s, %s, %s",
                                    #             instance["ip_addr"], pair, kpi)
                                    data = self.did_concat_input(list1,
                                                                 list2, list3, list4, call_count, kpi)

                                    before_data = self.before_did_concat_input(
                                        list1, list3, before_call_count, kpi)
                                    # and before_did(before_data, kpi) == False:
                                    # logger.info(data)
                                    if self.did(data, kpi, self.p_threshold) == True and self.did(before_data, kpi, self.p_threshold) == False:
                                        return instance["ip_addr"]

        return None

    def determine_old_instance_anomaly(self,
                                       instance,
                                       min_call_count,
                                       min_fail_count):
        """
        func determine_old_instance_anomaly: determine if an pre-change instance of a service that has undergone a gray change is abnormal
        :input
        - instance: instance ip
        - min_call_count: if the call count less min_call_count we ignore this pair
        - min_fail_count: if the fail count less min_fail_count we ignore this pair
        :return
        - if abnormal return instance ip
        - else return None
        """
        if "alert_metric" in instance and instance["alert_metric"]:
            for pair in instance["alert_metric"]:
                if "call_count" in instance["alert_metric"][pair]:
                    for kpi in instance["alert_metric"][pair]:
                        list1, list2 = self.get_old_instance_data(
                            instance, pair, kpi)

                        if list1 is not None:
                            if kpi == "call_count":
                                if max(list1 + list2) < min_call_count:
                                    # logger.debug(
                                    #     "skip because %s %s call_count less than 100 ", instance["ip_addr"], pair)
                                    break
                                call_count_list = list1 + list1 + list1 + list2
                                call_count_dict = {
                                    "call_count": call_count_list}
                                call_count = DataFrame(call_count_dict)

                            elif max(list1 + list2) > min_fail_count:
                                # anormaly = detect_outliers(list1, list2)

                                # if anormaly:
                                #     return instance["ip_addr"]

                                # logger.info(
                                #     "skip because %s %s %s less than %s ", instance["ip_addr"], pair,kpi,min_fail_count)

                                before_data = self.before_did_concat_input(
                                    list1, list2, call_count, kpi)

                                if self.did(before_data, kpi, self.p_threshold):
                                    return instance["ip_addr"]

    def determine_gray_change_anomaly(self,
                                      change_data,
                                      instance,
                                      min_call_count,
                                      min_fail_count):
        """
        func determine_old_instance_anomaly: determine if an post-change instance of a service that has undergone a gray change is abnormal
        :input
        - change_data: all change data of this module
        - instance: instance ip
        - min_call_count: if the call count less min_call_count we ignore this pair
        - min_fail_count: if the fail count less min_fail_count we ignore this pair
        :return
        - if abnormal return instance ip
        - else return None
        """
        if "alert_metric" in instance and instance["alert_metric"]:
            for pair in instance["alert_metric"]:
                if "call_count" in instance["alert_metric"][pair]:
                    old_instance = self.get_similar_instance(
                        change_data, instance, pair, "call_count")
                    # logger.info(old_instance)

                    if old_instance is None:
                        # if not have similar old_instance, use did method
                        for kpi in instance["alert_metric"][pair]:
                            list1, list2, list3, list4 = self.get_did_data(
                                instance, pair, kpi)
                            if kpi == "call_count":
                                if max(list1 + list2) < min_call_count:
                                    # logger.info(
                                    #     "skip because %s %s call_count less than 100 ", instance["ip_addr"], pair)
                                    break
                                before_call_count, call_count = self.get_call_count(
                                    list1, list2, list3, list4)

                            elif max(list1 + list2) > min_fail_count:
                                # logger.info(
                                #     "skip because %s %s %s less than %s ", instance["ip_addr"], pair,kpi,min_fail_count)
                                if detect_outliers(normal_data=list1, online_data=list2, abnormal_number=3):
                                    data = self.did_concat_input(list1,
                                                                 list2, list3, list4, call_count, kpi)

                                    before_data = self.before_did_concat_input(
                                        list1, list3, before_call_count, kpi)
                                    # and before_did(before_data, kpi) == False:
                                    if self.did(data, kpi, self.p_threshold) == True and self.did(before_data, kpi, self.p_threshold) == False:
                                        # logger.info("old instance none: %s, %s, %s",
                                        #             instance["ip_addr"], pair, kpi)
                                        # logger.info(data)
                                        return instance["ip_addr"]

                    else:
                        # use ddd method
                        for kpi in instance["alert_metric"][pair]:
                            list1, list2, list3, list4, list5, list6 = self.get_ddd_data(
                                instance, old_instance, pair, kpi)

                            if kpi == "call_count":
                                if max(list1 + list2) < min_call_count:
                                    # logger.warn(
                                    #     "skip because %s %s call_count less than 100 ", instance["ip_addr"], pair)
                                    break
                                before_call_count, call_count = self.get_call_count(
                                    list1, list2, list3, list4, list5, list6)

                            elif max(list1 + list2) > min_fail_count:
                                # logger.info(
                                #     "skip because %s %s %s less than %s ", instance["ip_addr"], pair,kpi,min_fail_count)
                                if detect_outliers(normal_data=list1, online_data=list2, abnormal_number=3):
                                    data = self.ddd_concat_input(list1,
                                                                 list2, list3, list4, list5, list6, call_count, kpi)
                                    # before_data1 = before_did_concat_input(
                                    #     list1, list3, before_call_count, kpi)

                                    # before_data2 = before_did_concat_input(
                                    #     list1, list5, before_call_count, kpi)
                                    # and before_did(before_data1, kpi) == False and before_did(before_data2, kpi) == False:

                                    before_data = self.before_did_concat_input(
                                        list1, list5, before_call_count, kpi)
                                    # and before_did(before_data, kpi) == False:
                                    if self.did(data, kpi, self.p_threshold) == True and self.did(before_data, kpi, self.p_threshold) == False:
                                        # logger.info("%s, %s, %s",
                                        #             instance["ip_addr"], pair, kpi)
                                        # logger.info(data)
                                        return instance["ip_addr"]
        return None

    def difference_method(self,
                          module,
                          target="change",
                          change_data=None,
                          taskid=None,
                          min_call_count=1000,
                          min_fail_count=100):
        """
        func difference_method: execute difference method
        : input
        - module: moudle name
        - target
            - target="change": determine post-change instance difference
            - target="old": determine pre-change instance difference
            - target="nochange": determine no-change instance difference
        - taskid: taskid in change ticket
        - min_call_count: minimum call count to consider
        - min_fail_count: minimum fail count to consider

        : output
        - module: module name
        - abnormal instance count / all instance count
        - target
        - abnormal instance count
        - all instance count
        - paltform
        - taskid
        """
        if change_data is None:
            # logger.info("module %s start", module)
            if taskid is None and "task_id" in self.data[module]["change_ticket"]:
                taskid = self.data[module]["change_ticket"]["task_id"]
            else:
                return (module, None, None, None, None, None, None)

            file = self.base_path + \
                module + "_" + \
                taskid + "_change_flow.json"

            if os.path.exists(file):
                with open(file, "r+") as f:
                    change_data = ujson.load(f)
            else:
                logger.error("file %s not exist", file)
                return (module, None, None, 0.0, 0.0, None, None)

        anomaly_instance_list = set()

        if target == "change":
            if len(change_data["change_ticket"]["host_list_change"]) == 0:
                return (module, None, None, 0.0, 0.0, self.data[module]["change_ticket"]["platform"], self.data[module]["change_ticket"]["task_id"])
            elif len(change_data["change_ticket"]["host_list_old"]) == 0:
                # if change complete, use did method
                # logger.info("module %s did", module)
                with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor3:
                    futures3 = {executor3.submit(
                        self.determine_change_complete_anomaly, instance, min_call_count, min_fail_count) for instance in change_data["change_ticket"]["host_list_change"]}

                    for future3 in concurrent.futures.as_completed(futures3):
                        result = future3.result()
                        if result is not None:
                            anomaly_instance_list.add(result)
                    executor3.shutdown()
                # logger.info("%s", anomaly_instance_list)
                # logger.info("%s, %s", module, len(anomaly_instance_list) /
                #             len(change_data["change_ticket"]["host_list_change"]))
                return (module, 1.0 * len(anomaly_instance_list) / len(change_data["change_ticket"]["host_list_change"]), "complete", len(anomaly_instance_list), len(change_data["change_ticket"]["host_list_change"]), self.data[module]["change_ticket"]["platform"], self.data[module]["change_ticket"]["task_id"])
            else:
                # logger.info("module %s ddd", module)
                with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor4:
                    futures4 = {executor4.submit(
                        self.determine_gray_change_anomaly, change_data, instance, min_call_count, min_fail_count) for instance in change_data["change_ticket"]["host_list_change"]}

                    for future4 in concurrent.futures.as_completed(futures4):
                        result = future4.result()
                        if result is not None:
                            anomaly_instance_list.add(result)
                    executor4.shutdown()
                # logger.info("%s", anomaly_instance_list)
                # logger.info("%s, %s", module, len(anomaly_instance_list) /
                #             len(change_data["change_ticket"]["host_list_change"]))
                return (module, 1.0 * len(anomaly_instance_list) / len(change_data["change_ticket"]["host_list_change"]), "gray", len(anomaly_instance_list), len(change_data["change_ticket"]["host_list_change"]), self.data[module]["change_ticket"]["platform"], self.data[module]["change_ticket"]["task_id"])

        elif target == "old":
            if len(change_data["change_ticket"]["host_list_old"]) > 0:
                with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor5:
                    futures5 = {executor5.submit(
                        self.determine_old_instance_anomaly, instance, min_call_count, min_fail_count) for instance in change_data["change_ticket"]["host_list_old"]}

                    for future5 in concurrent.futures.as_completed(futures5):
                        result = future5.result()
                        if result is not None:
                            anomaly_instance_list.add(result)
                    executor5.shutdown()
                return (module, 1.0 * len(anomaly_instance_list) / len(change_data["change_ticket"]["host_list_old"]), "old", len(anomaly_instance_list), len(change_data["change_ticket"]["host_list_old"]), self.data[module]["change_ticket"]["platform"], self.data[module]["change_ticket"]["task_id"])

            else:
                return (module, 0.0, None, 0, 0, self.data[module]["change_ticket"]["platform"], self.data[module]["change_ticket"]["task_id"])
        elif target == "nochange":
            if len(change_data["change_ticket"]["host_list_old"]) > 0:
                with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor6:
                    futures6 = {executor6.submit(
                        self.determine_old_instance_anomaly, instance, min_call_count, min_fail_count) for instance in change_data["change_ticket"]["host_list_old"]}

                    for future6 in concurrent.futures.as_completed(futures6):
                        result = future6.result()
                        if result is not None:
                            anomaly_instance_list.add(result)
                    executor6.shutdown()
                return (module, 1.0 * len(anomaly_instance_list) / len(change_data["change_ticket"]["host_list_old"]), "nochange", len(anomaly_instance_list), len(change_data["change_ticket"]["host_list_old"]), None, None)

            else:
                return (module, 0.0, None, 0, 0, None, None)


if __name__ == '__main__':
    incident = [
        {
            "alert_module": "serviceg",
            "alert_time": "2022-11-21 15:22:27",
            "root_module": "serviceh",
            "platform": "platfrom1",
            "task_id": "123456",
        }
    ]

    for i in range(len(incident)):
        logger.info(incident[i])
        differentiator = Differentiator(incident[i]["alert_time"],
                                        incident[i]["alert_module"])
        # result = differentiator.get_all_difference_result()
        # result = differentiator.get_all_difference_result("old")

        file = differentiator.base_path + \
            incident[i]["alert_module"] + "_change_flow.json"

        if os.path.exists(file):
            with open(file, "r+") as f:
                change_data = ujson.load(f)

        result = differentiator.difference_method(incident[i]["alert_module"])

        print(result)
