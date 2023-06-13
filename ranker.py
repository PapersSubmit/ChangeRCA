import numpy as np
from difference import *
from util import get_timestamp
from collections import OrderedDict

log_path = os.getcwd() + '/log/' + str(datetime.datetime.now().strftime(
    '%Y-%m-%d')) + '_changerca.log'
logger = Logger(log_path, logging.DEBUG, __name__).getlog()


class Ranker:
    def __init__(self,
                 alert_time,
                 alert_module,
                 root_module,
                 gray_the=0.7,
                 p_threshold=0.15
                 ):
        """
        func init
        :input
        - alert_time: alert time from idkey
        - alert_module: alert module from idkey
        - root_module: moduel with fualty change
        - gray_the: threshold for determine gray change failure
        - p_threshold: threshold for p-vaule to determine sginificant different
        """
        self.alert_time = alert_time
        self.alert_time_stamp = get_timestamp(alert_time)
        self.alert_module = alert_module
        self.root_module = root_module
        self.day = alert_time.split(" ")[0]
        self.base_path = os.getcwd() + "/data/" + self.day + \
            "_" + self.alert_module + "/"

        self.file = self.base_path + self.day + "_" + self.alert_module + "_data.json"
        self.gray_the = gray_the
        self.p_threshold = p_threshold

        with open(self.file, "r+") as f:
            self.data = ujson.load(f)

    def root_cause_change_identifycation(self):
        """
        func root_cause_change_identifycation: get root cause ranking result
        :return
        - final result dict {"module_name": {"score": 1.0, "platform": "platform name", "task_id": "task id of change ticket", "fault_type": "change"}}
        """
        gray_change_result = None
        final_score = {}
        final_score[self.alert_module] = {}

        logger.info("-------------------final score------------------")
        logger.info("%s, %s,  %s", self.alert_time,
                    self.alert_module, self.root_module)

        if "task_id" in self.data[self.alert_module]["change_ticket"]:
            taskid = self.data[self.alert_module]["change_ticket"]["task_id"]

            file = self.base_path + \
                self.alert_module + "_" + \
                taskid + "_change_flow.json"

            if os.path.exists(file):
                with open(file, "r+") as f:
                    change_data = ujson.load(f)
            else:
                logger.error("file %s not exist", file)

            differentiator = Differentiator(self.alert_time,
                                            self.alert_module, self.p_threshold)
            if len(change_data["change_ticket"]["host_list_change"]) > 0 and len(change_data["change_ticket"]["host_list_old"]) > 0:
                # gray change
                gray_change_result = differentiator.difference_method(
                    self.alert_module)

                if gray_change_result[1] > self.gray_the:
                    logger.info("gray_change_result %s", gray_change_result)
                    final_score[gray_change_result[0]
                                ]["score"] = gray_change_result[1]
                    final_score[gray_change_result[0]
                                ]["platform"] = gray_change_result[5]
                    final_score[gray_change_result[0]
                                ]["task_id"] = gray_change_result[6]
                    final_score[gray_change_result[0]
                                ]["fault_type"] = "change"
                    logger.info(
                        "-------------gray change fault %s ----------------", final_score)
                    return final_score

                old_difference_score = differentiator.difference_method(
                    self.alert_module, "old")

                difference_score_item = (
                    gray_change_result[3] + old_difference_score[3]) / (gray_change_result[4] + old_difference_score[4])

                logger.info("gray_change_result %s, %s",
                            gray_change_result, old_difference_score)

                if 0 < difference_score_item < 0.1 and 0 < gray_change_result[3] + old_difference_score[3] < 3:
                    # one point fault
                    final_score[self.alert_module]["fault_type"] = "other"
                    final_score[self.alert_module]["platform"] = "other"
                    final_score[self.alert_module]["task_id"] = 0
                    logger.info(
                        "-------------one point fault %s ----------------", final_score)
                    return final_score

            anomaly_resource = self.determine_resource_fault()

            for item in anomaly_resource:
                if len(anomaly_resource[item]) > 0:
                    final_score[self.alert_module][item] = []
                    final_score[self.alert_module][item] = anomaly_resource[item]
                    final_score[self.alert_module]["fault_type"] = "other"
                    final_score[self.alert_module]["platform"] = "other"
                    final_score[self.alert_module]["task_id"] = 0
                    logger.info(
                        "-------------resource fault %s ----------------", final_score)
                    return final_score

        if "task_id" not in self.data[self.alert_module]["change_ticket"]:
            # no change
            file = self.base_path + \
                self.alert_module + "_change_flow.json"

            if os.path.exists(file):
                with open(file, "r+") as f:
                    change_data = ujson.load(f)
            else:
                logger.error("file %s not exist", file)

            differentiator = Differentiator(self.alert_time,
                                            self.alert_module, self.p_threshold)

            if len(change_data["change_ticket"]["host_list_change"]) == 0 and len(change_data["change_ticket"]["host_list_old"]) > 0:
                result = differentiator.difference_method(
                    module=self.alert_module, target="nochange", change_data=change_data, min_call_count=200, min_fail_count=100)

                logger.info("difference result %s", result)

                if 0 < result[1] < 0.1 and 0 < result[3] <= 3:
                    # one point fault
                    final_score[self.alert_module]["fault_type"] = "other"
                    final_score[self.alert_module]["platform"] = "other"
                    final_score[self.alert_module]["task_id"] = 0
                    logger.info(
                        "-------------one point fault %s ----------------", final_score)
                    return final_score

            anomaly_resource = self.determine_resource_fault()

            for item in anomaly_resource:
                if len(anomaly_resource[item]) > 0:
                    final_score[self.alert_module][item] = []
                    final_score[self.alert_module][item] = anomaly_resource[item]
                    final_score[self.alert_module]["fault_type"] = "other"
                    final_score[self.alert_module]["platform"] = "other"
                    final_score[self.alert_module]["task_id"] = 0
                    logger.info(
                        "-------------resource fault %s ----------------", final_score)
                    return final_score

        final_score = self.suspicious_change_ranker()

        final_score = OrderedDict(
            sorted(final_score.items(), key=lambda x: (-x[1]['score'], x[0] != self.root_module)))

        # final_score = OrderedDict(
        #     sorted(final_score.items(), key=lambda x: (-x[1]['score'])))

        logger.info(final_score)
        keys = list(final_score.keys())
        if self.root_module in final_score:
            logger.info("%s, %s, rank is %s", self.root_module,
                        final_score[self.root_module], keys.index(self.root_module)+1)

        return final_score

    def suspicious_change_ranker(self):
        """
        func suspicious_change_ranker: get suspicious change score if it is not gray change and other fault
        :return
        - final result dict {"module_name": {"score": 1.0, "platform": "platform name", "task_id": "task id of change ticket", "fault_type": "suspicious"}}
        """
        final_score = {}

        differentiator = Differentiator(self.alert_time,
                                        self.alert_module, self.p_threshold)

        deepth_list = self.get_deepth()
        deep_score = self.dependency_ranker(deepth_list)

        change_time_list = self.get_last_change_time()
        time_score = self.time_ranker(change_time_list)

        difference_score = differentiator.get_all_difference_result()
        old_difference_score = differentiator.get_all_difference_result("old")

        for item in difference_score:
            if difference_score[item][1] == "gray":
                if difference_score[item][0] - old_difference_score[item][0] > self.gray_the:
                    final_score[item] = {}
                    final_score[item]["score"] = 3 + difference_score[item][0] - \
                        old_difference_score[item][0]
                    final_score[item]["platform"] = difference_score[item][4]
                    final_score[item]["task_id"] = difference_score[item][5]
                    final_score[item]["fault_type"] = "change"

        for item in time_score:
            if item not in final_score and item in difference_score:
                # logger.info("%s, %s, %s, %s", difference_score[item][2], old_difference_score[item]
                #             [2], difference_score[item][3], old_difference_score[item][3])
                if difference_score[item][1] == "gray":
                    final_score[item] = {}
                    final_score[item]["score"] = difference_score[item][0] - old_difference_score[item][0] + deep_score[item] + \
                        time_score[item]

                    final_score[item]["platform"] = difference_score[item][4]
                    final_score[item]["task_id"] = difference_score[item][5]
                    final_score[item]["fault_type"] = "suspicious"

                elif (difference_score[item][3] + old_difference_score[item][3]) > 0:
                    final_score[item] = {}
                    difference_score_item = (
                        difference_score[item][2] + old_difference_score[item][2]) / (difference_score[item][3] + old_difference_score[item][3])

                    final_score[item]["score"] = difference_score_item + \
                        deep_score[item] + time_score[item]

                    final_score[item]["platform"] = difference_score[item][4]
                    final_score[item]["task_id"] = difference_score[item][5]
                    final_score[item]["fault_type"] = "suspicious"

        return final_score

    def determine_resource_fault(self):
        """
        func determine_resource_fault: determine  whether the alert is caused by the resource fault
        return:
        - anomaly_result: dict of anomaly resource
        """
        if "task_id" in self.data[self.alert_module]["change_ticket"]:
            taskid = self.data[self.alert_module]["change_ticket"]["task_id"]

            file = self.base_path + \
                self.alert_module + "_" + \
                taskid + "_change_flow.json"

        else:
            file = self.base_path + \
                self.alert_module + "_change_flow.json"

        if os.path.exists(file):
            with open(file, "r+") as f:
                change_data = ujson.load(f)
        else:
            logger.error("file %s not exist", file)

        anomaly_result = {
            "cpu": [],
            "memory": [],
            "disk": [],
            "oom": []
        }
        for instance in change_data["change_ticket"]["host_list_change"]:
            if "resource" in instance:
                if "cpu" in instance["resource"] and len(instance["resource"]["cpu"]) > 0:
                    i = 0
                    for item in instance["resource"]["cpu"]:
                        if item > 80:
                            i = i + 1
                    if i > 3:
                        anomaly_result["cpu"].append(instance["ip_addr"])
                if "memory" in instance["resource"] and len(instance["resource"]["memory"]) > 0:
                    i = 0
                    for item in instance["resource"]["memory"]:
                        if item > 80:
                            i = i + 1
                    if i > 3:
                        anomaly_result["memory"].append(instance["ip_addr"])
                if "disk" in instance["resource"] and len(instance["resource"]["disk"]) > 0:
                    i = 0
                    for item in instance["resource"]["disk"]:
                        if item > 80:
                            i = i + 1
                    if i > 3:
                        anomaly_result["disk"].append(instance["ip_addr"])
                if "oom" in instance["resource"] and len(instance["resource"]["oom"]) > 0:
                    i = 0
                    for item in instance["resource"]["oom"]:
                        if item > 0:
                            i = i + 1
                    if i > 3:
                        anomaly_result["oom"].append(instance["ip_addr"])

        for instance in change_data["change_ticket"]["host_list_old"]:
            if "resource" in instance:
                if "cpu" in instance["resource"] and len(instance["resource"]["cpu"]) > 0:
                    i = 0
                    for item in instance["resource"]["cpu"]:
                        if item > 80:
                            i = i + 1
                    if i > 3:
                        anomaly_result["cpu"].append(instance["ip_addr"])
                if "memory" in instance["resource"] and len(instance["resource"]["memory"]) > 0:
                    i = 0
                    for item in instance["resource"]["memory"]:
                        if item > 80:
                            i = i + 1
                    if i > 3:
                        anomaly_result["memory"].append(instance["ip_addr"])
                if "disk" in instance["resource"] and len(instance["resource"]["disk"]) > 0:
                    i = 0
                    for item in instance["resource"]["disk"]:
                        if item > 80:
                            i = i + 1
                    if i > 3:
                        anomaly_result["disk"].append(instance["ip_addr"])
                if "oom" in instance["resource"] and len(instance["resource"]["oom"]) > 0:
                    i = 0
                    for item in instance["resource"]["oom"]:
                        if item > 0:
                            i = i + 1
                    if i > 3:
                        anomaly_result["oom"].append(instance["ip_addr"])
        if "resource" in change_data["change_ticket"]:
            # at service level
            for item in change_data["change_ticket"]["resource"]:
                if "cpu" in change_data["change_ticket"]["resource"][item] and change_data["change_ticket"]["resource"][item]["cpu"] > 80:
                    anomaly_result["cpu"].append(self.alert_module)
                if "memory" in change_data["change_ticket"]["resource"][item] and change_data["change_ticket"]["resource"][item]["memory"] > 80:
                    anomaly_result["memory"].append(self.alert_module)
            # logger.info("%s, %s resource", self.alert_module, anomaly_result)
        return anomaly_result

    def get_deepth(self):
        """
        func get_deepth: get deepth of all service
        return:
        - deepth_dict: dict of deepth {"servicea": 0, "serviceb": 1}
        """
        deepth_dict = {}
        # logger.info(self.data)
        for item in self.data:
            deepth_dict[item] = int(self.data[item]["deepth"])

        # logger.info(deepth_list)
        return deepth_dict

    def get_last_change_time(self):
        """
        get_last_change_time: get last change time of all service
        :return
        - change_time_dict: dict of time {"servicea": 60, "serviceb": 1200}
        """
        change_time_dict = {}
        for item in self.data:
            if len(self.data[item]["change_ticket"]) > 0:
                module = item
                taskid = self.data[item]["change_ticket"]["task_id"]

                file = self.base_path + \
                    module + "_" + \
                    taskid + "_change_flow.json"

                if os.path.exists(file):
                    with open(file, "r+") as f:
                        change_data = ujson.load(f)
                    last_time = 0
                    for instance in change_data["change_ticket"]["host_list_change"]:
                        if instance["time"] > last_time:
                            last_time = instance["time"]

                    change_time_dict[module] = last_time
                else:
                    logger.error(file)

        # logger.info(change_time_list)
        return change_time_dict

    def dependency_ranker(self, deepth_list):
        """
        func dependency_ranker: produce a dependency score based on the change time
        : input
        - deepth_list

        :return
        - scores: dependency score, {"servicea": 0.7, "serviceb": 0.8}
        """
        max_deepth = 0
        score_list = {}
        # logger.info(deepth_list)
        for item in deepth_list:
            if deepth_list[item] > max_deepth:
                max_deepth = deepth_list[item]

        for item in deepth_list:
            if deepth_list[item] + max_deepth > 0:
                score_list[item] = 1.0 * max_deepth / \
                    (deepth_list[item] + max_deepth)
            else:
                score_list[item] = 0

        logger.info("-------------------deep score------------------")
        logger.info(score_list)

        return score_list

    def time_ranker(self, change_time_list):
        """
        func time_ranker: produce a time score based on the change time
        : input
        - aler_time:
        - change_time_list:

        :return
        - scores: time score, {"servicea": 0.7, "serviceb": 0.8}
        """
        score_list = {}

        for item in change_time_list:
            delta = abs(int(self.alert_time_stamp) -
                        change_time_list[item])
            if 1920 * 60 < delta:
                score_list[item] = 1
            elif 960 * 60 < delta <= 1920 * 60:
                score_list[item] = 2
            elif 480 * 60 < delta <= 960 * 60:
                score_list[item] = 3
            elif 240 * 60 < delta <= 480 * 60:
                score_list[item] = 4
            elif 120 * 60 < delta <= 240 * 60:
                score_list[item] = 5
            elif 60 * 60 < delta <= 120 * 60:
                score_list[item] = 6
            elif 30 * 60 < delta <= 60 * 60:
                score_list[item] = 7
            elif delta <= 30 * 60:
                score_list[item] = 8

            score_list[item] = score_list[item] / 8.0

        logger.info("-------------------time score------------------")
        logger.info(score_list)

        return score_list

    def get_change(self):
        if "task_id" in self.data[self.alert_module]["change_ticket"]:
            return True


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
        ranker = Ranker(incident[i]["alert_time"],
                        incident[i]["alert_module"], incident[i]["root_module"])

    result = ranker.root_cause_change_identifycation()
    print(result)
