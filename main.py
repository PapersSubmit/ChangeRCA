from ranker import *
import csv

incident = [
    {
        "alert_module": "serviceg",
        "alert_time": "2022-11-21 15:22:27",
        "root_module": "serviceh",
        "platform": "platfrom1",
        "task_id": "123456",
    }

]


if __name__ == '__main__':
    # p_the_list = [0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]
    # gray_the_list = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    """
    get the final result of ChangeRCA
    """
    the = 0.7
    p_the = 0.15

    top_module_list = []
    top_taskid_list = []

    other_fault_number = 0
    for i in range(len(incident)):
        ranker = Ranker(incident[i]["alert_time"],
                        incident[i]["alert_module"], incident[i]["root_module"], the,
                        )
        score = ranker.root_cause_change_identifycation()

        k = 0
        for module in score:
            logger.info("%s,%s,%s,%s,%s,%s", module,
                        incident[i]["root_module"], score[module]["platform"], incident[i]["platform"], score[module]["task_id"], incident[i]["task_id"])
            k = k + 1
            if module == incident[i]["root_module"] and score[module]["platform"] == incident[i]["platform"] and score[module]["task_id"] == incident[i]["task_id"]:
                logger.info("%s,%s,%s,%s,%s,%s", module,
                            incident[i]["root_module"], score[module]["platform"], incident[i]["platform"], score[module]["task_id"], incident[i]["task_id"])
                if k > 20:
                    k = 20
                top_taskid_list.append(k)
                break

    top1 = 0
    top3 = 0
    top5 = 0
    all_num = 0

    for item in top_taskid_list:
        if item <= 5:
            top5 += 1
        if item <= 3:
            top3 += 1
        if item == 1:
            top1 += 1
        all_num += item

    logger.info(
        "-------------------Change Task Top1 score------------------")
    precision = top1/len(incident)
    logger.info("HR@1:%s", precision)

    logger.info(
        "-------------------Change Task Top3 score------------------")
    precision = top3/len(incident)
    logger.info("HR@3:%s", precision)

    logger.info(
        "-------------------Change Task Top5 score------------------")
    precision = top5/len(incident)
    logger.info("HR@5:%s", precision)

    logger.info(
        '-------------------Change Task MAR Result------------------')
    logger.info(all_num/len(incident))
