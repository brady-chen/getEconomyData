# -*-coding:utf-8 -*-

from mylog import MyLog
from bs4 import BeautifulSoup
import requests
import threading
from sqlserver import SqlServer
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class MostData(object):
    log = MyLog()

    def __init__(self):
        self.start_url = "http://quote.fx678.com/exchange/"
        self.DATA_DICT = {
            '国际外汇': 'WH',
            "国际黄金": 'WGJS',
            '全球指数': 'GJZS',
            'IPE原油': 'IPE',
            'NYMEX原油': 'NYMEX',
            '上海金': 'SGE',
            '伦敦金属': 'LME',
            '纽约金属': 'COMEX',
            '纸黄金': 'PGOLD',
            '纽约期货': 'NYBOT',
            '上海期货': 'SHFE',
            '津贵所': 'TJPME'}
        self.HEADERS = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'fx678.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'}

    def get_and_uploading_data(self, data_name):
        start_url = "http://quote.fx678.com/exchange/" + \
            self.DATA_DICT[data_name]
        list_name = []
        list_newPrice = []
        list_riseAndFall = []
        list_openPrice = []
        list_topPrice = []
        list_lowestPrice = []
        list_yesterdayPrice = []
        list_updateTime = []
        session = requests.session()
        session.headers = self.HEADERS
        html = session.get(start_url, timeout=30).content
        soup = BeautifulSoup(html, 'lxml')
        data_html = soup.findAll("td")
        for i in range(len(data_html) + 1):
            if i != 0:
                if i % 8 == 0:
                    list_name.append(
                        data_html[i - 8].get_text() if data_html[i - 8].get_text() else "null")
                    list_newPrice.append(
                        data_html[i - 7].get_text() if data_html[i - 7].get_text() else "null")
                    list_riseAndFall.append(
                        data_html[i - 6].get_text() if data_html[i - 6].get_text() else "null")
                    list_openPrice.append(
                        data_html[i - 5].get_text() if data_html[i - 5].get_text() else "null")
                    list_topPrice.append(
                        data_html[i - 4].get_text() if data_html[i - 4].get_text() else "null")
                    list_lowestPrice.append(
                        data_html[i - 3].get_text() if data_html[i - 3].get_text() else "null")
                    list_yesterdayPrice.append(
                        data_html[i - 2].get_text() if data_html[i - 2].get_text() else "null")
                    list_updateTime.append(
                        data_html[i - 1].get_text() if data_html[i - 1].get_text() else "null")
        db = SqlServer(
            host='www.example.com',
            user='username',
            pwd='password',
            db='databaseName')
        for i in range(len(list_name)):
            try:
                u_sql = "UPDATE [{0}] SET name = '{1}', newPrice = {2}, riseAndFall = {3}, openPrice = {4}, " \
                        "topPrice = {5}, lowestPrice = {6}, yesterdayPrice = {7}, updateTime = '{8}' WHERE id = {9}".format(
                            "inf_" + self.DATA_DICT[data_name], list_name[i], list_newPrice[i], list_riseAndFall[i], list_openPrice[i],
                            list_topPrice[i], list_lowestPrice[i], list_yesterdayPrice[i], list_updateTime[i], i + 1
                        )
                db.ExecNonQuery(u_sql.encode('utf-8'))
            except IndexError as e:
                print "出错于:%s函数，原因为:%s" % (sys._getframe().f_code.co_name, e)

    @log.deco_log(sys.argv[0][0:-3] + '.log', 'threads_run', True)
    def threads_run(self):
        global t
        threads = []
        t1 = threading.Thread(
            target=self.get_and_uploading_data, args=("国际外汇",))
        threads.append(t1)
        t2 = threading.Thread(
            target=self.get_and_uploading_data, args=("国际黄金",))
        threads.append(t2)
        t3 = threading.Thread(
            target=self.get_and_uploading_data, args=("全球指数",))
        threads.append(t3)
        t4 = threading.Thread(
            target=self.get_and_uploading_data, args=("IPE原油",))
        threads.append(t4)
        t5 = threading.Thread(
            target=self.get_and_uploading_data, args=("NYMEX原油",))
        threads.append(t5)
        t6 = threading.Thread(
            target=self.get_and_uploading_data, args=("上海金",))
        threads.append(t6)
        t7 = threading.Thread(
            target=self.get_and_uploading_data, args=("伦敦金属",))
        threads.append(t7)
        t8 = threading.Thread(
            target=self.get_and_uploading_data, args=("纽约金属",))
        threads.append(t8)
        t9 = threading.Thread(
            target=self.get_and_uploading_data, args=("纸黄金",))
        threads.append(t9)
        t10 = threading.Thread(
            target=self.get_and_uploading_data, args=("纽约期货",))
        threads.append(t10)
        t11 = threading.Thread(
            target=self.get_and_uploading_data, args=("上海期货",))
        threads.append(t11)
        t12 = threading.Thread(
            target=self.get_and_uploading_data, args=("津贵所",))
        threads.append(t12)
        for t in threads:
            t.setDaemon(True)
            t.start()
        t.join()


num = 1


def fun_timer():
    global num
    print "------------------正在启动第%s次数据更新------------------" % str(num)
    num += 1
    m = MostData()
    try:
        m.threads_run()
        timer = threading.Timer(1, fun_timer)
        timer.start()
    except BaseException as e:
        print "报错原因：" + str(e)
        fun_timer()

if __name__ == '__init__':
    print "已启动"
    fun_timer()
