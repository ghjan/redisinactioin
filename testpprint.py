# -*- coding: utf-8 -*-
# python pprint

'''python API中提供的Sample'''
'''python中的pprint.pprint()，类似于print()
    http://www.cnblogs.com/hongten/p/hongten_python_pprint.html

    https://docs.python.org/2/library/pprint.html#module-pprint
    http://www.pythontab.com/html/2014/pythonjichu_1021/893.html
'''
import json
import pprint
from urllib2 import urlopen
from testurllib2 import *

url_string = 'http://pypi.python.org/pypi/configparser/json'
stream = getStream_withsocks(url_string)
if stream:
    http_info = stream.info()
    if http_info.items():
        charset = getCharsetFromHeader(http_info.items())
    raw_data = stream.read()
    if charset:
        raw_data = raw_data.decode(charset)
    project_info = json.loads(raw_data)
    result = {'headers': http_info.items(), 'body': project_info}

    pprint.pprint(result)

    pprint.pprint('#' * 50)
    pprint.pprint(result, depth=3)

    pprint.pprint('#' * 50 + "---pprint.pprint(result['headers'], width=30)")
    # 格式化文本的默认输出宽度为80列。要调整这个宽度，可以再pprint()中使用参数width。
    # 宽度大小不能适应格式化数据结构时，如果斩断或转行会引入非法的语法，就不会进行截断或转行。
    pprint.pprint(result['headers'], width=30)

    pprint.pprint('#' * 50)
    # 自定义Demo
    test_list = ['a', 'c', 'e', 'd', '2']
    test_list.insert(0, test_list)
    pprint.pprint(test_list)
