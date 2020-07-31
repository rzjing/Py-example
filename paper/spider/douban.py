# @Author   : Wang Xiaoqiang
# @GitHub   : https://github.com/rzjing
# @Time     : 2020-07-23 17:54
# @File     : douban.py

import json
import re

import requests
from bs4 import BeautifulSoup


# 资源详情抓取
def getMovieDetail(url):
    result = {}
    r = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'})
    if r.status_code == 200:
        html = r.text.encode(r.encoding).decode('utf8')
        soup = BeautifulSoup(html, 'html5lib')

        # 提取详情
        html_data = soup.find('script', type='application/ld+json').get_text().strip()
        detail = json.loads(re.sub(r'[\n|\t]]', '', html_data))

        # 影片基础信息
        result['title'] = detail['name'].split()[0]
        result['titleOther'] = detail['name'].split()[1:-1]
        result['url'] = url
        result['cover'] = detail['image']
        result['director'] = [
            {'id': item['url'].split('/')[-2], 'name': item['name'], 'url': 'https://movie.douban.com' + item['url']}
            for item in detail['director']
        ]
        result['author'] = [
            {'id': item['url'].split('/')[-2], 'name': item['name'], 'url': 'https://movie.douban.com' + item['url']}
            for item in detail['author']
        ]
        result['actor'] = [
            {'id': item['url'].split('/')[-2], 'name': item['name'], 'url': 'https://movie.douban.com' + item['url']}
            for item in detail['actor']
        ]
        result['description'] = detail['description']
        result['releaseDate'] = detail['datePublished']
        result['genre'] = detail['genre']
        result['duration'] = detail['duration']

        # 推荐豆列
        doulist = []
        doulist_li = soup.find("div", id="subject-doulist").select_one("ul").select("li")
        for li in doulist_li:
            a = li.select_one('a')
            url = a.get('href')
            doulist.append({'id': url.split('/')[-2], 'name': a.get_text(), 'url': url})
        result['doulist'] = doulist

        # 相关推荐
        recommendlist = []
        recommend_dl = soup.find("div", class_="recommendations-bd").select("dl")
        for dl in recommend_dl:
            a = dl.select_one('dd').select_one('a')
            url = a.get('href')
            recommendlist.append({'id': url.split('/')[-2], 'name': a.get_text(), 'url': url})
        result['recommend'] = recommendlist
    else:
        print(r.status_code, r.reason)
    return result


MovieListUrl = 'https://movie.douban.com/j/search_subjects?type={}&tag=%E7%83%AD%E9%97%A8&sort=recommend&page_limit=20&page_start={}'


# 资源列表抓取
def getMovieList(category='movie', page=1):
    r = requests.get(MovieListUrl.format(category, page * 20), headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'})
    if r.status_code == 200:
        subjects = r.json()['subjects']
        for item in subjects:
            detail = getMovieDetail(item['url'])
            if detail:
                print(detail)
                detail.update({'id': item['id'], 'rate': item['rate']})
            break
    else:
        print(r.status_code, r.json())


# 人物详情抓取
def getPersonDetail(url):
    r = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'})
    if r.status_code == 200:
        html = r.text.encode(r.encoding).decode('utf8')
        soup = BeautifulSoup(html, 'html5lib')

        # 人物基础信息
        name = soup.head.title.string.strip().split()[0]
        # 标签定位
        info_tag = soup.find("div", id="headline")
        full_title = info_tag.find("a", class_="nbg").get("title")
        base = soup.find("div", id="intro").find("div", class_="bd")
        try:
            description = base.find("span", class_="all hidden").get_text().strip()
        except AttributeError:
            description = base.get_text().strip()

        # 基础信息
        a_name, c_name, occupation, family, imdb = [], [], [], [], {}
        gender = birthday = area = cx = ""
        for _ in info_tag.find("ul").find_all("li"):
            key = _.span.string
            if key == "性别":
                gender = _.span.next_sibling.replace(":", "").strip()
            elif key == "星座":
                cx = _.span.next_sibling.replace(":", "").strip()
            elif key == "出生日期":
                birthday = _.span.next_sibling.replace(":", "").strip()
            elif key == "出生地":
                area = _.span.next_sibling.replace(":", "").strip()
            elif key == "职业":
                _val = _.span.next_sibling.replace(":", "").strip().split("/")
                occupation = [_.strip() for _ in _val]
            elif key == "更多外文名":
                _val = _.span.next_sibling.replace(":", "").strip().split("/")
                a_name = [_.strip() for _ in _val]
            elif key == "更多中文名":
                _val = _.span.next_sibling.replace(":", "").strip().split("/")
                c_name = [_.strip() for _ in _val]
            elif key == "家庭成员":
                _val = _.span.next_sibling.replace(":", "").strip().split("/")
                family = [_.strip() for _ in _val]
            elif key == "imdb编号":
                a = _.select_one("a")
                imdb = {"id": a.get_text(), "url": a.get("href")}

        detail = {
            "id": url.split('/')[-2], "url": url, "name": name, "nameOther": " ".join(full_title.split()[1:]),
            "alias": a_name + c_name, "cover": info_tag.find("a", class_="nbg").get("href"),
            "description": description, "gender": gender, "birthday": birthday, "area": area, "country": "",
            "height": -1, "cx": cx, "ox": occupation, "onLine": 1,
            "extend": {"family": family, "imdb": imdb}
        }
        print(detail)
    else:
        print(r.status_code, r.reason)


if __name__ == '__main__':
    # getMovieList()
    getPersonDetail('https://movie.douban.com/celebrity/1054531/')
