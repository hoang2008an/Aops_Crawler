import json
import scrapy

class QuotesSpider(scrapy.Spider):
    name = "aops_crawler"

    async def start(self):
        urls = [
            "https://artofproblemsolving.com/community/c13_contests",

        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_contest,meta={"driver":"contest"})

    def parse_contest(self, response):
        # response.body is json.dump.encode('utf-8') we need to decode it to a object   
        json_data = json.loads(response.body.decode('utf-8'))
        # print(json_data)
        # write it to a file
        # with open("response.json", "w") as f:
        #     json.dump(json_data, f)
        for req in json_data["ajax_requests"]:
            rt = req.get("response_json")
            if isinstance(rt, dict):
                print("--------------------------------")
                cats = (rt.get("response") or {}).get("categories") or []
                for c in cats :
                    if "category_id" in c:
                        yield scrapy.Request(url=f"https://artofproblemsolving.com/community/c{c.get('category_id')}", callback=self.parse_category,meta={"driver":"category"})
                        # yield scrapy.Request(url=, callback=self.parse_contest,meta={"driver":"contest"})
    def parse_category(self, response):
        # response.body is json.dump.encode('utf-8') we need to decode it to a object   
        json_data = json.loads(response.body.decode('utf-8'))
        # save data to a file
        with open("test/category.json", "w") as f:
            json.dump(json_data, f)
        