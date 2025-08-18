import json
import scrapy

class QuotesSpider(scrapy.Spider):
    name = "aops_crawler"

    async def start(self):
        urls = [
            "https://artofproblemsolving.com/community/c13_contests",

        ]
        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_contest,
                meta={
                    "driver": "contest",
                    "id": 13,
                    "parent_id": None,
                },
            )

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
                        yield scrapy.Request(
                            url=f"https://artofproblemsolving.com/community/c{c.get('category_id')}",
                            callback=self.parse_category,
                            meta={
                                "driver": "category",
                                "id": c.get("category_id"),
                                "parent_id": response.meta.get("id", 13),
                            },
                        )
                        # yield scrapy.Request(url=, callback=self.parse_contest,meta={"driver":"contest"})
    def parse_category(self, response):
        # response.body is json.dump.encode('utf-8') we need to decode it to a object   
        json_data = json.loads(response.body.decode('utf-8'))
        
        # Extract items from the first filtered response
        first_filtered = json_data.get("first_filtered", {})
        response_json = first_filtered.get("response_json", {})
        
        category_data = response_json.get("response", {}).get("category", {})
        items = category_data.get("items", [])
        
        print(f"Found {len(items)} items in category")
        
        # Extract item_id and item_type for each item
        for item in items:
            item_id = item.get("item_id")
            item_type = item.get("item_type")
            
            # print(f"Item ID: {item_id}, Type: {item_type}, Text: {item_text}")
            
            # You can yield more requests here based on item_type
            if item_type == "folder" or item_type=='view_posts':
                # This is a subfolder, crawl it
                yield scrapy.Request(
                    url=f"https://artofproblemsolving.com/community/c{item_id}", 
                    callback=self.parse_category,
                    meta={
                        "driver": "category",
                        "id": item_id,
                        "parent_id": response.meta.get("id"),
                    }
                )
            elif item_type == "post" and item["post_data"]["post_type"]=="forum":
                # This is a forum, you might want to crawl posts
                yield scrapy.Request(
                    url=f"https://artofproblemsolving.com/community/p{item_id}", 
                    callback=self.parse_post,
                    meta={
                        "driver": "post",
                        "id": item_id,  # post id
                        "parent_id": response.meta.get("id"),  # parent category id
                    }
                )
                # Add forum crawling logic here if needed
        
        # # save data to a file for debugging
        # with open("test/category.json", "w") as f:
        #     json.dump(json_data, f)
    def parse_post(self, response):
        # the response is a html response
        # save it to a file to test
        with open("test/post.html", "w", encoding="utf-8") as f:
            f.write(response.css("body").get())
