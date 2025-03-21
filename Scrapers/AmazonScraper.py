import json
from abc import ABC

from Scrapers.BaseScraper import BaseScraper
from DAOs.Job import Job


class AmazonScraper(BaseScraper, ABC):
    def __init__(self):
        super().__init__()
        with open("Utils/ISO-3166-1-alpha-3.json", "r", encoding="utf-8") as file:
            self.country_mapping = json.load(file)

    def get_scrape_url(self):
        return "https://www.amazon.jobs/en-gb/search.json?category%5B%5D=software-development&schedule_type_id%5B%5D=Full-Time&industry_experience[]=less_than_1_year&normalized_country_code%5B%5D=DEU&normalized_country_code%5B%5D=GBR&normalized_country_code%5B%5D=ESP&normalized_country_code%5B%5D=LUX&normalized_country_code%5B%5D=JOR&normalized_country_code%5B%5D=POL&normalized_country_code%5B%5D=IRL&normalized_country_code%5B%5D=ROU&normalized_country_code%5B%5D=ZAF&normalized_country_code%5B%5D=NLD&normalized_country_code%5B%5D=FIN&normalized_country_code%5B%5D=FRA&normalized_country_code%5B%5D=ITA&normalized_country_code%5B%5D=CAN&normalized_country_code%5B%5D=EGY&radius=24km&industry_experience[]=less_than_1_year&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset=0&result_limit=100&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&"

    def get_scrape_headers(self, token):
        pass

    def parse_jobs(self, url):
        response = self.session.get(url)
        data = response.json()
        job_cards = data.get("jobs", [])

        jobs = []
        for job_card in job_cards:
            job_title = job_card.get("title")
            company_name = job_card.get("company_name")
            job_type = job_card.get("job_schedule_type", "Not Specified").replace("-", " ").title()
            city = job_card.get("city")
            country_code = job_card.get("country_code")
            country = self.country_mapping.get(country_code)
            job_url = f"https://www.amazon.jobs{job_card.get('job_path', '')}"

            job = Job(job_title=job_title, company_name=company_name, job_type=job_type, city=city,
                      country=country, job_url=job_url)
            jobs.append(job)

        return jobs
