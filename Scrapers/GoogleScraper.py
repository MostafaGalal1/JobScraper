import json
from abc import ABC

from Scrapers.BaseScraper import BaseScraper
from DAOs.Job import Job


class GoogleScraper(BaseScraper, ABC):
    def __init__(self):
        super().__init__()
        with open("Utils/ISO-3166-1-alpha-2.json", "r", encoding="utf-8") as file:
            self.country_mapping = json.load(file)

    def get_scrape_url(self):
        return "https://careers.google.com/api/v3/search/"

    def parse_jobs(self, url):
        params = {
            "distance": "50",
            "has_remote": "false",
            "location": ["London, United Kingdom", "Dublin, Ireland", "Paris, France", "Berlin, Germany", "Warsaw, Poland"],
            "q": "Software Engineer",
            "sort_by": "date",
            "page": "1",
            "page_size": "100"
        }

        response = self.session.get(url, params=params)
        data = response.json()
        job_cards = data.get("jobs", [])

        jobs = []
        for job_card in job_cards:
            if job_card.get("target_level") != "Early" and job_card.get("target_level") != "Mid":
                continue
            job_title = job_card.get("title")
            company_name = job_card.get("company_name")
            experience_level = job_card.get("target_level")
            city = job_card["locations"][0].get("city")
            country_code = job_card["locations"][0].get("country_code")
            country = self.country_mapping.get(country_code)
            job_url = job_card.get("apply_url", "Unknown URL")

            job = Job(job_title=job_title, company_name=company_name, experience_level=experience_level,
                      city=city, country=country, job_url=job_url)
            jobs.append(job)

        return jobs

