from abc import ABC
from bs4 import BeautifulSoup

from Scrapers.BaseScraper import BaseScraper
from DAOs.Job import Job


class LinkedInScraper(BaseScraper, ABC):
    def __init__(self):
        super().__init__()

    def get_scrape_url(self):
        return "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

    def parse_jobs(self, url):
        params = {
            "keywords": "Software Engineer",
            "f_E": "1,2",  # Entry Level (1), Internship (2)
            "location": "Egypt",
            "trk": "public_jobs_jobs-search-bar_search-submit"
        }

        jobs = []
        max_jobs = 100  # Maximum number of jobs to scrape
        jobs_per_page = 10  # Number of jobs per page

        for start in range(0, max_jobs, jobs_per_page):
            params["start"] = start
            response = self.session.get(url, params=params)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                job_cards = soup.find_all("div",
                                          class_="base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card")

                for job_card in job_cards:
                    job_info = job_card.find("div", class_="base-search-card__info")
                    title_tag = job_info.find("h3", class_="base-search-card__title").text.strip()
                    company_tag = job_info.find("h4", class_="base-search-card__subtitle").text.strip()
                    location_tag = job_info.find("span", class_="job-search-card__location").text.strip()
                    city = location_tag.rsplit(",", 1)[0].strip() if "," in location_tag else "Not Provided"
                    country = location_tag.rsplit(",", 1)[1].strip() if "," in location_tag else location_tag
                    job_url = job_card.find("a", class_="base-card__full-link absolute top-0 right-0 bottom-0 left-0 p-0 z-[2]").get("href")

                    job = Job(job_title=title_tag, company_name=company_tag, city=city, country=country, job_url=job_url)
                    jobs.append(job)

                if len(jobs) >= max_jobs:
                    break

        return jobs
