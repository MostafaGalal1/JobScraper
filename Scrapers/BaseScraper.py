import csv
import requests
from abc import ABC, abstractmethod
from dotenv import load_dotenv


class BaseScraper(ABC):
    def __init__(self):
        load_dotenv()
        self.name = self.__class__.__name__.replace("Scraper", "")
        self.session = requests.Session()

    def scrape_jobs(self):
        # Scrape
        scrape_url = self.get_scrape_url()
        jobs = self.parse_jobs(scrape_url)
        return jobs

        # Save
        # self.save_jobs(jobs)

    @abstractmethod
    def get_scrape_url(self):
        pass

    @abstractmethod
    def parse_jobs(self, url):
        pass

    def save_jobs(self, jobs):
        with open(f"../CSVs/{self.name}_jobs.csv", "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Job Title", "Company Name", "Job Type", "Experience Years", "Experience Level", "Salary", "City", "Country", "Job URL"])
            writer.writerows(jobs)