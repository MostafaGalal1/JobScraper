from Scrapers.AmazonScraper import AmazonScraper
from Scrapers.GoogleScraper import GoogleScraper
from Scrapers.LinkedInScraper import LinkedInScraper
from Scrapers.MetaScraper import MetaScraper
from Scrapers.WuzzufScraper import WuzzufScraper
from DAOs.JobDAO import JobDAO


def fetch_jobs(scrapers, job_dao, job_set):
    unseen_jobs = []
    print("Fetching jobs...")

    for scraper in scrapers:
        jobs = scraper.scrape_jobs()
        for job in jobs:
            job_id = job.job_id

            # Avoid duplicates
            if job_id not in job_set:
                job_set.add(job_id)
                unseen_jobs.append(job)

    job_dao.insert_jobs(unseen_jobs)
    print(f"Inserted {len(unseen_jobs)} new jobs into the database")


class JobScraper:
    def __init__(self):
        job_dao = JobDAO()
        job_dao.create_jobs_table()
        job_set = job_dao.get_jobs_hashes()

        scrapers = [WuzzufScraper(), MetaScraper(), AmazonScraper(), GoogleScraper(), LinkedInScraper()]
        fetch_jobs(scrapers, job_dao, job_set)


if __name__ == "__main__":
    JobScraper()
