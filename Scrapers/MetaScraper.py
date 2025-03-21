from abc import ABC

from Scrapers.BaseScraper import BaseScraper
from DAOs.Job import Job


class MetaScraper(BaseScraper, ABC):
    def __init__(self):
        super().__init__()

    def get_scrape_url(self):
        return "https://www.metacareers.com/graphql"

    def parse_jobs(self, url):
        headers = {
            "x-fb-friendly-name": "CareersJobSearchResultsDataQuery",
            "x-fb-lsd": "AVoJ6b0H5Pg",
        }
        body = {
            "lsd": "AVoJ6b0H5Pg",
            "fb_api_req_friendly_name": "CareersJobSearchResultsDataQuery",
            "variables": '{"search_input":{"q":null,"divisions":[],"offices": ["Dublin, Ireland", "London, UK", "Paris, France", "Berlin, Germany", "Warsaw, Poland", "Zurich, Switzerland", "Madrid, Spain", "Johannesburg, South Africa", "Milan, Italy", "Brussels, Belgium", "Cork, Ireland"],"leadership_levels":[],"saved_jobs":[],"saved_searches":[],"sub_teams":[],"teams":[],"is_leadership":false,"is_remote_only":false,"sort_by_new":false,"results_per_page":null}}',
            "doc_id": "9509267205807711"
        }

        response = self.session.post(url, headers=headers, data=body)
        data = response.json()
        job_cards = data.get("data", {}).get("job_search_with_featured_jobs", {}).get("all_jobs", [])

        jobs = []
        company_name = "Meta"
        for job_card in job_cards:
            job_title = job_card.get("title")
            locations = job_card.get("locations", [])
            city = ", ".join([loc.split(",")[0].strip() for loc in locations]) if locations else None
            country = ", ".join([loc.split(",")[-1].strip() for loc in locations]) if locations else None
            job_id = job_card.get("id", "")
            job_url = f"https://www.metacareers.com/jobs/{job_id}" if job_id else None

            job = Job(job_title=job_title, company_name=company_name, city=city, country=country, job_url=job_url)
            jobs.append(job)

        return jobs
