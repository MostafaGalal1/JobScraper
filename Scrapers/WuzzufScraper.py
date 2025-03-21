from abc import ABC
from os import environ

from Scrapers.BaseScraper import BaseScraper
from DAOs.Job import Job


class WuzzufScraper(BaseScraper, ABC):
    def __init__(self):
        super().__init__()

    def login(self):
        credentials = {
            "email": environ.get("WUZZUF_EMAIL"),
            "password": environ.get("WUZZUF_PASSWORD")
        }
        response = self.session.post("https://wuzzuf.net/login/submit", data=credentials)
        token = response.headers["set-cookie"].split("user_access_token=")[1].split(";")[0]
        return token

    def get_scrape_url(self):
        return "https://wuzzuf.net/api/talent/related-opportunities?page%5Bsize%5D=100&page%5Bnumber%5D=1&include=city.country%2Ccountry%2Ckeywords%2Carea.city%2Ccompany%2CcareerLevel%2CopportunityWorkRoles.workRole%2CopportunityWorkTypes.workType%2CeducationalDegree%2CsalaryCurrency%2CworkplaceArrangement"

    def parse_jobs(self, url):
        token = self.login()
        headers = {
            "Authorization": f"Bearer {token}"
        }

        response = self.session.get(url, headers=headers)
        data = response.json()
        job_cards = data.get("data", [])
        included_list = data.get("included", [])

        included_dict = {}
        for item in included_list:
            type_ = item.get("type")
            id_ = item.get("id")
            if type_ not in included_dict:
                included_dict[type_] = {}
            included_dict[type_][id_] = item

        jobs = []
        for job_card in job_cards:
            attributes = job_card.get("attributes", {})
            relationships = job_card.get("relationships", {})

            job_title = attributes.get("title")
            experience_years = attributes.get("experienceYears")

            # Extract company details using the included dictionary
            company_rel = relationships.get("company", {}).get("data", {})
            company_id = company_rel.get("id", "N/A") if company_rel else "N/A"
            company_obj = included_dict.get("company", {}).get(company_id, {})
            company_name = company_obj.get("attributes", {}).get("name")

            # Extract job type details using the included dictionary
            job_type_rel = relationships.get("opportunityWorkTypes", {}).get("data", [{}])[0]
            job_type_id = job_type_rel.get("id", "N/A")
            job_type_obj = included_dict.get("opportunityWorkType", {}).get(job_type_id, {})
            job_type_id = job_type_obj.get("relationships", {}).get("workType", {}).get("data", {}).get("id", "N/A")
            job_type_obj = included_dict.get("workType", {}).get(job_type_id, {})
            job_type = job_type_obj.get("attributes", {}).get("displayedName")

            # Extract city details using the included dictionary
            city_rel = relationships.get("city", {}).get("data", {})
            city_id = city_rel.get("id", "N/A")
            city_obj = included_dict.get("city", {}).get(city_id, {})
            city = city_obj.get("attributes", {}).get("name")

            # Extract country details using the included dictionary
            country_rel = relationships.get("country", {}).get("data", {})
            country_id = country_rel.get("id", "N/A")
            country_obj = included_dict.get("country", {}).get(country_id, {})
            country = country_obj.get("attributes", {}).get("name")

            # Extract career level details using the included dictionary
            career_rel = relationships.get("careerLevel", {}).get("data", {})
            career_id = career_rel.get("id", "N/A")
            career_obj = included_dict.get("careerLevel", {}).get(career_id, {})
            experience_level = career_obj.get("attributes", {}).get("name")
            salary_min = attributes.get("salaryMin")
            salary_max = attributes.get("salaryMax")
            salary = f"{salary_min}-{salary_max} EGP" if salary_min is not None and salary_max is not None else "Salary not provided"
            job_url = f"https://wuzzuf.net/jobs/p/{job_card.get('id', '')}"

            job = Job(job_title=job_title, company_name=company_name, job_type=job_type, experience_years=experience_years,
                      experience_level=experience_level, salary=salary, city=city, country=country, job_url=job_url)
            jobs.append(job)

        return jobs
