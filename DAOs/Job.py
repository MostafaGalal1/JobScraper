import hashlib
from dataclasses import dataclass, field


@dataclass
class Job:
    job_id: str = field(init=False)
    job_title: str = field(default="Unknown")
    company_name: str = field(default="Unknown")
    job_type: str = field(default="Full-time")
    experience_years: str = field(default="Not Provided")
    experience_level: str = field(default="Entry")
    salary: str = field(default="Not Disclosed")
    city: str = field(default="Unknown")
    country: str = field(default="Unknown")
    job_url: str = field(default="N/A")

    def __post_init__(self):
        self.generate_job_id()

    def generate_job_id(self):
        hash_input = f"{self.job_title}{self.company_name}{self.job_type}{self.experience_years}{self.experience_level}{self.salary}{self.city}{self.country}"
        self.job_id = hashlib.sha256(hash_input.encode()).hexdigest()

    def __setattr__(self, key, value):
        super().__setattr__(key, value) if value else super().__setattr__(key, self.__getattribute__(key))

    def __iter__(self):
        return iter(vars(self).values())