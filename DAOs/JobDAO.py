from config.database import Database


class JobDAO:
    def __init__(self):
        self.db = Database()

    def create_jobs_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                job_title TEXT NOT NULL,
                company_name TEXT NOT NULL,
                job_type TEXT NOT NULL,
                experience_years TEXT,
                experience_level TEXT,
                salary TEXT,
                city TEXT,
                country TEXT,
                job_url TEXT UNIQUE NOT NULL
            );
            """
        self.db.execute_query(query)

    def get_jobs_hashes(self):
        return {job[0] for job in self.get_all_jobs()}

    def insert_jobs(self, jobs):
        job_data = [
            (
                job.job_id,
                job.job_title,
                job.company_name,
                job.job_type,
                job.experience_years,
                job.experience_level,
                job.salary,
                job.city,
                job.country,
                job.job_url
            )
            for job in jobs
        ]

        query = """
            INSERT INTO jobs (job_id, job_title, company_name, job_type, experience_years, experience_level, salary, city, country, job_url)
            VALUES %s
            ON CONFLICT (job_url) DO NOTHING;
        """
        from psycopg2.extras import execute_values
        execute_values(self.db.cursor, query, job_data)
        self.db.conn.commit()

    def get_all_jobs(self):
        return self.db.fetch_all("SELECT * FROM jobs;")
